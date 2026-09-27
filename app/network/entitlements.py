"""用户权益事件账本与按订单版本归并的投影。

设计要点：

- ``entitlement_events`` 是不可变账本，只插入不更新；同一 ``(order_id, event_seq)``
  的来源事件号只记录一次，重复投递按幂等处理。
- 接收闸门依次是：来源事件号去重 → 业务版本闸门（``business_version`` 必须大于投影
  当前版本，否则记为 ``stale`` 过时写入）→ 身份一致性 → 生命周期迁移校验。未通过的
  事件也会留在账本里（``stale``/``rejected``），客服可以在时间线里看到每一次到达。
- 投影是 ``transition`` 纯函数对已应用事件按 ``(business_version, event_seq, id)``
  归并的结果，只依赖账本内容，不依赖挂钟，因此重建投影必然得到相同状态。
- 已应用事件的业务版本在到达时严格递增，所以在线归并顺序与重放的规范顺序一致，
  在线处理、重放与重建三者结果相同。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, DomainError, NotFoundError, ValidationError
from app.core.security import request_fingerprint
from app.database import get_connection, transaction
from app.network.repository import NetworkRepository
from app.network.schema import ensure_network_schema

EVENT_TYPES = ("purchase", "extend", "pause", "resume", "refund", "expire")

# 投影状态到既有权益最终行（subscriber_entitlements.state）的映射，保持 1:1。
STATUS_TO_ENTITLEMENT_STATE = {
    "active": "active",
    "suspended": "suspended",
    "expired": "expired",
    "cancelled": "cancelled",
}

ELIGIBILITY_REASONS = {
    "active": "权益有效",
    "suspended": "权益已暂停",
    "expired": "权益已到期",
    "cancelled": "订单已退款",
    "not_yet_valid": "权益尚未生效",
    "validity_ended": "权益有效期已结束",
}

PROJECTION_COMPARE_FIELDS = (
    "subscriber_hash",
    "scenario_id",
    "product_code",
    "status",
    "version",
    "valid_from",
    "valid_until",
    "paused_at",
    "pause_total_seconds",
    "last_event_seq",
    "applied_events",
)


class InvalidTransition(Exception):
    """生命周期迁移不合法，事件会被记录为 rejected。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class FoldEvent:
    """参与归并的事件视图，全部使用业务时间，与到达时刻无关。"""

    order_id: str
    event_seq: int
    event_type: str
    business_version: int
    subscriber_hash: str
    scenario_id: int | None
    product_code: str | None
    valid_from: datetime | None
    valid_until: datetime | None
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectionState:
    order_id: str
    subscriber_hash: str
    scenario_id: int
    product_code: str
    status: str
    version: int
    valid_from: datetime
    valid_until: datetime
    paused_at: datetime | None
    pause_total_seconds: int
    last_event_seq: int
    applied_events: int


def transition(state: ProjectionState | None, event: FoldEvent) -> ProjectionState:
    """权益订单状态机，是投影重建与在线归并共用的唯一归并函数。

    明确规则：

    - purchase：仅允许作为订单首个事件；重复购买必须换新的订单号。
    - extend：有效期只延长不缩短，新截止时间必须晚于当前截止；与现有有效期交叉时
      归并为连续区间（起点不变、截止取更晚者）。订单已到期时视为续期，按事件给定
      的起点（缺省取事件发生时间）重新激活。订单已退款（cancelled）时不允许续期，
      必须重新下单。
    - pause：仅 active 可暂停，记录暂停起点，本身不改变有效期。
    - resume：仅 suspended 可恢复，按暂停时长等额顺延有效期（暂停补偿）。
    - refund：active/suspended/expired 均可退款，退款是终态，之后任何事件都会被拒绝。
    - expire：active/suspended 可到期；暂停期间到期不发生暂停补偿。
    """

    if event.event_type == "purchase":
        if state is not None:
            raise InvalidTransition("duplicate_purchase")
        if event.scenario_id is None or not event.product_code:
            raise InvalidTransition("invalid_purchase")
        if event.valid_from is None or event.valid_until is None or event.valid_until <= event.valid_from:
            raise InvalidTransition("invalid_period")
        return ProjectionState(
            order_id=event.order_id,
            subscriber_hash=event.subscriber_hash,
            scenario_id=event.scenario_id,
            product_code=event.product_code,
            status="active",
            version=event.business_version,
            valid_from=event.valid_from,
            valid_until=event.valid_until,
            paused_at=None,
            pause_total_seconds=0,
            last_event_seq=event.event_seq,
            applied_events=1,
        )
    if state is None:
        raise InvalidTransition("not_purchased")
    if event.subscriber_hash != state.subscriber_hash:
        raise InvalidTransition("identity_mismatch")
    if event.scenario_id is not None and event.scenario_id != state.scenario_id:
        raise InvalidTransition("identity_mismatch")
    if event.product_code is not None and event.product_code != state.product_code:
        raise InvalidTransition("identity_mismatch")
    if state.status == "cancelled":
        raise InvalidTransition("order_closed")
    base = replace(
        state,
        version=event.business_version,
        last_event_seq=event.event_seq,
        applied_events=state.applied_events + 1,
    )
    if event.event_type == "extend":
        if event.valid_until is None:
            raise InvalidTransition("invalid_period")
        if event.valid_until <= state.valid_until:
            raise InvalidTransition("extension_not_effective")
        if state.status in ("active", "suspended"):
            return replace(base, valid_until=event.valid_until)
        new_start = event.valid_from or event.occurred_at
        if event.valid_until <= new_start:
            raise InvalidTransition("invalid_period")
        return replace(base, status="active", valid_from=new_start, valid_until=event.valid_until, paused_at=None)
    if event.event_type == "pause":
        if state.status != "active":
            raise InvalidTransition("invalid_transition")
        return replace(base, status="suspended", paused_at=event.occurred_at)
    if event.event_type == "resume":
        if state.status != "suspended" or state.paused_at is None:
            raise InvalidTransition("invalid_transition")
        paused_seconds = max(0, int((event.occurred_at - state.paused_at).total_seconds()))
        return replace(
            base,
            status="active",
            valid_until=state.valid_until + timedelta(seconds=paused_seconds),
            paused_at=None,
            pause_total_seconds=state.pause_total_seconds + paused_seconds,
        )
    if event.event_type == "refund":
        return replace(base, status="cancelled", paused_at=None)
    if event.event_type == "expire":
        if state.status not in ("active", "suspended"):
            raise InvalidTransition("invalid_transition")
        return replace(base, status="expired", paused_at=None)
    raise InvalidTransition("unknown_event")


def fold_events(events: list[FoldEvent]) -> ProjectionState | None:
    """按规范顺序归并事件，等价于在线处理结果，用于投影重建与对账。"""

    state: ProjectionState | None = None
    for event in events:
        state = transition(state, event)
    return state


class EntitlementLedgerService:
    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        ensure_network_schema(self.connection)
        self.clock = clock or SystemClock()
        self.repository = NetworkRepository(self.connection)

    # ------------------------------------------------------------------ 写入

    def ingest_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        event = self._normalize(payload)
        digest = request_fingerprint(self._canonical_payload(event, payload))
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT * FROM entitlement_events WHERE order_id=? AND event_seq=?",
                (event["order_id"], event["event_seq"]),
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != digest:
                    raise ConflictError("相同来源事件号对应了不同事件内容")
                return self._ingest_result(existing, duplicate=True)
            state = self._stored_state(connection, event["order_id"])
            decision = "applied"
            reason = ""
            new_state: ProjectionState | None = None
            if state is not None and event["business_version"] <= state.version:
                decision = "stale"
                reason = f"业务版本 {event['business_version']} 不晚于当前版本 {state.version}"
            else:
                try:
                    new_state = transition(state, self._fold_event(event))
                except InvalidTransition as exc:
                    decision = "rejected"
                    reason = exc.reason
            cursor = connection.execute(
                "INSERT INTO entitlement_events(order_id,event_seq,event_type,business_version,subscriber_hash,scenario_id,product_code,valid_from,valid_until,occurred_at,received_at,actor,payload_digest,decision,decision_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    event["order_id"],
                    event["event_seq"],
                    event["event_type"],
                    event["business_version"],
                    event["subscriber_hash"],
                    event["scenario_id"],
                    event["product_code"] or "",
                    event["valid_from"],
                    event["valid_until"],
                    event["occurred_at"],
                    now,
                    event["actor"],
                    digest,
                    decision,
                    reason,
                ),
            )
            if new_state is not None:
                self._store_projection(connection, new_state, now)
                self._sync_entitlement_row(connection, new_state, now)
            row = connection.execute("SELECT * FROM entitlement_events WHERE id=?", (cursor.lastrowid,)).fetchone()
            return self._ingest_result(row, duplicate=False)

    def ingest_batch(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        results = []
        for item in items:
            try:
                results.append(self.ingest_event(item))
            except DomainError as exc:
                results.append(
                    {
                        "order_id": item.get("order_id"),
                        "event_seq": item.get("event_seq"),
                        "decision": "error",
                        "decision_reason": exc.code,
                        "message": exc.message,
                    }
                )
        return {"items": results, "accepted": sum(1 for item in results if item["decision"] == "applied")}

    # ------------------------------------------------------------------ 查询

    def current_projection(self, order_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM entitlement_order_projection WHERE order_id=?", (order_id,)).fetchone()
        if row is None:
            raise NotFoundError("权益订单投影不存在")
        result = self._projection_payload(row)
        result["eligibility"] = self._eligibility(row)
        return result

    def timeline(self, order_id: str) -> dict[str, Any]:
        rows = self._ledger_rows(order_id)
        if not rows:
            raise NotFoundError("权益订单不存在")
        projection = self.connection.execute("SELECT * FROM entitlement_order_projection WHERE order_id=?", (order_id,)).fetchone()
        return {
            "order_id": order_id,
            "projection": self._projection_payload(projection) if projection else None,
            "events": [self._event_payload(row) for row in rows],
        }

    def reconcile_order(self, order_id: str) -> dict[str, Any]:
        rows = self._ledger_rows(order_id)
        if not rows:
            raise NotFoundError("权益订单不存在")
        stored = self.connection.execute("SELECT * FROM entitlement_order_projection WHERE order_id=?", (order_id,)).fetchone()
        applied = [row for row in rows if row["decision"] == "applied"]
        fold_error = ""
        rebuilt: ProjectionState | None = None
        try:
            rebuilt = fold_events([self._fold_event_from_row(row) for row in applied])
        except InvalidTransition as exc:
            fold_error = exc.reason
        projection_diffs = [] if fold_error else self._projection_diffs(stored, rebuilt)
        legacy_diffs = [] if fold_error else self._legacy_diffs(order_id, rebuilt)
        decision_counts: dict[str, int] = {}
        for row in rows:
            decision_counts[row["decision"]] = decision_counts.get(row["decision"], 0) + 1
        return {
            "order_id": order_id,
            "consistent": not fold_error and not projection_diffs and not legacy_diffs,
            "fold_error": fold_error,
            "projection_diffs": projection_diffs,
            "legacy_row_diffs": legacy_diffs,
            "missing_versions": self._missing_versions(rows, applied),
            "decision_counts": decision_counts,
            "ledger_events": len(rows),
            "applied_events": len(applied),
        }

    def reconcile_all(self) -> dict[str, Any]:
        order_ids = [row[0] for row in self.connection.execute("SELECT DISTINCT order_id FROM entitlement_events ORDER BY order_id").fetchall()]
        items = []
        for order_id in order_ids:
            report = self.reconcile_order(order_id)
            items.append(
                {
                    "order_id": order_id,
                    "consistent": report["consistent"],
                    "projection_diffs": len(report["projection_diffs"]),
                    "legacy_row_diffs": len(report["legacy_row_diffs"]),
                    "missing_versions": len(report["missing_versions"]),
                    "decision_counts": report["decision_counts"],
                }
            )
        return {"items": items, "inconsistent_orders": [item["order_id"] for item in items if not item["consistent"]]}

    # ------------------------------------------------------------------ 重建

    def rebuild_order(self, order_id: str) -> dict[str, Any]:
        rows = self._ledger_rows(order_id)
        if not rows:
            raise NotFoundError("权益订单不存在")
        applied = [row for row in rows if row["decision"] == "applied"]
        try:
            rebuilt = fold_events([self._fold_event_from_row(row) for row in applied])
        except InvalidTransition as exc:
            raise ConflictError("账本重放失败，已应用事件无法按序归并", context={"order_id": order_id, "reason": exc.reason}) from exc
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            stored = connection.execute("SELECT * FROM entitlement_order_projection WHERE order_id=?", (order_id,)).fetchone()
            projection_diffs = self._projection_diffs(stored, rebuilt)
            legacy_diffs = self._legacy_diffs(order_id, rebuilt, connection)
            if projection_diffs:
                if rebuilt is None:
                    connection.execute("DELETE FROM entitlement_order_projection WHERE order_id=?", (order_id,))
                else:
                    self._store_projection(connection, rebuilt, now)
            if legacy_diffs and rebuilt is not None:
                self._sync_entitlement_row(connection, rebuilt, now)
        return {
            "order_id": order_id,
            "replayed": len(applied),
            "consistent": not projection_diffs and not legacy_diffs,
            "projection_diffs": projection_diffs,
            "legacy_row_diffs": legacy_diffs,
            "projection": self.current_projection(order_id) if rebuilt is not None else None,
        }

    def rebuild_all(self) -> dict[str, Any]:
        order_ids = [row[0] for row in self.connection.execute("SELECT DISTINCT order_id FROM entitlement_events ORDER BY order_id").fetchall()]
        results = [self.rebuild_order(order_id) for order_id in order_ids]
        return {"orders": len(results), "inconsistent_orders": [item["order_id"] for item in results if not item["consistent"]]}

    # ------------------------------------------------------------------ 内部

    def _normalize(self, payload: dict[str, Any]) -> dict[str, Any]:
        event_type = payload["event_type"]
        if event_type not in EVENT_TYPES:
            raise ValidationError("不支持的事件类型")
        occurred_at = self._required_time(payload.get("occurred_at"), "事件发生时间")
        valid_from = self._optional_time(payload.get("valid_from"), "有效期开始时间")
        valid_until = self._optional_time(payload.get("valid_until"), "有效期截止时间")
        if event_type == "purchase":
            missing = [name for name in ("scenario_code", "product_code", "valid_from", "valid_until") if not payload.get(name)]
            if missing:
                raise ValidationError("购买事件必须提供场景、产品与有效期")
            if valid_until <= valid_from:
                raise ValidationError("权益结束时间必须晚于开始时间")
        if event_type == "extend" and not valid_until:
            raise ValidationError("延期事件必须提供新的截止时间")
        scenario_id = None
        if payload.get("scenario_code"):
            scenario = self.repository.scenario_by_code(payload["scenario_code"])
            if scenario is None:
                raise NotFoundError("网络场景不存在")
            scenario_id = int(scenario["id"])
        return {
            "order_id": payload["order_id"],
            "event_seq": int(payload["event_seq"]),
            "event_type": event_type,
            "business_version": int(payload["business_version"]),
            "subscriber_hash": payload["subscriber_hash"],
            "scenario_id": scenario_id,
            "product_code": payload.get("product_code"),
            "valid_from": valid_from,
            "valid_until": valid_until,
            "occurred_at": occurred_at,
            "actor": payload.get("actor") or "carrier-app",
        }

    @staticmethod
    def _canonical_payload(event: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "order_id": event["order_id"],
            "event_seq": event["event_seq"],
            "event_type": event["event_type"],
            "business_version": event["business_version"],
            "subscriber_hash": event["subscriber_hash"],
            "scenario_code": payload.get("scenario_code"),
            "product_code": event["product_code"],
            "valid_from": event["valid_from"],
            "valid_until": event["valid_until"],
            "occurred_at": event["occurred_at"],
        }

    def _fold_event(self, event: dict[str, Any]) -> FoldEvent:
        return FoldEvent(
            order_id=event["order_id"],
            event_seq=event["event_seq"],
            event_type=event["event_type"],
            business_version=event["business_version"],
            subscriber_hash=event["subscriber_hash"],
            scenario_id=event["scenario_id"],
            product_code=event["product_code"],
            valid_from=from_storage(event["valid_from"]),
            valid_until=from_storage(event["valid_until"]),
            occurred_at=from_storage(event["occurred_at"]),
        )

    @staticmethod
    def _fold_event_from_row(row: sqlite3.Row) -> FoldEvent:
        return FoldEvent(
            order_id=row["order_id"],
            event_seq=row["event_seq"],
            event_type=row["event_type"],
            business_version=row["business_version"],
            subscriber_hash=row["subscriber_hash"],
            scenario_id=row["scenario_id"],
            product_code=row["product_code"] or None,
            valid_from=from_storage(row["valid_from"]),
            valid_until=from_storage(row["valid_until"]),
            occurred_at=from_storage(row["occurred_at"]),
        )

    def _stored_state(self, connection: sqlite3.Connection, order_id: str) -> ProjectionState | None:
        row = connection.execute("SELECT * FROM entitlement_order_projection WHERE order_id=?", (order_id,)).fetchone()
        if row is None:
            return None
        return ProjectionState(
            order_id=row["order_id"],
            subscriber_hash=row["subscriber_hash"],
            scenario_id=row["scenario_id"],
            product_code=row["product_code"],
            status=row["status"],
            version=row["version"],
            valid_from=from_storage(row["valid_from"]),
            valid_until=from_storage(row["valid_until"]),
            paused_at=from_storage(row["paused_at"]),
            pause_total_seconds=row["pause_total_seconds"],
            last_event_seq=row["last_event_seq"],
            applied_events=row["applied_events"],
        )

    @staticmethod
    def _store_projection(connection: sqlite3.Connection, state: ProjectionState, now: str) -> None:
        connection.execute(
            "INSERT INTO entitlement_order_projection(order_id,subscriber_hash,scenario_id,product_code,status,version,valid_from,valid_until,paused_at,pause_total_seconds,last_event_seq,applied_events,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(order_id) DO UPDATE SET subscriber_hash=excluded.subscriber_hash,scenario_id=excluded.scenario_id,product_code=excluded.product_code,status=excluded.status,version=excluded.version,valid_from=excluded.valid_from,valid_until=excluded.valid_until,paused_at=excluded.paused_at,pause_total_seconds=excluded.pause_total_seconds,last_event_seq=excluded.last_event_seq,applied_events=excluded.applied_events,updated_at=excluded.updated_at",
            (
                state.order_id,
                state.subscriber_hash,
                state.scenario_id,
                state.product_code,
                state.status,
                state.version,
                to_storage(state.valid_from),
                to_storage(state.valid_until),
                to_storage(state.paused_at) if state.paused_at else None,
                state.pause_total_seconds,
                state.last_event_seq,
                state.applied_events,
                now,
            ),
        )

    @staticmethod
    def _sync_entitlement_row(connection: sqlite3.Connection, state: ProjectionState, now: str) -> None:
        connection.execute(
            "INSERT INTO subscriber_entitlements(subscriber_hash,scenario_id,product_code,valid_from,valid_until,state,source_order_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(source_order_id) DO UPDATE SET subscriber_hash=excluded.subscriber_hash,scenario_id=excluded.scenario_id,product_code=excluded.product_code,valid_from=excluded.valid_from,valid_until=excluded.valid_until,state=excluded.state,updated_at=excluded.updated_at",
            (
                state.subscriber_hash,
                state.scenario_id,
                state.product_code,
                to_storage(state.valid_from),
                to_storage(state.valid_until),
                STATUS_TO_ENTITLEMENT_STATE[state.status],
                state.order_id,
                now,
                now,
            ),
        )

    def _ledger_rows(self, order_id: str) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM entitlement_events WHERE order_id=? ORDER BY business_version,event_seq,id",
            (order_id,),
        ).fetchall()

    def _projection_diffs(self, stored: sqlite3.Row | None, rebuilt: ProjectionState | None) -> list[dict[str, Any]]:
        stored_payload = self._projection_compare_payload(stored) if stored is not None else None
        rebuilt_payload = self._state_compare_payload(rebuilt) if rebuilt is not None else None
        if stored_payload == rebuilt_payload:
            return []
        if stored_payload is None or rebuilt_payload is None:
            return [{"field": "projection", "stored": stored_payload, "rebuilt": rebuilt_payload}]
        return [
            {"field": field, "stored": stored_payload[field], "rebuilt": rebuilt_payload[field]}
            for field in PROJECTION_COMPARE_FIELDS
            if stored_payload[field] != rebuilt_payload[field]
        ]

    def _legacy_diffs(self, order_id: str, rebuilt: ProjectionState | None, connection: sqlite3.Connection | None = None) -> list[dict[str, Any]]:
        connection = connection or self.connection
        row = connection.execute("SELECT * FROM subscriber_entitlements WHERE source_order_id=?", (order_id,)).fetchone()
        if rebuilt is None:
            return [] if row is None else [{"field": "legacy_row", "stored": "present", "rebuilt": "absent"}]
        expected = {
            "subscriber_hash": rebuilt.subscriber_hash,
            "scenario_id": rebuilt.scenario_id,
            "product_code": rebuilt.product_code,
            "valid_from": to_storage(rebuilt.valid_from),
            "valid_until": to_storage(rebuilt.valid_until),
            "state": STATUS_TO_ENTITLEMENT_STATE[rebuilt.status],
        }
        if row is None:
            return [{"field": "legacy_row", "stored": "absent", "rebuilt": "present"}]
        return [
            {"field": f"legacy_row.{field}", "stored": row[field], "rebuilt": value}
            for field, value in expected.items()
            if row[field] != value
        ]

    @staticmethod
    def _missing_versions(rows: list[sqlite3.Row], applied: list[sqlite3.Row]) -> list[dict[str, Any]]:
        if not applied:
            return []
        applied_versions = {row["business_version"] for row in applied}
        by_version: dict[int, sqlite3.Row] = {}
        for row in rows:
            by_version.setdefault(row["business_version"], row)
        missing = []
        for version in range(1, max(applied_versions) + 1):
            if version in applied_versions:
                continue
            row = by_version.get(version)
            missing.append(
                {
                    "business_version": version,
                    "decision": row["decision"] if row else "absent",
                    "event_seq": row["event_seq"] if row else None,
                    "event_type": row["event_type"] if row else None,
                }
            )
        return missing

    @staticmethod
    def _projection_compare_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {field: row[field] for field in PROJECTION_COMPARE_FIELDS}

    @staticmethod
    def _state_compare_payload(state: ProjectionState) -> dict[str, Any]:
        return {
            "subscriber_hash": state.subscriber_hash,
            "scenario_id": state.scenario_id,
            "product_code": state.product_code,
            "status": state.status,
            "version": state.version,
            "valid_from": to_storage(state.valid_from),
            "valid_until": to_storage(state.valid_until),
            "paused_at": to_storage(state.paused_at) if state.paused_at else None,
            "pause_total_seconds": state.pause_total_seconds,
            "last_event_seq": state.last_event_seq,
            "applied_events": state.applied_events,
        }

    def _projection_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        scenario = self.repository.scenario_by_id(row["scenario_id"])
        result["scenario_code"] = scenario["code"] if scenario else None
        return result

    def _event_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        if row["scenario_id"] is not None:
            scenario = self.repository.scenario_by_id(row["scenario_id"])
            result["scenario_code"] = scenario["code"] if scenario else None
        else:
            result["scenario_code"] = None
        return result

    def _ingest_result(self, row: sqlite3.Row, *, duplicate: bool) -> dict[str, Any]:
        projection = self.connection.execute("SELECT * FROM entitlement_order_projection WHERE order_id=?", (row["order_id"],)).fetchone()
        result = {
            "order_id": row["order_id"],
            "event_seq": row["event_seq"],
            "event_type": row["event_type"],
            "business_version": row["business_version"],
            "decision": "duplicate" if duplicate else row["decision"],
            "decision_reason": row["decision_reason"],
            "ledger_id": row["id"],
            "projection": self._projection_payload(projection) if projection else None,
        }
        if duplicate:
            result["recorded_decision"] = row["decision"]
        return result

    def _eligibility(self, row: sqlite3.Row) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        status = row["status"]
        if status != "active":
            eligible, reason = False, status
        elif now < row["valid_from"]:
            eligible, reason = False, "not_yet_valid"
        elif now >= row["valid_until"]:
            eligible, reason = False, "validity_ended"
        else:
            eligible, reason = True, "active"
        return {"eligible": eligible, "reason": reason, "message": ELIGIBILITY_REASONS[reason], "checked_at": now}

    @staticmethod
    def _required_time(value: str | None, label: str) -> str:
        if not value:
            raise ValidationError(f"{label}不能为空")
        try:
            return to_storage(from_storage(value))
        except (TypeError, ValueError) as exc:
            raise ValidationError(f"{label}格式不正确") from exc

    @classmethod
    def _optional_time(cls, value: str | None, label: str) -> str | None:
        return cls._required_time(value, label) if value else None
