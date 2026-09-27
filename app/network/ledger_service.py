"""用户权益账本服务：不可变事件账本 + 按订单业务版本归并的投影。

写入路径（ingest_event）：
1. 字段与格式校验失败 -> 422，不写入账本；
2. 同一 (source_system, source_event_no) 重复送达 -> 去重返回原记录（200），
   内容摘要不一致 -> 409；
3. 业务版本 <= 已归并版本 -> 记录 stale 并返回 409；
4. 业务版本跳号或违反状态机规则 -> 记录 invalid 并返回 409，
   发送方补齐缺失版本后重放同一事件即可归并（重放安全且幂等）；
5. 其余事件归并进投影并返回 201。

投影（subscriber_entitlements）只是账本中 accepted 事件的折叠结果，
随时可以通过 rebuild_projection 删除重建并得到相同业务状态。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator

from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import request_fingerprint
from app.database import get_connection
from app.network.ledger import EVENT_TYPES, LedgerEvent, LedgerRuleError, OrderState, apply_event, entitlement_view, fold_events
from app.network.schema import ensure_network_schema

# 投影与规范状态之间逐字段比较的业务字段
BUSINESS_FIELDS = ("valid_from", "valid_until", "state", "suspended_at", "refunded_at", "applied_version")


@contextmanager
def _immediate_transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """在服务自身连接上开启即时事务（不依赖线程局部连接，便于多库重放对比）。"""
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


class EntitlementLedgerService:
    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        ensure_network_schema(self.connection)
        self.clock = clock or SystemClock()

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def ingest_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._validate(payload)
        canonical = {key: value for key, value in normalized["payload"].items() if value is not None}
        digest = request_fingerprint(canonical)
        payload_json = json.dumps(canonical, ensure_ascii=False, sort_keys=True)
        now = to_storage(self.clock.now())
        with _immediate_transaction(self.connection) as connection:
            existing = connection.execute(
                "SELECT * FROM entitlement_event_log WHERE source_system=? AND source_event_no=? AND disposition='accepted'",
                (normalized["payload"]["source_system"], normalized["payload"]["source_event_no"]),
            ).fetchone()
            if existing is not None:
                if existing["payload_digest"] != digest:
                    raise ConflictError("相同来源事件号对应了不同的事件内容")
                return {
                    "event": self._record(existing),
                    "projection": self._projection_payload(connection, existing["order_id"]),
                    "duplicate": True,
                }
            scenario = connection.execute(
                "SELECT * FROM network_scenarios WHERE code=?", (normalized["payload"]["scenario_code"],)
            ).fetchone()
            if scenario is None:
                raise NotFoundError("网络场景不存在")
            row = connection.execute(
                "SELECT * FROM subscriber_entitlements WHERE source_order_id=?", (normalized["payload"]["order_id"],)
            ).fetchone()
            disposition, reject_reason, state = self._classify(connection, row, normalized)
            try:
                cursor = connection.execute(
                    "INSERT INTO entitlement_event_log"
                    "(source_system,source_event_no,order_id,event_type,business_version,subscriber_hash,scenario_code,"
                    "product_code,occurred_at,valid_from,valid_until,new_valid_until,reason,payload_json,payload_digest,"
                    "disposition,reject_reason,recorded_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        normalized["payload"]["source_system"],
                        normalized["payload"]["source_event_no"],
                        normalized["payload"]["order_id"],
                        normalized["payload"]["event_type"],
                        normalized["payload"]["business_version"],
                        normalized["payload"]["subscriber_hash"],
                        normalized["payload"]["scenario_code"],
                        normalized["payload"]["product_code"],
                        normalized["payload"]["occurred_at"],
                        normalized["payload"]["valid_from"],
                        normalized["payload"]["valid_until"],
                        normalized["payload"]["new_valid_until"],
                        normalized["payload"]["reason"],
                        payload_json,
                        digest,
                        disposition,
                        reject_reason,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                # 并发下同一来源事件号已被其他请求归并：按去重命中处理
                existing = connection.execute(
                    "SELECT * FROM entitlement_event_log WHERE source_system=? AND source_event_no=? AND disposition='accepted'",
                    (normalized["payload"]["source_system"], normalized["payload"]["source_event_no"]),
                ).fetchone()
                if existing is None or existing["payload_digest"] != digest:
                    raise ConflictError("相同来源事件号对应了不同的事件内容") from exc
                return {
                    "event": self._record(existing),
                    "projection": self._projection_payload(connection, existing["order_id"]),
                    "duplicate": True,
                }
            if disposition == "accepted" and state is not None:
                self._upsert_projection(connection, scenario["id"], row, state, cursor.lastrowid, now)
            record = connection.execute("SELECT * FROM entitlement_event_log WHERE seq=?", (cursor.lastrowid,)).fetchone()
            return {
                "event": self._record(record),
                "projection": self._projection_payload(connection, normalized["payload"]["order_id"]),
                "duplicate": False,
            }

    def ingest_batch(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        results = []
        counts = {"accepted": 0, "duplicates": 0, "rejected": 0}
        for item in items:
            result = self.ingest_event(item)
            results.append(result)
            if result["duplicate"]:
                counts["duplicates"] += 1
            elif result["event"]["disposition"] == "accepted":
                counts["accepted"] += 1
            else:
                counts["rejected"] += 1
        return {"items": results, **counts}

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def order_timeline(self, order_id: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM entitlement_event_log WHERE order_id=? ORDER BY seq", (order_id,)
        ).fetchall()
        projection = self._projection_payload(self.connection, order_id)
        if not rows and projection is None:
            raise NotFoundError("订单不存在")
        return {
            "order_id": order_id,
            "evaluated_at": to_storage(self.clock.now()),
            "projection": projection,
            "events": [self._record(row) for row in rows],
        }

    def order_detail(self, order_id: str) -> dict[str, Any]:
        projection = self._projection_payload(self.connection, order_id)
        counts = self.connection.execute(
            "SELECT disposition,COUNT(*) FROM entitlement_event_log WHERE order_id=? GROUP BY disposition", (order_id,)
        ).fetchall()
        if projection is None and not counts:
            raise NotFoundError("订单不存在")
        return {
            "order_id": order_id,
            "evaluated_at": to_storage(self.clock.now()),
            "projection": projection,
            "event_counts": {row[0]: row[1] for row in counts},
        }

    def current_entitlement(self, subscriber_hash: str, scenario_code: str, at: str | None = None) -> dict[str, Any]:
        scenario = self.connection.execute("SELECT * FROM network_scenarios WHERE code=?", (scenario_code,)).fetchone()
        if scenario is None:
            raise NotFoundError("网络场景不存在")
        moment = self._parse_moment(at) if at else self.clock.now()
        rows = self.connection.execute(
            "SELECT * FROM subscriber_entitlements WHERE subscriber_hash=? AND scenario_id=? ORDER BY source_order_id",
            (subscriber_hash, scenario["id"]),
        ).fetchall()
        states = [self._state_from_row(self.connection, row) for row in rows]
        view = entitlement_view(states, moment)
        return {
            "subscriber_hash": subscriber_hash,
            "scenario_code": scenario_code,
            "evaluated_at": to_storage(moment),
            "eligible": view.eligible,
            "eligible_order_ids": list(view.eligible_order_ids),
            "orders": [self._order_payload(state, moment) for state in states],
            "coverage_windows": [self._window_payload(window) for window in view.coverage_windows],
            "active_windows": [self._window_payload(window) for window in view.active_windows],
            "overlaps": [{"order_ids": [left, right]} for left, right in view.crossings],
        }

    # ------------------------------------------------------------------
    # 对账与重建
    # ------------------------------------------------------------------
    def reconcile_order(self, order_id: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM entitlement_event_log WHERE order_id=? ORDER BY business_version,seq", (order_id,)
        ).fetchall()
        projection = self.connection.execute(
            "SELECT * FROM subscriber_entitlements WHERE source_order_id=?", (order_id,)
        ).fetchone()
        if not rows and projection is None:
            raise NotFoundError("订单不存在")
        accepted = [row for row in rows if row["disposition"] == "accepted"]
        counts = {"accepted": 0, "duplicate": 0, "stale": 0, "invalid": 0}
        for row in rows:
            counts[row["disposition"]] += 1
        canonical: OrderState | None = None
        diffs: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        if accepted:
            versions = [row["business_version"] for row in accepted]
            if versions != list(range(1, len(versions) + 1)):
                diffs.append({"check": "version_contiguity", "expected": list(range(1, len(versions) + 1)), "actual": versions})
            last_seq = max(row["seq"] for row in accepted)
            if projection is not None and projection["last_event_seq"] != last_seq:
                diffs.append({"check": "last_event_seq", "expected": last_seq, "actual": projection["last_event_seq"]})
            try:
                canonical = self._canonical_state(accepted)
            except ConflictError as exc:
                diffs.append({"check": "canonical_fold", "expected": "foldable", "actual": exc.context.get("reason", "failed")})
            if canonical is not None:
                diffs.extend(self._compare(projection, canonical))
        elif projection is not None:
            # 没有账本事件却有投影：旧系统迁移数据，单独提示而不算差异
            warnings.append({"check": "projection_without_ledger", "message": "投影没有对应的账本事件（可能来自旧系统）"})
        return {
            "order_id": order_id,
            "consistent": not diffs,
            "diffs": diffs,
            "warnings": warnings,
            "event_counts": counts,
            "projection": self._projection_payload(self.connection, order_id),
            "canonical": self._state_payload(canonical) if canonical else None,
        }

    def reconcile_all(self) -> dict[str, Any]:
        order_ids = {
            row[0]
            for row in self.connection.execute("SELECT DISTINCT order_id FROM entitlement_event_log").fetchall()
        }
        order_ids.update(
            row[0] for row in self.connection.execute("SELECT source_order_id FROM subscriber_entitlements").fetchall()
        )
        results = [self.reconcile_order(order_id) for order_id in sorted(order_ids)]
        return {
            "checked_orders": len(results),
            "consistent": all(item["consistent"] for item in results),
            "orders": results,
        }

    def rebuild_projection(self) -> dict[str, Any]:
        """按账本重建全部投影；无账本事件的旧投影保留并单独报告。"""
        with _immediate_transaction(self.connection) as connection:
            ledger_order_ids = {
                row[0] for row in connection.execute("SELECT DISTINCT order_id FROM entitlement_event_log").fetchall()
            }
            accepted = connection.execute(
                "SELECT * FROM entitlement_event_log WHERE disposition='accepted' ORDER BY order_id,business_version,seq"
            ).fetchall()
            by_order: dict[str, list[sqlite3.Row]] = {}
            for row in accepted:
                by_order.setdefault(row["order_id"], []).append(row)
            existing = {
                row["source_order_id"]: row
                for row in connection.execute("SELECT * FROM subscriber_entitlements").fetchall()
            }
            repairs: list[dict[str, Any]] = []
            unchanged = 0
            for order_id, events in sorted(by_order.items()):
                canonical = self._canonical_state(events)
                assert canonical is not None
                row = existing.get(order_id)
                last_seq = max(event["seq"] for event in events)
                if row is not None and not self._compare(row, canonical) and row["last_event_seq"] == last_seq:
                    unchanged += 1
                    continue
                before = self._projection_payload(connection, order_id)
                scenario_id = connection.execute(
                    "SELECT id FROM network_scenarios WHERE code=?", (events[0]["scenario_code"],)
                ).fetchone()
                if scenario_id is None:
                    raise ConflictError("账本事件引用的场景不存在，无法重建投影", context={"order_id": order_id})
                created_at = row["created_at"] if row is not None else events[0]["recorded_at"]
                updated_at = events[-1]["recorded_at"]
                self._write_projection(connection, scenario_id[0], row, canonical, last_seq, created_at, updated_at)
                repairs.append({
                    "order_id": order_id,
                    "before": before,
                    "after": self._projection_payload(connection, order_id),
                })
            removed = 0
            for order_id, row in sorted(existing.items()):
                if order_id in by_order or order_id not in ledger_order_ids:
                    continue
                # 账本中没有 accepted 事件却存在投影：视为脏数据移除
                connection.execute("DELETE FROM subscriber_entitlements WHERE source_order_id=?", (order_id,))
                repairs.append({"order_id": order_id, "before": dict(row), "after": None})
                removed += 1
            preserved = sum(1 for order_id in existing if order_id not in ledger_order_ids)
            return {
                "rebuilt_orders": len(repairs) - removed,
                "removed_projections": removed,
                "unchanged_orders": unchanged,
                "preserved_without_ledger": preserved,
                "repairs": repairs,
                "consistent": True,
            }

    # ------------------------------------------------------------------
    # 内部：分类与归并
    # ------------------------------------------------------------------
    def _classify(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row | None,
        normalized: dict[str, Any],
    ) -> tuple[str, str, OrderState | None]:
        payload = normalized["payload"]
        event = normalized["event"]
        if row is None:
            try:
                state = apply_event(
                    None,
                    event,
                    order_id=payload["order_id"],
                    subscriber_hash=payload["subscriber_hash"],
                    scenario_code=payload["scenario_code"],
                    product_code=payload["product_code"],
                )
            except LedgerRuleError as exc:
                return "invalid", exc.reason, None
            return "accepted", "", state
        if (
            row["subscriber_hash"] != payload["subscriber_hash"]
            or row["product_code"] != payload["product_code"]
            or self._scenario_code(connection, row["scenario_id"]) != payload["scenario_code"]
        ):
            return "invalid", "order_field_mismatch", None
        state = self._state_from_row(connection, row)
        try:
            updated = apply_event(state, event)
        except LedgerRuleError as exc:
            return ("stale" if exc.reason == "stale_version" else "invalid"), exc.reason, None
        return "accepted", "", updated

    def _canonical_state(self, accepted: list[sqlite3.Row]) -> OrderState | None:
        if not accepted:
            return None
        first = accepted[0]
        try:
            return fold_events(
                [self._to_domain(row) for row in accepted],
                order_id=first["order_id"],
                subscriber_hash=first["subscriber_hash"],
                scenario_code=first["scenario_code"],
                product_code=first["product_code"],
            )
        except LedgerRuleError as exc:
            raise ConflictError(
                "账本中的已接受事件无法归并，请先修复数据",
                context={"order_id": first["order_id"], "reason": exc.reason},
            ) from exc

    def _compare(self, row: sqlite3.Row | None, state: OrderState | None) -> list[dict[str, Any]]:
        if row is None and state is None:
            return []
        if row is None:
            return [{"check": "projection", "expected": "present", "actual": "missing"}]
        if state is None:
            return [{"check": "projection", "expected": "absent", "actual": "present"}]
        diffs: list[dict[str, Any]] = []
        actual = {
            "valid_from": row["valid_from"],
            "valid_until": row["valid_until"],
            "state": row["state"],
            "suspended_at": row["suspended_at"],
            "refunded_at": row["refunded_at"],
            "applied_version": row["applied_version"],
        }
        expected = {
            "valid_from": to_storage(state.valid_from),
            "valid_until": to_storage(state.valid_until),
            "state": state.status,
            "suspended_at": to_storage(state.suspended_at) if state.suspended_at else None,
            "refunded_at": to_storage(state.refunded_at) if state.refunded_at else None,
            "applied_version": state.applied_version,
        }
        for field in BUSINESS_FIELDS:
            if actual[field] != expected[field]:
                diffs.append({"check": field, "expected": expected[field], "actual": actual[field]})
        return diffs

    # ------------------------------------------------------------------
    # 内部：投影读写
    # ------------------------------------------------------------------
    def _upsert_projection(
        self,
        connection: sqlite3.Connection,
        scenario_id: int,
        row: sqlite3.Row | None,
        state: OrderState,
        event_seq: int,
        now: str,
    ) -> None:
        self._write_projection(connection, scenario_id, row, state, event_seq, now, now)

    def _write_projection(
        self,
        connection: sqlite3.Connection,
        scenario_id: int,
        row: sqlite3.Row | None,
        state: OrderState,
        event_seq: int,
        created_at: str,
        updated_at: str,
    ) -> None:
        values = (
            to_storage(state.valid_from),
            to_storage(state.valid_until),
            state.status,
            state.applied_version,
            event_seq,
            to_storage(state.suspended_at) if state.suspended_at else None,
            to_storage(state.refunded_at) if state.refunded_at else None,
            updated_at,
        )
        if row is None:
            connection.execute(
                "INSERT INTO subscriber_entitlements"
                "(subscriber_hash,scenario_id,product_code,valid_from,valid_until,state,source_order_id,"
                "applied_version,last_event_seq,suspended_at,refunded_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    state.subscriber_hash,
                    scenario_id,
                    state.product_code,
                    values[0],
                    values[1],
                    values[2],
                    state.order_id,
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                    created_at,
                    values[7],
                ),
            )
        else:
            connection.execute(
                "UPDATE subscriber_entitlements SET valid_from=?,valid_until=?,state=?,applied_version=?,"
                "last_event_seq=?,suspended_at=?,refunded_at=?,updated_at=? WHERE source_order_id=?",
                (*values, state.order_id),
            )

    def _projection_payload(self, connection: sqlite3.Connection, order_id: str) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT * FROM subscriber_entitlements WHERE source_order_id=?", (order_id,)
        ).fetchone()
        if row is None:
            return None
        state = self._state_from_row(connection, row)
        return self._order_payload(state, self.clock.now())

    def _order_payload(self, state: OrderState, at: datetime) -> dict[str, Any]:
        return {
            "order_id": state.order_id,
            "subscriber_hash": state.subscriber_hash,
            "scenario_code": state.scenario_code,
            "product_code": state.product_code,
            "valid_from": to_storage(state.valid_from),
            "valid_until": to_storage(state.valid_until),
            "status": state.status,
            "derived_status": state.derived_status(at),
            "eligible": state.eligible_at(at),
            "applied_version": state.applied_version,
            "suspended_at": to_storage(state.suspended_at) if state.suspended_at else None,
            "refunded_at": to_storage(state.refunded_at) if state.refunded_at else None,
        }

    def _state_payload(self, state: OrderState) -> dict[str, Any]:
        payload = self._order_payload(state, self.clock.now())
        payload["purchased_at"] = to_storage(state.purchased_at)
        return payload

    @staticmethod
    def _window_payload(window: tuple[datetime, datetime]) -> dict[str, Any]:
        return {"valid_from": to_storage(window[0]), "valid_until": to_storage(window[1])}

    # ------------------------------------------------------------------
    # 内部：行转换与校验
    # ------------------------------------------------------------------
    def _state_from_row(self, connection: sqlite3.Connection, row: sqlite3.Row) -> OrderState:
        status = row["state"]
        if status == "cancelled":
            status = "refunded"  # 旧库状态映射：取消视为终态退款
        elif status == "expired":
            status = "active"  # 到期是派生状态，由有效期在查询时计算
        return OrderState(
            order_id=row["source_order_id"],
            subscriber_hash=row["subscriber_hash"],
            scenario_code=self._scenario_code(connection, row["scenario_id"]),
            product_code=row["product_code"],
            valid_from=from_storage(row["valid_from"]),
            valid_until=from_storage(row["valid_until"]),
            status=status,
            applied_version=int(row["applied_version"]),
            purchased_at=from_storage(row["created_at"]),
            suspended_at=from_storage(row["suspended_at"]),
            refunded_at=from_storage(row["refunded_at"]),
        )

    @staticmethod
    def _to_domain(row: sqlite3.Row) -> LedgerEvent:
        return LedgerEvent(
            event_type=row["event_type"],
            business_version=int(row["business_version"]),
            occurred_at=from_storage(row["occurred_at"]),
            valid_from=from_storage(row["valid_from"]),
            valid_until=from_storage(row["valid_until"]),
            new_valid_until=from_storage(row["new_valid_until"]),
        )

    def _scenario_code(self, connection: sqlite3.Connection, scenario_id: int) -> str:
        cache = getattr(self, "_scenario_cache", None)
        if cache is None:
            cache = {}
            self._scenario_cache = cache
        if scenario_id not in cache:
            row = connection.execute("SELECT code FROM network_scenarios WHERE id=?", (scenario_id,)).fetchone()
            cache[scenario_id] = row["code"] if row else ""
        return cache[scenario_id]

    @staticmethod
    def _record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "seq": row["seq"],
            "source_system": row["source_system"],
            "source_event_no": row["source_event_no"],
            "order_id": row["order_id"],
            "event_type": row["event_type"],
            "business_version": row["business_version"],
            "subscriber_hash": row["subscriber_hash"],
            "scenario_code": row["scenario_code"],
            "product_code": row["product_code"],
            "occurred_at": row["occurred_at"],
            "valid_from": row["valid_from"],
            "valid_until": row["valid_until"],
            "new_valid_until": row["new_valid_until"],
            "reason": row["reason"],
            "payload_digest": row["payload_digest"],
            "disposition": row["disposition"],
            "reject_reason": row["reject_reason"],
            "recorded_at": row["recorded_at"],
        }

    def _validate(self, payload: dict[str, Any]) -> dict[str, Any]:
        def require(key: str) -> Any:
            value = payload.get(key)
            if value is None or value == "":
                raise ValidationError(f"缺少必填字段 {key}")
            return value

        occurred = self._parse_moment(require("occurred_at"), "事件生效时间格式不正确")
        event_type = require("event_type")
        if event_type not in EVENT_TYPES:
            raise ValidationError(f"不支持的事件类型 {event_type}")
        valid_from = valid_until = new_valid_until = None
        if event_type == "purchase":
            valid_from = self._parse_moment(require("valid_from"), "权益有效期格式不正确")
            valid_until = self._parse_moment(require("valid_until"), "权益有效期格式不正确")
            if valid_until <= valid_from:
                raise ValidationError("权益结束时间必须晚于开始时间")
        if event_type == "extend":
            new_valid_until = self._parse_moment(require("new_valid_until"), "延期结束时间格式不正确")
        try:
            version = int(require("business_version"))
        except (TypeError, ValueError) as exc:
            raise ValidationError("业务版本必须是正整数") from exc
        if version < 1:
            raise ValidationError("业务版本必须是正整数")
        normalized_payload = {
            "source_system": payload.get("source_system") or "carrier-app",
            "source_event_no": require("source_event_no"),
            "order_id": require("order_id"),
            "event_type": event_type,
            "business_version": version,
            "subscriber_hash": require("subscriber_hash"),
            "scenario_code": require("scenario_code"),
            "product_code": require("product_code"),
            "occurred_at": to_storage(occurred),
            "valid_from": to_storage(valid_from) if valid_from else None,
            "valid_until": to_storage(valid_until) if valid_until else None,
            "new_valid_until": to_storage(new_valid_until) if new_valid_until else None,
            "reason": payload.get("reason") or "",
        }
        event = LedgerEvent(
            event_type=event_type,
            business_version=version,
            occurred_at=occurred,
            valid_from=valid_from,
            valid_until=valid_until,
            new_valid_until=new_valid_until,
        )
        return {"payload": normalized_payload, "event": event}

    @staticmethod
    def _parse_moment(value: str, message: str = "时间格式不正确") -> datetime:
        try:
            parsed = from_storage(value)
        except (ValueError, TypeError) as exc:
            raise ValidationError(message) from exc
        if parsed is None:
            raise ValidationError(message)
        return parsed
