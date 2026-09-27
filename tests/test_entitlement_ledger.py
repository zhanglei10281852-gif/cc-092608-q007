"""用户权益事件账本测试。

覆盖：
- 纯规则：暂停顺延、退款终态、到期后续期、过时/跳号拒绝、有效期交叉并集；
- API：来源事件号去重、摘要冲突、乱序拒绝后重放归并、时间线、当前权益、账本不可变；
- 等价性：同一段打乱顺序的请求流分别在线处理到两个数据库，
  结果逐行一致；accepted 事件在第三种随机顺序下重放到第三个数据库，
  投影仍然一致；投影删除重建与人为破坏后修复均回到相同状态。
"""

from __future__ import annotations

import os
import random
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.clock import to_storage
from app.database import close_connection, get_connection
from app.main import app
from app.network.ledger import LedgerEvent, LedgerRuleError, apply_event, entitlement_view, fold_events
from app.network.ledger_service import EntitlementLedgerService

SCENARIO = "gdh-rail"
SUBSCRIBER = "subscriber-ledger-0000001"
SUBSCRIBER2 = "subscriber-ledger-0000002"
SUBSCRIBER3 = "subscriber-ledger-0000003"
D0 = datetime(2026, 9, 1, tzinfo=UTC)


def at(day: int, hour: int = 0) -> str:
    return to_storage(D0 + timedelta(days=day, hours=hour))


def event(source_event_no, order_id, event_type, version, *, subscriber=SUBSCRIBER, occurred=None,
          valid_from=None, valid_until=None, new_valid_until=None, source_system="carrier-app", reason=""):
    payload = {
        "source_system": source_system,
        "source_event_no": source_event_no,
        "order_id": order_id,
        "event_type": event_type,
        "business_version": version,
        "subscriber_hash": subscriber,
        "scenario_code": SCENARIO,
        "product_code": "rail-boost-day",
        "occurred_at": occurred or at(1),
        "reason": reason,
    }
    if valid_from is not None:
        payload["valid_from"] = valid_from
    if valid_until is not None:
        payload["valid_until"] = valid_until
    if new_valid_until is not None:
        payload["new_valid_until"] = new_valid_until
    return payload


def purchase(no, order_id, *, start=0, end=10, subscriber=SUBSCRIBER, occurred=None):
    return event(no, order_id, "purchase", 1, subscriber=subscriber,
                 occurred=occurred or at(start), valid_from=at(start), valid_until=at(end))


def make_live_scenario(client: TestClient) -> None:
    response = client.post(
        "/api/network/scenarios",
        json={"code": SCENARIO, "name": "广深高铁", "scene_type": "railway",
              "timezone": "Asia/Shanghai", "max_concurrent_sessions": 100, "capacity_mbps": 3000},
    )
    assert response.status_code in (201, 409), response.text


def open_client(db_path: Path) -> TestClient:
    """打开指向独立数据库的 TestClient 并保持生命周期（线程局部连接已切换）。"""
    os.environ["NETWORK_DATABASE_PATH"] = str(db_path)
    close_connection()
    client = TestClient(app)
    client.__enter__()
    make_live_scenario(client)
    return client


def bind_database(db_path: Path) -> None:
    """把线程局部连接切回指定数据库（供直接 SQL 与服务调用使用）。"""
    os.environ["NETWORK_DATABASE_PATH"] = str(db_path)
    close_connection()


def direct_connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


# ---------------------------------------------------------------------------
# 纯领域规则
# ---------------------------------------------------------------------------

def test_pure_rules_suspend_resume_shifts_window():
    state = apply_event(None, LedgerEvent("purchase", 1, D0, valid_from=D0, valid_until=D0 + timedelta(days=10)),
                        order_id="A")
    assert state.status == "active"
    state = apply_event(state, LedgerEvent("suspend", 2, D0 + timedelta(days=3)))
    assert state.status == "suspended"
    state = apply_event(state, LedgerEvent("resume", 3, D0 + timedelta(days=5)))
    assert state.status == "active"
    assert state.valid_until == D0 + timedelta(days=12)  # 顺延整个挂起时长 2 天


def test_pure_rules_refund_is_terminal_and_expire_allows_renewal():
    state = fold_events(
        [
            LedgerEvent("purchase", 1, D0, valid_from=D0, valid_until=D0 + timedelta(days=10)),
            LedgerEvent("expire", 2, D0 + timedelta(days=8)),
            LedgerEvent("extend", 3, D0 + timedelta(days=8), new_valid_until=D0 + timedelta(days=20)),
            LedgerEvent("refund", 4, D0 + timedelta(days=9)),
        ],
        order_id="A", subscriber_hash=SUBSCRIBER, scenario_code=SCENARIO, product_code="p",
    )
    assert state.status == "refunded"
    assert state.valid_until == D0 + timedelta(days=20)  # 到期不是终态，到期后续期生效
    with pytest.raises(LedgerRuleError) as info:
        apply_event(state, LedgerEvent("extend", 5, D0 + timedelta(days=10), new_valid_until=D0 + timedelta(days=30)))
    assert info.value.reason == "order_refunded"  # 退款后续期必须创建新订单


def test_pure_rules_stale_version_and_gap_rejected():
    state = apply_event(None, LedgerEvent("purchase", 1, D0, valid_from=D0, valid_until=D0 + timedelta(days=10)),
                        order_id="A")
    suspended = apply_event(state, LedgerEvent("suspend", 2, D0))
    assert suspended.status == "suspended"
    with pytest.raises(LedgerRuleError) as info:
        apply_event(suspended, LedgerEvent("suspend", 2, D0))  # 同一旧版本重放
    assert info.value.reason == "stale_version"
    with pytest.raises(LedgerRuleError) as info:
        apply_event(suspended, LedgerEvent("resume", 2, D0))  # 版本 <= 已归并
    assert info.value.reason == "stale_version"
    with pytest.raises(LedgerRuleError) as info:
        apply_event(suspended, LedgerEvent("resume", 4, D0))  # 业务版本跳号
    assert info.value.reason == "version_gap"


def test_pure_rules_first_event_must_be_purchase():
    with pytest.raises(LedgerRuleError) as info:
        apply_event(None, LedgerEvent("refund", 1, D0))
    assert info.value.reason == "first_event_must_be_purchase_v1"
    with pytest.raises(LedgerRuleError) as info:
        apply_event(None, LedgerEvent("purchase", 2, D0, valid_from=D0, valid_until=D0 + timedelta(days=1)))
    assert info.value.reason == "first_event_must_be_purchase_v1"


def test_pure_rules_overlapping_windows_union():
    common = dict(subscriber_hash=SUBSCRIBER, scenario_code=SCENARIO, product_code="p")
    a = apply_event(None, LedgerEvent("purchase", 1, D0, valid_from=D0 + timedelta(days=5), valid_until=D0 + timedelta(days=12)),
                    order_id="A", **common)
    b = apply_event(None, LedgerEvent("purchase", 1, D0, valid_from=D0 + timedelta(days=8), valid_until=D0 + timedelta(days=15)),
                    order_id="B", **common)
    view = entitlement_view([a, b], D0 + timedelta(days=10))
    assert view.eligible is True
    assert set(view.eligible_order_ids) == {"A", "B"}
    assert view.coverage_windows == ((D0 + timedelta(days=5), D0 + timedelta(days=15)),)
    assert view.crossings == (("A", "B"),)
    assert entitlement_view([a, b], D0 + timedelta(days=13)).eligible_order_ids == ("B",)


# ---------------------------------------------------------------------------
# API：去重、版本拒绝、时间线、当前权益
# ---------------------------------------------------------------------------

def test_ingest_lifecycle_dedup_and_stale_rejection(client):
    make_live_scenario(client)
    first = client.post("/api/network/ledger/events", json=purchase("evt-0001", "order-A", start=1, end=10))
    assert first.status_code == 201, first.text

    suspend = client.post("/api/network/ledger/events", json=event("evt-0002", "order-A", "suspend", 2, occurred=at(3)))
    assert suspend.status_code == 201

    # 同一来源事件号重复送达 -> 去重
    duplicate = client.post("/api/network/ledger/events", json=event("evt-0002", "order-A", "suspend", 2, occurred=at(3)))
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True

    # 重复事件号但内容不同 -> 冲突
    tampered = client.post("/api/network/ledger/events", json=event("evt-0002", "order-A", "suspend", 2, occurred=at(4)))
    assert tampered.status_code == 409

    # 恢复后窗口顺延 2 天
    resume = client.post("/api/network/ledger/events", json=event("evt-0003", "order-A", "resume", 3, occurred=at(5)))
    assert resume.status_code == 201
    assert resume.json()["projection"]["valid_until"] == at(12)

    # 旧版本重放 -> 过时写入，账本留痕但不改变投影
    stale = client.post("/api/network/ledger/events", json=event("evt-0002b", "order-A", "suspend", 2, occurred=at(3)))
    assert stale.status_code == 409
    assert stale.json()["event"]["disposition"] == "stale"
    assert stale.json()["event"]["reject_reason"] == "stale_version"
    assert stale.json()["projection"]["valid_until"] == at(12)

    # 跳号 -> 版本间隙
    gap = client.post("/api/network/ledger/events", json=event("evt-0009", "order-A", "refund", 9, occurred=at(8)))
    assert gap.status_code == 409
    assert gap.json()["event"]["disposition"] == "invalid"
    assert gap.json()["event"]["reject_reason"] == "version_gap"


def test_gap_event_can_merge_after_missing_version_replay(client):
    make_live_scenario(client)
    client.post("/api/network/ledger/events", json=purchase("evt-1001", "order-G", start=1, end=10))
    # v3 提前到达：间隙拒绝但留痕
    early = client.post("/api/network/ledger/events", json=event("evt-1003", "order-G", "resume", 3, occurred=at(4)))
    assert early.status_code == 409
    assert early.json()["event"]["reject_reason"] == "version_gap"
    # v2 到达后，重放同一 v3 事件 -> 归并成功
    assert client.post("/api/network/ledger/events", json=event("evt-1002", "order-G", "suspend", 2, occurred=at(3))).status_code == 201
    replayed = client.post("/api/network/ledger/events", json=event("evt-1003", "order-G", "resume", 3, occurred=at(4)))
    assert replayed.status_code == 201
    assert replayed.json()["projection"]["applied_version"] == 3
    assert replayed.json()["duplicate"] is False
    # 再次重放 -> 去重
    assert client.post("/api/network/ledger/events", json=event("evt-1003", "order-G", "resume", 3, occurred=at(4))).status_code == 200


def test_refund_then_renewal_rejected(client):
    make_live_scenario(client)
    client.post("/api/network/ledger/events", json=purchase("evt-2001", "order-R", start=1, end=10))
    client.post("/api/network/ledger/events", json=event("evt-2002", "order-R", "refund", 2, occurred=at(5)))
    renewed = client.post("/api/network/ledger/events",
                          json=event("evt-2003", "order-R", "extend", 3, occurred=at(6), new_valid_until=at(20)))
    assert renewed.status_code == 409
    assert renewed.json()["event"]["reject_reason"] == "order_refunded"
    # 同一事件号反复重放仍然拒绝（退款是终态），每次拒绝都留痕
    again = client.post("/api/network/ledger/events",
                        json=event("evt-2003", "order-R", "extend", 3, occurred=at(6), new_valid_until=at(20)))
    assert again.status_code == 409
    timeline = client.get("/api/network/ledger/orders/order-R/timeline").json()
    assert [item["event_type"] for item in timeline["events"]] == ["purchase", "refund", "extend", "extend"]
    assert timeline["events"][-1]["disposition"] == "invalid"


def test_current_entitlement_explains_why_traveler_is_ineligible(client):
    make_live_scenario(client)
    client.post("/api/network/ledger/events",
                json=purchase("evt-3001", "order-S", start=1, end=10, subscriber=SUBSCRIBER2))
    client.post("/api/network/ledger/events",
                json=event("evt-3002", "order-S", "suspend", 2, occurred=at(4), subscriber=SUBSCRIBER2, reason="计费暂停"))
    # 暂停期间上车：客服可以从当前权益与时间线解释“为什么无资格”
    current = client.get("/api/network/ledger/entitlements/current",
                         params={"subscriber_hash": SUBSCRIBER2, "scenario_code": SCENARIO, "at": at(5)}).json()
    assert current["eligible"] is False
    assert current["orders"][0]["derived_status"] == "suspended"
    assert current["orders"][0]["suspended_at"] == at(4)
    timeline = client.get("/api/network/ledger/orders/order-S/timeline").json()
    assert [item["event_type"] for item in timeline["events"]] == ["purchase", "suspend"]
    assert timeline["events"][1]["reason"] == "计费暂停"


def test_timeline_overlaps_and_reconcile(client):
    make_live_scenario(client)
    client.post("/api/network/ledger/events", json=purchase("evt-4001", "order-X", start=5, end=12))
    client.post("/api/network/ledger/events", json=purchase("evt-4002", "order-Y", start=8, end=15))
    current = client.get("/api/network/ledger/entitlements/current",
                         params={"subscriber_hash": SUBSCRIBER, "scenario_code": SCENARIO, "at": at(10)}).json()
    assert current["eligible"] is True
    assert set(current["eligible_order_ids"]) == {"order-X", "order-Y"}
    assert len(current["coverage_windows"]) == 1  # 交叉窗口取并集
    assert current["overlaps"] == [{"order_ids": ["order-X", "order-Y"]}]

    expired = client.get("/api/network/ledger/entitlements/current",
                         params={"subscriber_hash": SUBSCRIBER, "scenario_code": SCENARIO, "at": at(16)}).json()
    assert expired["eligible"] is False
    assert {order["derived_status"] for order in expired["orders"]} == {"expired"}

    assert client.get("/api/network/ledger/orders/order-X/reconcile").json()["consistent"] is True
    assert client.get("/api/network/ledger/reconcile").json()["consistent"] is True


def test_ledger_table_is_immutable(client):
    make_live_scenario(client)
    client.post("/api/network/ledger/events", json=purchase("evt-5001", "order-I"))
    connection = get_connection()
    with pytest.raises(sqlite3.Error):
        connection.execute("UPDATE entitlement_event_log SET event_type='refund' WHERE source_event_no='evt-5001'")
    with pytest.raises(sqlite3.Error):
        connection.execute("DELETE FROM entitlement_event_log WHERE source_event_no='evt-5001'")
    # 触发器拦截后账本写入仍然正常
    follow = client.post("/api/network/ledger/events", json=event("evt-5002", "order-I", "refund", 2, occurred=at(5)))
    assert follow.status_code == 201


def test_unknown_order_and_scenario(client):
    make_live_scenario(client)
    assert client.get("/api/network/ledger/orders/missing-order/timeline").status_code == 404
    body = purchase("evt-6001", "order-Z", start=1, end=2)
    body["scenario_code"] = "no-such-scene"
    # 未知场景不写入账本
    response = client.post("/api/network/ledger/events", json=body)
    assert response.status_code == 404
    assert client.get("/api/network/ledger/entitlements/current",
                      params={"subscriber_hash": SUBSCRIBER, "scenario_code": "no-such-scene"}).status_code == 404


def test_suspended_traveler_denied_acceleration_and_explained(client):
    """旅客上车后被判无资格的完整故事：加速资格随账本事件实时变化，时间线给出解释。"""
    from app.network.rules import DEFAULT_RULES

    make_live_scenario(client)
    client.post(
        "/api/network/scenarios/gdh-rail/segments",
        json={"code": "gz-sz-01", "name": "广州南至虎门", "sequence_no": 1, "expected_dwell_seconds": 900, "capacity_mbps": 1200},
    )
    client.post(
        "/api/network/applications",
        json={"app_code": "video-call", "name": "视频通话", "category": "video_call", "latency_target_ms": 100,
              "packet_loss_target": 0.01, "min_downlink_mbps": 8, "min_uplink_mbps": 4, "default_priority": 70},
    )
    policy = client.post("/api/network/scenarios/gdh-rail/policies", json={"rules": DEFAULT_RULES, "actor": "tests"}).json()
    client.post(f"/api/network/policies/{policy['id']}/publish",
                json={"actor": "tests", "effective_from": "2020-01-01T00:00:00Z"})

    now = datetime.now(UTC)
    subscriber = "subscriber-traveler-00001"
    sample = {
        "sample_key": "traveler-sample-1", "scenario_code": SCENARIO, "segment_code": "gz-sz-01",
        "app_code": "video-call", "subscriber_hash": subscriber, "device_class": "phone",
        "train_speed_kmh": 300, "latency_ms": 350, "packet_loss": 0.08,
        "downlink_mbps": 1.5, "uplink_mbps": 0.5, "observed_at": to_storage(now),
    }
    client.post("/api/network/ledger/events", json=event(
        "evt-t1", "order-traveler", "purchase", 1, subscriber=subscriber,
        occurred=to_storage(now - timedelta(hours=2)),
        valid_from=to_storage(now - timedelta(hours=2)), valid_until=to_storage(now + timedelta(days=1)),
    ))
    # 权益有效：可以启动加速
    first = client.post("/api/network/samples", json=sample).json()
    started = client.post(f"/api/network/incidents/{first['incident_id']}/accelerate", json={"actor": "tests"})
    assert started.status_code == 200, started.text
    client.post(f"/api/network/sessions/{started.json()['id']}/finish",
                json={"actor": "tests", "reason": "体验恢复", "result": "completed"})

    # 用户暂停权益后再次上车：加速被拒，当前权益与时间线解释原因
    client.post("/api/network/ledger/events", json=event(
        "evt-t2", "order-traveler", "suspend", 2, subscriber=subscriber,
        occurred=to_storage(now - timedelta(minutes=30)), reason="用户主动暂停",
    ))
    second = client.post("/api/network/samples", json={**sample, "sample_key": "traveler-sample-2"}).json()
    denied = client.post(f"/api/network/incidents/{second['incident_id']}/accelerate", json={"actor": "tests"})
    assert denied.status_code == 409
    current = client.get("/api/network/ledger/entitlements/current",
                         params={"subscriber_hash": subscriber, "scenario_code": SCENARIO}).json()
    assert current["eligible"] is False
    assert current["orders"][0]["derived_status"] == "suspended"
    timeline = client.get("/api/network/ledger/orders/order-traveler/timeline").json()
    assert [item["event_type"] for item in timeline["events"]] == ["purchase", "suspend"]
    assert timeline["events"][1]["reason"] == "用户主动暂停"

    # 恢复后（有效期顺延挂起时长）可以再次加速
    client.post("/api/network/ledger/events", json=event(
        "evt-t3", "order-traveler", "resume", 3, subscriber=subscriber, occurred=to_storage(now),
    ))
    third = client.post("/api/network/samples", json={**sample, "sample_key": "traveler-sample-3"}).json()
    restarted = client.post(f"/api/network/incidents/{third['incident_id']}/accelerate", json={"actor": "tests"})
    assert restarted.status_code == 200, restarted.text


def test_legacy_entitlement_endpoint_flows_through_ledger(client):
    make_live_scenario(client)
    payload = {
        "subscriber_hash": SUBSCRIBER,
        "scenario_code": SCENARIO,
        "product_code": "rail-boost-day",
        "valid_from": at(1),
        "valid_until": at(10),
        "source_order_id": "order-legacy-1",
    }
    first = client.post("/api/network/entitlements", json=payload)
    assert first.status_code == 201, first.text
    assert first.json()["status"] == "active"
    # 重复登记幂等返回同一投影
    assert client.post("/api/network/entitlements", json=payload).status_code == 201
    # 旧入口的购买已写入账本，投影与账本对账一致
    timeline = client.get("/api/network/ledger/orders/order-legacy-1/timeline").json()
    assert [item["event_type"] for item in timeline["events"]] == ["purchase"]
    assert timeline["events"][0]["source_system"] == "legacy-api"
    assert client.get("/api/network/ledger/orders/order-legacy-1/reconcile").json()["consistent"] is True


def test_legacy_projection_table_is_migrated(tmp_path: Path):
    """旧版权益表（无版本归并字段、无 refunded 状态）应被重建为投影表且数据保留。"""
    path = tmp_path / "legacy.db"
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE subscriber_entitlements ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "subscriber_hash TEXT NOT NULL,"
        "scenario_id INTEGER NOT NULL,"
        "product_code TEXT NOT NULL,"
        "valid_from TEXT NOT NULL,"
        "valid_until TEXT NOT NULL,"
        "state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended','expired','cancelled')),"
        "source_order_id TEXT NOT NULL UNIQUE,"
        "created_at TEXT NOT NULL,"
        "updated_at TEXT NOT NULL)"
    )
    connection.execute(
        "INSERT INTO subscriber_entitlements(subscriber_hash,scenario_id,product_code,valid_from,valid_until,"
        "state,source_order_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (SUBSCRIBER, 1, "rail-boost-day", at(1), at(10), "active", "order-old-1", at(1), at(1)),
    )
    connection.commit()

    from app.network.schema import ensure_network_schema

    ensure_network_schema(connection)
    columns = {row[1] for row in connection.execute("PRAGMA table_info(subscriber_entitlements)").fetchall()}
    assert {"applied_version", "last_event_seq", "suspended_at", "refunded_at"} <= columns
    row = connection.execute("SELECT * FROM subscriber_entitlements WHERE source_order_id='order-old-1'").fetchone()
    assert row["applied_version"] == 1
    # 新状态约束生效
    connection.execute(
        "INSERT INTO subscriber_entitlements(subscriber_hash,scenario_id,product_code,valid_from,valid_until,"
        "state,source_order_id,applied_version,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (SUBSCRIBER, 1, "rail-boost-day", at(2), at(11), "refunded", "order-old-2", 3, at(2), at(2)),
    )
    # 重复执行迁移是幂等的
    ensure_network_schema(connection)
    count = connection.execute("SELECT COUNT(*) FROM subscriber_entitlements").fetchone()[0]
    assert count == 2
    connection.close()


# ---------------------------------------------------------------------------
# 打乱顺序请求流：在线处理、跨库重放、删除重建三者一致
# ---------------------------------------------------------------------------

# 投影逐字段对比列（自增 id、记录时间与到达顺序相关，不参与业务等价比较）
PROJECTION_COLS = (
    "source_order_id,subscriber_hash,product_code,valid_from,valid_until,state,"
    "applied_version,suspended_at,refunded_at"
)
ACCEPTED_LEDGER_COLS = (
    "source_event_no,order_id,event_type,business_version,subscriber_hash,"
    "scenario_code,product_code,occurred_at,valid_from,valid_until,new_valid_until"
)


def dump_database(path: Path) -> dict:
    """导出跨处理顺序必须一致的内容：accepted 账本行与投影行。

    被拒（stale/invalid）的送达记录是到达顺序的产物，不参与等价比较。
    """
    connection = direct_connect(path)
    ledger = [dict(row) for row in connection.execute(
        f"SELECT {ACCEPTED_LEDGER_COLS} FROM entitlement_event_log WHERE disposition='accepted' "
        "ORDER BY source_event_no"
    ).fetchall()]
    projections = [dict(row) for row in connection.execute(
        f"SELECT {PROJECTION_COLS} FROM subscriber_entitlements ORDER BY source_order_id"
    ).fetchall()]
    connection.close()
    return {"accepted_ledger": ledger, "projections": projections}


def canonical_stream():
    """一个订单的完整生命周期 + 多个交叉订单；evt-A7 是退款后的续期，注定被拒。"""
    return [
        purchase("evt-A1", "order-A", start=1, end=10),
        event("evt-A2", "order-A", "suspend", 2, occurred=at(3)),
        event("evt-A3", "order-A", "resume", 3, occurred=at(5)),
        event("evt-A4", "order-A", "expire", 4, occurred=at(11)),
        event("evt-A5", "order-A", "extend", 5, occurred=at(11), new_valid_until=at(20)),
        event("evt-A6", "order-A", "refund", 6, occurred=at(15)),
        event("evt-A7", "order-A", "extend", 7, occurred=at(16), new_valid_until=at(30)),
        purchase("evt-B1", "order-B", start=8, end=18),
        purchase("evt-C1", "order-C", start=2, end=4, subscriber=SUBSCRIBER2),
        purchase("evt-D1", "order-D", start=5, end=12, subscriber=SUBSCRIBER3),
        purchase("evt-E1", "order-E", start=8, end=15, subscriber=SUBSCRIBER3),
    ]


EXPECTED_ACCEPTED = {
    "evt-A1", "evt-A2", "evt-A3", "evt-A4", "evt-A5", "evt-A6",
    "evt-B1", "evt-C1", "evt-D1", "evt-E1",
}


def post_batch(client: TestClient, items: list[dict]) -> dict:
    response = client.post("/api/network/ledger/events/batch", json={"items": items})
    assert response.status_code == 200, response.text
    return response.json()


def accepted_event_nos(path: Path) -> set[str]:
    connection = direct_connect(path)
    rows = {row[0] for row in connection.execute(
        "SELECT source_event_no FROM entitlement_event_log WHERE disposition='accepted'"
    ).fetchall()}
    connection.close()
    return rows


def converge(client: TestClient, path: Path, items: list[dict], seed: int) -> int:
    """反复以新的打乱顺序重放同一批事件，直到可归并事件全部归并。返回轮数。"""
    rng = random.Random(seed)
    for round_no in range(1, 31):
        shuffled = list(items)
        rng.shuffle(shuffled)
        post_batch(client, shuffled)
        if accepted_event_nos(path) >= EXPECTED_ACCEPTED:
            return round_no
    raise AssertionError("事件流在 30 轮重放后仍未收敛")


def tail_round() -> list[dict]:
    """收敛后的收尾重放：过早事件变为过时、退款后续期仍拒绝、已接受事件去重。"""
    return [
        event("evt-A2-early", "order-A", "suspend", 2, occurred=at(3)),
        event("evt-A4-early", "order-A", "expire", 4, occurred=at(11)),
        event("evt-A7", "order-A", "extend", 7, occurred=at(16), new_valid_until=at(30)),
        event("evt-A5", "order-A", "extend", 5, occurred=at(11), new_valid_until=at(20)),
    ]


def test_shuffled_online_replay_and_rebuild_are_equivalent(tmp_path: Path):
    items = canonical_stream()
    early_round = [
        event("evt-A2-early", "order-A", "suspend", 2, occurred=at(3)),
        event("evt-A4-early", "order-A", "expire", 4, occurred=at(11)),
    ]

    # ---- 在线处理 A：购买事件尚未到达时先有后续事件（注定拒绝），再反复打乱重放至收敛 ----
    path_a = tmp_path / "a.db"
    client_a = open_client(path_a)
    early = post_batch(client_a, early_round)
    assert {item["status"] for item in early["items"]} == {409}
    assert converge(client_a, path_a, items, seed=20260927) >= 1
    tail = post_batch(client_a, tail_round())
    tail_events = {item["event"]["source_event_no"]: item["event"] for item in tail["items"] if "event" in item}
    assert tail_events["evt-A2-early"]["disposition"] == "stale"
    assert tail_events["evt-A4-early"]["disposition"] == "stale"
    assert tail_events["evt-A7"]["disposition"] == "invalid"
    assert tail_events["evt-A7"]["reject_reason"] == "order_refunded"
    assert tail["duplicates"] == 1
    assert client_a.get("/api/network/ledger/reconcile").json()["consistent"] is True
    dump_a = dump_database(path_a)

    # ---- 在线处理 B：同一段流、不同打乱种子、不同数据库 ----
    path_b = tmp_path / "b.db"
    client_b = open_client(path_b)
    assert {item["status"] for item in post_batch(client_b, early_round)["items"]} == {409}
    converge(client_b, path_b, items, seed=987654321)
    post_batch(client_b, tail_round())
    # 1) 两次在线处理的 accepted 账本行与投影逐行一致
    assert dump_database(path_b) == dump_a
    assert client_b.get("/api/network/ledger/reconcile").json()["consistent"] is True
    client_b.__exit__(None, None, None)

    # ---- 重放 C：只取 A 的 accepted 事件，用第三种种子反复打乱重放到新数据库 ----
    path_c = tmp_path / "c.db"
    client_c = open_client(path_c)
    client_c.__exit__(None, None, None)
    source = direct_connect(path_a)
    import json

    accepted_payloads = [
        json.loads(row[0]) for row in source.execute(
            "SELECT payload_json FROM entitlement_event_log WHERE disposition='accepted'"
        ).fetchall()
    ]
    source.close()
    bind_database(path_c)
    service = EntitlementLedgerService(get_connection())
    rng = random.Random(424242)
    for _ in range(30):
        shuffled = list(accepted_payloads)
        rng.shuffle(shuffled)
        for raw in shuffled:
            service.ingest_event(raw)
        if dump_database(path_c)["projections"] == dump_a["projections"]:
            break
    else:
        raise AssertionError("accepted 事件重放未收敛到相同投影")
    # 2) 重放投影与在线投影一致，且 accepted 账本行集合一致
    assert dump_database(path_c) == dump_a

    # ---- 3) 删除 A 的全部投影后重建，必须得到删除前的相同状态 ----
    direct = direct_connect(path_a)
    direct.execute("DELETE FROM subscriber_entitlements")
    direct.commit()
    direct.close()
    rebuilt = client_a.post("/api/network/ledger/rebuild")
    assert rebuilt.status_code == 200, rebuilt.text
    assert rebuilt.json()["rebuilt_orders"] == len(dump_a["projections"])
    assert dump_database(path_a) == dump_a
    assert client_a.get("/api/network/ledger/reconcile").json()["consistent"] is True

    # ---- 4) 人为破坏投影：对账发现差异，重建修复回相同状态 ----
    direct = direct_connect(path_a)
    direct.execute("UPDATE subscriber_entitlements SET valid_until=? WHERE source_order_id='order-B'", (at(1),))
    direct.commit()
    direct.close()
    broken = client_a.get("/api/network/ledger/orders/order-B/reconcile").json()
    assert broken["consistent"] is False
    assert any(diff["check"] == "valid_until" for diff in broken["diffs"])
    client_a.post("/api/network/ledger/rebuild")
    assert dump_database(path_a) == dump_a
    assert client_a.get("/api/network/ledger/orders/order-B/reconcile").json()["consistent"] is True

    # ---- 5) 业务终态：order-A 已退款且退款后续期未生效；上车时刻由 order-B 提供权益 ----
    order_a = client_a.get("/api/network/ledger/orders/order-A").json()
    assert order_a["projection"]["status"] == "refunded"
    assert order_a["projection"]["valid_until"] == at(20)  # 到期后续期生效过，退款后不再顺延
    assert "evt-A7" not in accepted_event_nos(path_a)  # 退款后续期从未归并
    direct = direct_connect(path_a)
    invalid_reasons = {row[0] for row in direct.execute(
        "SELECT DISTINCT reject_reason FROM entitlement_event_log "
        "WHERE source_event_no='evt-A7' AND disposition='invalid'"
    ).fetchall()}
    direct.close()
    # 乱序期间 evt-A7 可能因购买未到/版本间隙被拒，收敛后必定因退款终态被拒
    assert "order_refunded" in invalid_reasons
    current = client_a.get("/api/network/ledger/entitlements/current",
                           params={"subscriber_hash": SUBSCRIBER, "scenario_code": SCENARIO, "at": at(16)}).json()
    assert current["eligible"] is True
    assert current["eligible_order_ids"] == ["order-B"]
    client_a.__exit__(None, None, None)
