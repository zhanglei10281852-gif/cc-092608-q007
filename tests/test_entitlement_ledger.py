from __future__ import annotations

import os
import random
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.core.clock import FrozenClock
from app.database import close_connection, get_connection
from app.network.entitlements import EntitlementLedgerService, FoldEvent, InvalidTransition, fold_events, transition
from app.network.rules import DEFAULT_RULES
from app.network.service import NetworkAccelerationService

EVENTS_URL = "/api/network/entitlement-events"
ORDERS_URL = "/api/network/entitlement-orders"

SUB_A = "subscriber-a-0000000001"
SUB_B = "subscriber-b-0000000002"
SUB_C = "subscriber-c-0000000003"
SUB_D = "subscriber-d-0000000004"

ORDER_A = "order-rail-a001"
ORDER_B = "order-rail-b002"
ORDER_C = "order-rail-c003"
ORDER_D = "order-rail-d004"


def event(order_id, seq, event_type, version, occurred_at, subscriber, **extra):
    payload = {
        "order_id": order_id,
        "event_seq": seq,
        "event_type": event_type,
        "business_version": version,
        "subscriber_hash": subscriber,
        "scenario_code": None,
        "product_code": None,
        "valid_from": None,
        "valid_until": None,
        "occurred_at": occurred_at,
        "actor": "carrier-app",
    }
    payload.update(extra)
    return payload


def purchase(order_id, seq, version, subscriber, valid_from, valid_until, occurred_at):
    return event(
        order_id,
        seq,
        "purchase",
        version,
        occurred_at,
        subscriber,
        scenario_code="gdh-rail",
        product_code="rail-boost-day",
        valid_from=valid_from,
        valid_until=valid_until,
    )


def order_events():
    """四个订单的完整业务事件（按业务版本排序），覆盖全部事件类型与规则。"""
    return {
        ORDER_A: [
            purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z"),
            event(ORDER_A, 2, "extend", 2, "2026-09-20T06:00:00Z", SUB_A, valid_until="2026-09-22T00:00:00Z"),
            event(ORDER_A, 3, "pause", 3, "2026-09-20T12:00:00Z", SUB_A),
            event(ORDER_A, 4, "resume", 4, "2026-09-20T18:00:00Z", SUB_A),
            event(ORDER_A, 5, "expire", 5, "2026-09-22T06:00:00Z", SUB_A),
        ],
        ORDER_B: [
            purchase(ORDER_B, 1, 1, SUB_B, "2026-09-20T00:00:00Z", "2026-09-23T00:00:00Z", "2026-09-20T00:00:00Z"),
            event(ORDER_B, 2, "pause", 2, "2026-09-21T00:00:00Z", SUB_B),
            event(ORDER_B, 3, "refund", 3, "2026-09-21T12:00:00Z", SUB_B),
            event(ORDER_B, 4, "extend", 4, "2026-09-22T00:00:00Z", SUB_B, valid_until="2026-09-25T00:00:00Z"),
            event(ORDER_B, 5, "resume", 5, "2026-09-22T01:00:00Z", SUB_B),
        ],
        ORDER_C: [
            purchase(ORDER_C, 1, 1, SUB_C, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z"),
            event(ORDER_C, 2, "extend", 2, "2026-09-20T06:00:00Z", SUB_C, valid_until="2026-09-24T00:00:00Z"),
            event(ORDER_C, 3, "pause", 3, "2026-09-20T08:00:00Z", SUB_C),
        ],
        ORDER_D: [
            event(ORDER_D, 1, "pause", 1, "2026-09-20T00:00:00Z", SUB_D),
            purchase(ORDER_D, 2, 2, SUB_D, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z"),
            purchase(ORDER_D, 3, 2, SUB_D, "2026-09-20T00:00:00Z", "2026-09-22T00:00:00Z", "2026-09-20T01:00:00Z"),
            event(ORDER_D, 4, "resume", 3, "2026-09-20T10:00:00Z", SUB_D),
        ],
    }


def scripted_stream():
    """一组打乱顺序的到达流：订单交错、C 的延期乱序、D 的暂停先于购买、末尾重复投递。"""
    events = order_events()
    stream = [
        (events[ORDER_C][0], "applied"),
        (events[ORDER_A][0], "applied"),
        (events[ORDER_D][0], "rejected"),
        (events[ORDER_B][0], "applied"),
        (events[ORDER_A][1], "applied"),
        (events[ORDER_C][2], "applied"),
        (events[ORDER_B][1], "applied"),
        (events[ORDER_D][1], "applied"),
        (events[ORDER_A][2], "applied"),
        (events[ORDER_C][1], "stale"),
        (events[ORDER_B][2], "applied"),
        (events[ORDER_D][2], "stale"),
        (events[ORDER_A][3], "applied"),
        (events[ORDER_B][3], "rejected"),
        (events[ORDER_D][3], "rejected"),
        (events[ORDER_A][4], "applied"),
        (events[ORDER_B][4], "rejected"),
        (events[ORDER_D][1], "duplicate"),
    ]
    return stream


def expected_projections():
    return {
        ORDER_A: {
            "status": "expired",
            "version": 5,
            "valid_from": "2026-09-20T00:00:00+00:00",
            "valid_until": "2026-09-22T06:00:00+00:00",
            "paused_at": None,
            "pause_total_seconds": 6 * 3600,
            "applied_events": 5,
            "last_event_seq": 5,
        },
        ORDER_B: {
            "status": "cancelled",
            "version": 3,
            "valid_from": "2026-09-20T00:00:00+00:00",
            "valid_until": "2026-09-23T00:00:00+00:00",
            "paused_at": None,
            "pause_total_seconds": 0,
            "applied_events": 3,
            "last_event_seq": 3,
        },
        ORDER_C: {
            "status": "suspended",
            "version": 3,
            "valid_from": "2026-09-20T00:00:00+00:00",
            "valid_until": "2026-09-21T00:00:00+00:00",
            "paused_at": "2026-09-20T08:00:00+00:00",
            "pause_total_seconds": 0,
            "applied_events": 2,
            "last_event_seq": 3,
        },
        ORDER_D: {
            "status": "active",
            "version": 2,
            "valid_from": "2026-09-20T00:00:00+00:00",
            "valid_until": "2026-09-21T00:00:00+00:00",
            "paused_at": None,
            "pause_total_seconds": 0,
            "applied_events": 1,
            "last_event_seq": 2,
        },
    }


def prepare_scenario(client):
    response = client.post(
        "/api/network/scenarios",
        json={"code": "gdh-rail", "name": "广深高铁", "scene_type": "railway", "timezone": "Asia/Shanghai", "max_concurrent_sessions": 10, "capacity_mbps": 3000},
    )
    assert response.status_code == 201, response.text


def play_stream(client, stream):
    decisions = []
    for payload, _ in stream:
        response = client.post(EVENTS_URL, json=payload)
        assert response.status_code in (200, 201), response.text
        decisions.append(response.json())
    return decisions


def projection_without_eligibility(payload):
    return {key: value for key, value in payload.items() if key != "eligibility"}


def assert_expected_projection(client, order_id, expected):
    current = client.get(f"{ORDERS_URL}/{order_id}")
    assert current.status_code == 200, current.text
    projection = current.json()
    for field, value in expected.items():
        assert projection[field] == value, f"{order_id}.{field}: {projection[field]!r} != {value!r}"
    return projection


# ---------------------------------------------------------------------- 状态机规则


def fold_event(order_id, seq, event_type, version, occurred_at, **extra):
    return FoldEvent(
        order_id=order_id,
        event_seq=seq,
        event_type=event_type,
        business_version=version,
        subscriber_hash=extra.get("subscriber_hash", "subscriber-unit-0001"),
        scenario_id=extra.get("scenario_id", 1),
        product_code=extra.get("product_code", "rail-boost-day"),
        valid_from=extra.get("valid_from"),
        valid_until=extra.get("valid_until"),
        occurred_at=occurred_at,
    )


def at(value):
    return datetime.fromisoformat(value).astimezone(UTC)


def purchased_state():
    return transition(
        None,
        fold_event("order-unit", 1, "purchase", 1, at("2026-09-20T00:00:00+00:00"), valid_from=at("2026-09-20T00:00:00+00:00"), valid_until=at("2026-09-21T00:00:00+00:00")),
    )


def test_transition_purchase_only_once():
    state = purchased_state()
    assert state.status == "active"
    with pytest.raises(InvalidTransition, match="duplicate_purchase"):
        transition(state, fold_event("order-unit", 2, "purchase", 2, at("2026-09-20T01:00:00+00:00"), valid_from=at("2026-09-20T00:00:00+00:00"), valid_until=at("2026-09-25T00:00:00+00:00")))


def test_transition_extend_only_lengthens_and_merges_crossing_periods():
    state = purchased_state()
    with pytest.raises(InvalidTransition, match="extension_not_effective"):
        transition(state, fold_event("order-unit", 2, "extend", 2, at("2026-09-20T06:00:00+00:00"), valid_until=at("2026-09-20T12:00:00+00:00")))
    extended = transition(state, fold_event("order-unit", 2, "extend", 2, at("2026-09-20T06:00:00+00:00"), valid_until=at("2026-09-25T00:00:00+00:00")))
    assert extended.valid_until == at("2026-09-25T00:00:00+00:00")
    assert extended.valid_from == state.valid_from


def test_transition_extend_after_expiry_renews_but_after_refund_is_closed():
    state = purchased_state()
    expired = transition(state, fold_event("order-unit", 2, "expire", 2, at("2026-09-21T00:00:00+00:00")))
    renewed = transition(expired, fold_event("order-unit", 3, "extend", 3, at("2026-09-22T00:00:00+00:00"), valid_until=at("2026-09-25T00:00:00+00:00")))
    assert renewed.status == "active"
    assert renewed.valid_from == at("2026-09-22T00:00:00+00:00")
    assert renewed.valid_until == at("2026-09-25T00:00:00+00:00")
    refunded = transition(state, fold_event("order-unit", 2, "refund", 2, at("2026-09-20T08:00:00+00:00")))
    assert refunded.status == "cancelled"
    with pytest.raises(InvalidTransition, match="order_closed"):
        transition(refunded, fold_event("order-unit", 3, "extend", 3, at("2026-09-20T09:00:00+00:00"), valid_until=at("2026-09-25T00:00:00+00:00")))


def test_transition_pause_resume_compensates_paused_time():
    state = purchased_state()
    with pytest.raises(InvalidTransition, match="invalid_transition"):
        transition(state, fold_event("order-unit", 2, "resume", 2, at("2026-09-20T01:00:00+00:00")))
    paused = transition(state, fold_event("order-unit", 2, "pause", 2, at("2026-09-20T06:00:00+00:00")))
    assert paused.status == "suspended"
    resumed = transition(paused, fold_event("order-unit", 3, "resume", 3, at("2026-09-20T08:30:00+00:00")))
    assert resumed.status == "active"
    assert resumed.valid_until == at("2026-09-21T02:30:00+00:00")
    assert resumed.pause_total_seconds == 9000
    with pytest.raises(InvalidTransition, match="invalid_transition"):
        transition(paused, fold_event("order-unit", 3, "pause", 3, at("2026-09-20T07:00:00+00:00")))


def test_transition_expire_and_refund_terminal_rules():
    state = purchased_state()
    paused = transition(state, fold_event("order-unit", 2, "pause", 2, at("2026-09-20T06:00:00+00:00")))
    expired = transition(paused, fold_event("order-unit", 3, "expire", 3, at("2026-09-21T00:00:00+00:00")))
    assert expired.status == "expired"
    with pytest.raises(InvalidTransition, match="invalid_transition"):
        transition(expired, fold_event("order-unit", 4, "expire", 4, at("2026-09-22T00:00:00+00:00")))
    refunded = transition(expired, fold_event("order-unit", 4, "refund", 4, at("2026-09-23T00:00:00+00:00")))
    assert refunded.status == "cancelled"
    with pytest.raises(InvalidTransition, match="order_closed"):
        transition(refunded, fold_event("order-unit", 5, "pause", 5, at("2026-09-23T01:00:00+00:00")))


def test_transition_requires_purchase_and_matching_identity():
    with pytest.raises(InvalidTransition, match="not_purchased"):
        transition(None, fold_event("order-unit", 1, "pause", 1, at("2026-09-20T00:00:00+00:00")))
    state = purchased_state()
    with pytest.raises(InvalidTransition, match="identity_mismatch"):
        transition(state, fold_event("order-unit", 2, "pause", 2, at("2026-09-20T06:00:00+00:00"), subscriber_hash="subscriber-other-999"))
    with pytest.raises(InvalidTransition, match="identity_mismatch"):
        transition(state, fold_event("order-unit", 2, "pause", 2, at("2026-09-20T06:00:00+00:00"), scenario_id=99))


def test_fold_events_matches_sequential_application():
    events = [
        fold_event("order-unit", 1, "purchase", 1, at("2026-09-20T00:00:00+00:00"), valid_from=at("2026-09-20T00:00:00+00:00"), valid_until=at("2026-09-21T00:00:00+00:00")),
        fold_event("order-unit", 2, "pause", 2, at("2026-09-20T06:00:00+00:00")),
        fold_event("order-unit", 3, "resume", 3, at("2026-09-20T09:00:00+00:00")),
    ]
    assert fold_events(events) == fold_events(list(events))
    assert fold_events(events).valid_until == at("2026-09-21T03:00:00+00:00")


# ---------------------------------------------------------------------- API 接收规则


def test_event_validation_and_unknown_order(client):
    prepare_scenario(client)
    missing = client.post(EVENTS_URL, json=event(ORDER_A, 1, "purchase", 1, "2026-09-20T00:00:00Z", SUB_A, scenario_code="gdh-rail", product_code="rail-boost-day"))
    assert missing.status_code == 422
    bad_time = client.post(EVENTS_URL, json=purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "not-a-time"))
    assert bad_time.status_code == 422
    unknown_scenario = client.post(EVENTS_URL, json=purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z") | {"scenario_code": "missing-line"})
    assert unknown_scenario.status_code == 404
    assert client.get(f"{ORDERS_URL}/order-missing").status_code == 404
    assert client.get(f"{ORDERS_URL}/order-missing/timeline").status_code == 404
    assert client.get(f"{ORDERS_URL}/order-missing/reconciliation").status_code == 404


def test_duplicate_event_seq_is_idempotent_and_conflict_on_different_payload(client):
    prepare_scenario(client)
    payload = purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z")
    first = client.post(EVENTS_URL, json=payload)
    assert first.status_code == 201
    assert first.json()["decision"] == "applied"
    again = client.post(EVENTS_URL, json=payload)
    assert again.status_code == 200
    assert again.json()["decision"] == "duplicate"
    assert again.json()["recorded_decision"] == "applied"
    assert again.json()["ledger_id"] == first.json()["ledger_id"]
    conflict = client.post(EVENTS_URL, json=payload | {"valid_until": "2026-09-22T00:00:00Z"})
    assert conflict.status_code == 409
    timeline = client.get(f"{ORDERS_URL}/{ORDER_A}/timeline").json()
    assert len(timeline["events"]) == 1


def test_stale_version_recorded_but_not_applied(client):
    prepare_scenario(client)
    client.post(EVENTS_URL, json=purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z"))
    client.post(EVENTS_URL, json=event(ORDER_A, 2, "pause", 3, "2026-09-20T05:00:00Z", SUB_A))
    stale = client.post(EVENTS_URL, json=event(ORDER_A, 3, "extend", 2, "2026-09-20T04:00:00Z", SUB_A, valid_until="2026-09-25T00:00:00Z"))
    assert stale.status_code == 201
    assert stale.json()["decision"] == "stale"
    current = client.get(f"{ORDERS_URL}/{ORDER_A}").json()
    assert current["status"] == "suspended"
    assert current["version"] == 3
    assert current["valid_until"] == "2026-09-21T00:00:00+00:00"
    timeline = client.get(f"{ORDERS_URL}/{ORDER_A}/timeline").json()
    assert [(item["event_type"], item["decision"]) for item in timeline["events"]] == [("purchase", "applied"), ("extend", "stale"), ("pause", "applied")]
    reconciliation = client.get(f"{ORDERS_URL}/{ORDER_A}/reconciliation").json()
    assert reconciliation["consistent"] is True
    assert reconciliation["missing_versions"] == [{"business_version": 2, "decision": "stale", "event_seq": 3, "event_type": "extend"}]


def test_refund_is_terminal_and_renewal_after_refund_rejected(client):
    prepare_scenario(client)
    client.post(EVENTS_URL, json=purchase(ORDER_B, 1, 1, SUB_B, "2026-09-20T00:00:00Z", "2026-09-23T00:00:00Z", "2026-09-20T00:00:00Z"))
    refund = client.post(EVENTS_URL, json=event(ORDER_B, 2, "refund", 2, "2026-09-21T00:00:00Z", SUB_B))
    assert refund.json()["decision"] == "applied"
    renewal = client.post(EVENTS_URL, json=event(ORDER_B, 3, "extend", 3, "2026-09-22T00:00:00Z", SUB_B, valid_until="2026-09-25T00:00:00Z"))
    assert renewal.status_code == 201
    assert renewal.json()["decision"] == "rejected"
    assert renewal.json()["decision_reason"] == "order_closed"
    current = client.get(f"{ORDERS_URL}/{ORDER_B}").json()
    assert current["status"] == "cancelled"
    assert current["version"] == 2
    assert current["eligibility"]["eligible"] is False
    assert current["eligibility"]["reason"] == "cancelled"


def test_batch_ingest_processes_items_independently(client):
    prepare_scenario(client)
    items = [
        purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z"),
        event(ORDER_A, 3, "pause", 3, "2026-09-20T05:00:00Z", SUB_A),
        event(ORDER_A, 2, "extend", 2, "2026-09-20T04:00:00Z", SUB_A, valid_until="2026-09-25T00:00:00Z"),
        purchase(ORDER_B, 1, 1, SUB_B, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z") | {"scenario_code": "missing-line"},
    ]
    response = client.post(f"{EVENTS_URL}/batch", json={"items": items})
    assert response.status_code == 202
    decisions = [item["decision"] for item in response.json()["items"]]
    assert decisions == ["applied", "applied", "stale", "error"]
    assert response.json()["accepted"] == 2
    duplicate_batch = client.post(f"{EVENTS_URL}/batch", json={"items": items[:1]})
    assert duplicate_batch.json()["items"][0]["decision"] == "duplicate"
    repeated_keys = client.post(f"{EVENTS_URL}/batch", json={"items": [items[0], items[0]]})
    assert repeated_keys.status_code == 422


# ---------------------------------------------------------------------- 打乱顺序的一致性证明


def test_shuffled_delivery_rebuild_and_replay_are_consistent(client):
    prepare_scenario(client)
    stream = scripted_stream()
    results = play_stream(client, stream)
    assert [item["decision"] for item in results] == [expected for _, expected in stream]

    expected = expected_projections()
    snapshots = {order_id: assert_expected_projection(client, order_id, fields) for order_id, fields in expected.items()}

    # 重建投影：结果必须与在线处理完全一致
    for order_id, snapshot in snapshots.items():
        rebuilt = client.post(f"{ORDERS_URL}/{order_id}/rebuild")
        assert rebuilt.status_code == 200, rebuilt.text
        report = rebuilt.json()
        assert report["consistent"] is True
        assert report["projection_diffs"] == []
        assert report["legacy_row_diffs"] == []
        assert projection_without_eligibility(report["projection"]) == projection_without_eligibility(snapshot)

    # 对账差异：投影与账本一致，缺失版本与决策分布符合预期
    reconciliation = {order_id: client.get(f"{ORDERS_URL}/{order_id}/reconciliation").json() for order_id in expected}
    assert all(report["consistent"] for report in reconciliation.values())
    assert reconciliation[ORDER_C]["missing_versions"] == [{"business_version": 2, "decision": "stale", "event_seq": 2, "event_type": "extend"}]
    assert reconciliation[ORDER_D]["missing_versions"] == [{"business_version": 1, "decision": "rejected", "event_seq": 1, "event_type": "pause"}]
    assert reconciliation[ORDER_A]["missing_versions"] == []
    assert reconciliation[ORDER_B]["decision_counts"] == {"applied": 3, "rejected": 2}
    assert reconciliation[ORDER_C]["decision_counts"] == {"applied": 2, "stale": 1}
    assert reconciliation[ORDER_D]["decision_counts"] == {"rejected": 2, "applied": 1, "stale": 1}

    # 权益最终行（客服看到的行）与投影保持一致
    legacy_states = {row["source_order_id"]: row["state"] for row in get_connection().execute("SELECT source_order_id,state FROM subscriber_entitlements").fetchall()}
    assert legacy_states == {ORDER_A: "expired", ORDER_B: "cancelled", ORDER_C: "suspended", ORDER_D: "active"}

    # 重放同一段到达流：全部幂等去重，投影不变
    replayed = play_stream(client, [(payload, None) for payload, _ in stream])
    assert all(item["decision"] == "duplicate" for item in replayed)
    for order_id, snapshot in snapshots.items():
        current = client.get(f"{ORDERS_URL}/{order_id}").json()
        assert projection_without_eligibility(current) == projection_without_eligibility(snapshot)

    # 全量重建与全量对账也不应发现任何差异
    rebuild_all = client.post("/api/network/entitlement-projections/rebuild")
    assert rebuild_all.status_code == 200
    assert rebuild_all.json() == {"orders": 4, "inconsistent_orders": []}
    summary = client.get("/api/network/entitlement-reconciliation")
    assert summary.status_code == 200
    assert summary.json()["inconsistent_orders"] == []
    assert len(summary.json()["items"]) == 4


def test_replaying_recorded_stream_into_fresh_database_reproduces_state(client):
    prepare_scenario(client)
    play_stream(client, scripted_stream())
    for order_id, fields in expected_projections().items():
        assert_expected_projection(client, order_id, fields)
        timeline = client.get(f"{ORDERS_URL}/{order_id}/timeline").json()
        applied_versions = [item["business_version"] for item in timeline["events"] if item["decision"] == "applied"]
        assert applied_versions == sorted(applied_versions)


def test_random_shuffles_online_rebuild_and_replay_stay_consistent(tmp_path: Path):
    payloads = [payload for events in order_events().values() for payload in events]
    orders = sorted(order_events())
    for seed in range(8):
        os.environ["NETWORK_DATABASE_PATH"] = str(tmp_path / f"shuffle-{seed}.db")
        close_connection()
        NetworkAccelerationService().create_scenario(
            {"code": "gdh-rail", "name": "广深高铁", "scene_type": "railway", "timezone": "Asia/Shanghai", "max_concurrent_sessions": 10, "capacity_mbps": 3000}
        )
        service = EntitlementLedgerService()
        shuffled = list(payloads)
        random.Random(20260927 + seed).shuffle(shuffled)
        for payload in shuffled:
            service.ingest_event(payload)
        for order_id in orders:
            report = service.reconcile_order(order_id)
            assert report["consistent"], f"seed={seed} order={order_id}: {report}"
        # 重放：同一到达流再次投递必须全部幂等，且不改变任何投影
        for payload in shuffled:
            assert service.ingest_event(payload)["decision"] == "duplicate"
        for order_id in orders:
            assert service.reconcile_order(order_id)["consistent"]
        assert service.rebuild_all()["inconsistent_orders"] == []
    close_connection()


# ---------------------------------------------------------------------- 资格解释与加速联动


def prepare_acceleration(client):
    prepare_scenario(client)
    client.post(
        "/api/network/scenarios/gdh-rail/segments",
        json={"code": "gz-sz-01", "name": "广州南至虎门", "sequence_no": 1, "expected_dwell_seconds": 900, "capacity_mbps": 1200},
    )
    client.post(
        "/api/network/applications",
        json={"app_code": "video-call", "name": "视频通话", "category": "video_call", "latency_target_ms": 100, "packet_loss_target": 0.01, "min_downlink_mbps": 8, "min_uplink_mbps": 4, "default_priority": 70},
    )
    policy = client.post("/api/network/scenarios/gdh-rail/policies", json={"rules": DEFAULT_RULES, "actor": "tests"})
    client.post(f"/api/network/policies/{policy.json()['id']}/publish", json={"actor": "tests", "effective_from": "2026-01-01T00:00:00Z"})


def degraded_sample(sample_key, subscriber):
    return {
        "sample_key": sample_key,
        "scenario_code": "gdh-rail",
        "segment_code": "gz-sz-01",
        "app_code": "video-call",
        "subscriber_hash": subscriber,
        "device_class": "phone",
        "train_speed_kmh": 300,
        "latency_ms": 350,
        "packet_loss": 0.08,
        "downlink_mbps": 1.5,
        "uplink_mbps": 0.5,
        "observed_at": "2026-09-26T05:30:00Z",
    }


def accelerate(client, sample_key, subscriber):
    sample = client.post("/api/network/samples", json=degraded_sample(sample_key, subscriber))
    assert sample.status_code == 202, sample.text
    return client.post(f"/api/network/incidents/{sample.json()['incident_id']}/accelerate", json={"actor": "tests"})


def test_refund_explains_why_passenger_lost_eligibility(client):
    prepare_acceleration(client)
    subscriber = "subscriber-p-0000000005"
    order_id = "order-rail-p005"
    client.post(EVENTS_URL, json=purchase(order_id, 1, 1, subscriber, "2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z", "2026-09-20T00:00:00Z"))
    assert accelerate(client, "sample-before-refund", subscriber).status_code == 200

    refund = client.post(EVENTS_URL, json=event(order_id, 2, "refund", 2, "2026-09-26T05:40:00Z", subscriber))
    assert refund.json()["decision"] == "applied"

    denied = accelerate(client, "sample-after-refund", subscriber)
    assert denied.status_code == 409
    current = client.get(f"{ORDERS_URL}/{order_id}").json()
    assert current["eligibility"] == {"eligible": False, "reason": "cancelled", "message": "订单已退款", "checked_at": current["eligibility"]["checked_at"]}
    timeline = client.get(f"{ORDERS_URL}/{order_id}/timeline").json()
    assert [(item["event_type"], item["decision"]) for item in timeline["events"]] == [("purchase", "applied"), ("refund", "applied")]


def test_pause_and_resume_drive_acceleration_eligibility(client):
    prepare_acceleration(client)
    subscriber = "subscriber-q-0000000006"
    order_id = "order-rail-q006"
    client.post(EVENTS_URL, json=purchase(order_id, 1, 1, subscriber, "2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z", "2026-09-20T00:00:00Z"))
    client.post(EVENTS_URL, json=event(order_id, 2, "pause", 2, "2026-09-26T05:00:00Z", subscriber))
    assert accelerate(client, "sample-while-paused", subscriber).status_code == 409
    current = client.get(f"{ORDERS_URL}/{order_id}").json()
    assert current["eligibility"]["reason"] == "suspended"

    client.post(EVENTS_URL, json=event(order_id, 3, "resume", 3, "2026-09-26T07:00:00Z", subscriber))
    resumed = client.get(f"{ORDERS_URL}/{order_id}").json()
    assert resumed["valid_until"] == "2099-01-01T02:00:00+00:00"
    assert resumed["pause_total_seconds"] == 7200
    assert accelerate(client, "sample-after-resume", subscriber).status_code == 200


def test_eligibility_uses_fixed_clock(client):
    prepare_scenario(client)
    service = EntitlementLedgerService(get_connection(), FrozenClock(datetime(2026, 9, 20, 12, 0, tzinfo=UTC)))
    service.ingest_event(purchase(ORDER_A, 1, 1, SUB_A, "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:00:00Z"))
    assert service.current_projection(ORDER_A)["eligibility"]["eligible"] is True
    later = EntitlementLedgerService(get_connection(), FrozenClock(datetime(2026, 9, 22, 0, 0, tzinfo=UTC)))
    projection = later.current_projection(ORDER_A)
    assert projection["eligibility"]["eligible"] is False
    assert projection["eligibility"]["reason"] == "validity_ended"
