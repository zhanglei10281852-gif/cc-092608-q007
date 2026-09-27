from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.core.errors import DomainError
from app.network.ledger_schemas import EntitlementEventBatch, EntitlementEventIn
from app.network.ledger_service import EntitlementLedgerService

router = APIRouter(prefix="/api/network/ledger", tags=["用户权益账本"])


def service() -> EntitlementLedgerService:
    return EntitlementLedgerService()


def _status_for(result: dict) -> int:
    if result["event"]["disposition"] == "accepted":
        return 200 if result["duplicate"] else 201
    return 409


def _respond(result: dict) -> dict | JSONResponse:
    status = _status_for(result)
    if status == 201:
        return result
    return JSONResponse(status_code=status, content=result)


@router.post("/events", status_code=201)
def ingest_event(payload: EntitlementEventIn):
    """接收单个权益事件：201 归并；200 去重命中；409 过时/乱序/规则拒绝（已记账）。"""
    return _respond(service().ingest_event(payload.model_dump()))


@router.post("/events/batch")
def ingest_batch(payload: EntitlementEventBatch):
    """批量接收事件，每个事件独立归并，乱序批次可整体重放直至全部归并。"""
    svc = service()
    items = []
    counts = {"accepted": 0, "duplicates": 0, "rejected": 0, "failed": 0}
    for item in payload.items:
        try:
            result = svc.ingest_event(item.model_dump())
        except DomainError as exc:
            items.append({
                "status": exc.status_code,
                "source_event_no": item.source_event_no,
                "order_id": item.order_id,
                "error": {"code": exc.code, "message": exc.message},
            })
            counts["failed"] += 1
            continue
        status = _status_for(result)
        items.append({"status": status, **result})
        if result["duplicate"]:
            counts["duplicates"] += 1
        elif status == 201:
            counts["accepted"] += 1
        else:
            counts["rejected"] += 1
    return {"items": items, **counts}


@router.get("/orders/{order_id}")
def order_detail(order_id: str):
    return service().order_detail(order_id)


@router.get("/orders/{order_id}/timeline")
def order_timeline(order_id: str):
    """订单完整时间线：账本中的全部事件（含被拒绝的）与当前投影。"""
    return service().order_timeline(order_id)


@router.get("/orders/{order_id}/reconcile")
def reconcile_order(order_id: str):
    """把投影与账本中 accepted 事件的规范归并结果逐字段对账。"""
    return service().reconcile_order(order_id)


@router.get("/reconcile")
def reconcile_all():
    return service().reconcile_all()


@router.post("/rebuild")
def rebuild_projection():
    """删除并按账本重建全部投影，返回修复明细；无账本事件的旧投影保留。"""
    return service().rebuild_projection()


@router.get("/entitlements/current")
def current_entitlement(
    subscriber_hash: str = Query(min_length=16, max_length=128),
    scenario_code: str = Query(min_length=2, max_length=64),
    at: str | None = None,
):
    """某用户在某场景、某时刻的权益视图：资格、覆盖并集与有效期交叉。"""
    return service().current_entitlement(subscriber_hash, scenario_code, at)
