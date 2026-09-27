from __future__ import annotations

from fastapi import APIRouter, Response

from app.network.entitlement_schemas import EntitlementEventBatch, EntitlementEventCreate
from app.network.entitlements import EntitlementLedgerService

router = APIRouter(prefix="/api/network", tags=["用户权益账本"])


def service() -> EntitlementLedgerService:
    return EntitlementLedgerService()


@router.post("/entitlement-events", status_code=201)
def ingest_event(payload: EntitlementEventCreate, response: Response):
    result = service().ingest_event(payload.model_dump())
    if result["decision"] == "duplicate":
        response.status_code = 200
    return result


@router.post("/entitlement-events/batch", status_code=202)
def ingest_batch(payload: EntitlementEventBatch):
    return service().ingest_batch([item.model_dump() for item in payload.items])


@router.get("/entitlement-orders/{order_id}")
def current_projection(order_id: str):
    return service().current_projection(order_id)


@router.get("/entitlement-orders/{order_id}/timeline")
def order_timeline(order_id: str):
    return service().timeline(order_id)


@router.get("/entitlement-orders/{order_id}/reconciliation")
def order_reconciliation(order_id: str):
    return service().reconcile_order(order_id)


@router.post("/entitlement-orders/{order_id}/rebuild")
def rebuild_order(order_id: str):
    return service().rebuild_order(order_id)


@router.post("/entitlement-projections/rebuild")
def rebuild_all_orders():
    return service().rebuild_all()


@router.get("/entitlement-reconciliation")
def reconciliation_summary():
    return service().reconcile_all()
