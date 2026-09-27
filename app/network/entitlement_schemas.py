from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class EntitlementEventCreate(BaseModel):
    order_id: str = Field(min_length=4, max_length=160)
    event_seq: int = Field(ge=1, le=2_000_000_000)
    event_type: Literal["purchase", "extend", "pause", "resume", "refund", "expire"]
    business_version: int = Field(ge=1, le=2_000_000_000)
    subscriber_hash: str = Field(min_length=16, max_length=128)
    scenario_code: str | None = Field(default=None, min_length=2, max_length=64)
    product_code: str | None = Field(default=None, min_length=2, max_length=80)
    valid_from: str | None = None
    valid_until: str | None = None
    occurred_at: str = Field(min_length=10, max_length=40)
    actor: str = Field(default="carrier-app", min_length=1, max_length=120)

    @model_validator(mode="after")
    def validate_requirements(self) -> "EntitlementEventCreate":
        if self.event_type == "purchase":
            if not self.scenario_code or not self.product_code or not self.valid_from or not self.valid_until:
                raise ValueError("购买事件必须提供场景、产品与有效期")
        if self.event_type == "extend" and not self.valid_until:
            raise ValueError("延期事件必须提供新的截止时间")
        return self


class EntitlementEventBatch(BaseModel):
    items: list[EntitlementEventCreate] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def unique_event_refs(self) -> "EntitlementEventBatch":
        refs = [(item.order_id, item.event_seq) for item in self.items]
        if len(refs) != len(set(refs)):
            raise ValueError("同一批次内来源事件号不能重复")
        return self
