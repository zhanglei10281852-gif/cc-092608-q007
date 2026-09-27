from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class EntitlementEventIn(BaseModel):
    """运营商 App 上报的权益事件。

    occurred_at 是事件的业务生效时间；purchase 需要 valid_from/valid_until，
    extend 需要 new_valid_until，其余类型只需要 occurred_at。
    """

    source_system: str = Field(default="carrier-app", min_length=2, max_length=60, pattern=r"^[a-z0-9][a-z0-9._-]+$")
    source_event_no: str = Field(min_length=4, max_length=160)
    order_id: str = Field(min_length=4, max_length=160)
    event_type: Literal["purchase", "extend", "suspend", "resume", "refund", "expire"]
    business_version: int = Field(ge=1, le=1_000_000)
    subscriber_hash: str = Field(min_length=16, max_length=128)
    scenario_code: str = Field(min_length=2, max_length=64)
    product_code: str = Field(min_length=2, max_length=80)
    occurred_at: str
    valid_from: str | None = None
    valid_until: str | None = None
    new_valid_until: str | None = None
    reason: str = Field(default="", max_length=500)


class EntitlementEventBatch(BaseModel):
    items: list[EntitlementEventIn] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def unique_event_numbers(self) -> "EntitlementEventBatch":
        keys = [(item.source_system, item.source_event_no) for item in self.items]
        if len(keys) != len(set(keys)):
            raise ValueError("同一批次内来源事件号不能重复")
        return self
