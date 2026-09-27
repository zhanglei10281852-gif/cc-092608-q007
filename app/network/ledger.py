"""用户权益账本的纯领域规则。

本模块不依赖数据库、时钟或网络，所有函数对相同输入返回相同结果，
因此在线归并、乱序重放与投影重建必然收敛到同一状态。

归并规则（与接口文档一致）：

1. 业务版本：每个订单的事件必须按 business_version 从 1 开始连续递增。
   version <= 已归并版本视为过时写入（stale）拒绝；version 跳号视为版本间隙
   （version_gap）拒绝，发送方重放同一事件、待缺失版本补齐后即可归并；
   同一来源事件号重复送达按去重处理，不产生新账本行。
2. 状态机：purchase 创建订单（active）；suspend 仅允许 active -> suspended；
   resume 仅允许 suspended -> active，并把有效期顺延整个挂起时长；
   extend 顺延 valid_until（不向前推进的续期被接受但不改变窗口）；
   refund 使订单进入 refunded 终态；expire 把 valid_until 截断到事件发生时间
   并解除挂起（已到期时为幂等无操作）。
3. 退款是终态：退款后的续期、延期、暂停、恢复与到期事件一律拒绝
   （order_refunded）。退款后如需继续提供权益，必须创建新订单。
4. 到期不是终态：到期后允许续期（extend），窗口按新结束时间顺延。
5. 有效期交叉：同一用户同一场景的多个订单窗口允许交叉，不报错；
   覆盖区间取并集，任一订单在评估时刻有效即视为有权益。
6. 派生状态：refunded 优先，其次按评估时刻判定 expired，
   再其次 suspended，最后 active；只有 active 具备加速资格。
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Iterable

EVENT_TYPES = ("purchase", "extend", "suspend", "resume", "refund", "expire")
TERMINAL_STATUS = "refunded"


class LedgerRuleError(Exception):
    """业务规则拒绝：事件不能归并进订单投影。"""

    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    """归并所需的事件字段（已解析为感知时区的时间）。"""

    event_type: str
    business_version: int
    occurred_at: datetime
    valid_from: datetime | None = None
    valid_until: datetime | None = None
    new_valid_until: datetime | None = None


@dataclass(frozen=True, slots=True)
class OrderState:
    """单个订单权益的投影状态（纯数据）。"""

    order_id: str
    subscriber_hash: str
    scenario_code: str
    product_code: str
    valid_from: datetime
    valid_until: datetime
    status: str  # active | suspended | refunded
    applied_version: int
    purchased_at: datetime
    suspended_at: datetime | None = None
    refunded_at: datetime | None = None

    def derived_status(self, at: datetime) -> str:
        if self.status == TERMINAL_STATUS:
            return "refunded"
        if at >= self.valid_until:
            return "expired"
        return self.status

    def eligible_at(self, at: datetime) -> bool:
        return self.derived_status(at) == "active"


def apply_event(
    state: OrderState | None,
    event: LedgerEvent,
    *,
    order_id: str = "",
    subscriber_hash: str = "",
    scenario_code: str = "",
    product_code: str = "",
) -> OrderState:
    """把单个事件归并进订单状态，规则不满足时抛出 LedgerRuleError。"""
    if state is None:
        if event.event_type != "purchase" or event.business_version != 1:
            raise LedgerRuleError("first_event_must_be_purchase_v1", "订单首个事件必须是版本为 1 的购买事件")
        if event.valid_from is None or event.valid_until is None:
            raise LedgerRuleError("missing_validity_window", "购买事件必须携带有效期")
        if event.valid_until <= event.valid_from:
            raise LedgerRuleError("invalid_validity_window", "权益结束时间必须晚于开始时间")
        return OrderState(
            order_id=order_id,
            subscriber_hash=subscriber_hash,
            scenario_code=scenario_code,
            product_code=product_code,
            valid_from=event.valid_from,
            valid_until=event.valid_until,
            status="active",
            applied_version=1,
            purchased_at=event.occurred_at,
        )
    if event.business_version <= state.applied_version:
        raise LedgerRuleError("stale_version", "事件业务版本已被归并，属于过时写入")
    if event.business_version > state.applied_version + 1:
        raise LedgerRuleError("version_gap", "事件业务版本存在间隙，请先补齐缺失版本后重放")
    if event.event_type == "purchase":
        raise LedgerRuleError("duplicate_purchase", "订单已存在购买事件")
    if state.status == TERMINAL_STATUS:
        raise LedgerRuleError("order_refunded", "订单已退款并处于终态，退款后续期必须创建新订单")
    next_version = event.business_version
    if event.event_type == "extend":
        if event.new_valid_until is None:
            raise LedgerRuleError("missing_new_valid_until", "延期事件必须携带新的有效期结束时间")
        # 续期只允许顺延窗口；不向前推进的续期被接受但不改变状态
        return replace(state, valid_until=max(state.valid_until, event.new_valid_until), applied_version=next_version)
    if event.event_type == "suspend":
        if state.status == "suspended":
            raise LedgerRuleError("already_suspended", "订单已处于暂停状态")
        if event.occurred_at >= state.valid_until:
            raise LedgerRuleError("suspend_after_expiry", "暂停生效时间不能晚于权益结束时间")
        return replace(state, status="suspended", suspended_at=event.occurred_at, applied_version=next_version)
    if event.event_type == "resume":
        if state.status != "suspended" or state.suspended_at is None:
            raise LedgerRuleError("not_suspended", "订单未处于暂停状态，不能恢复")
        if event.occurred_at < state.suspended_at:
            raise LedgerRuleError("resume_before_suspend", "恢复时间早于暂停时间")
        # 恢复时把有效期顺延整个挂起时长，补偿用户暂停期间
        shift = event.occurred_at - state.suspended_at
        return replace(state, status="active", suspended_at=None, valid_until=state.valid_until + shift, applied_version=next_version)
    if event.event_type == "refund":
        return replace(state, status=TERMINAL_STATUS, suspended_at=None, refunded_at=event.occurred_at, applied_version=next_version)
    if event.event_type == "expire":
        if event.occurred_at >= state.valid_until:
            return replace(state, applied_version=next_version)  # 已到期，幂等无操作
        # 到期截断窗口并解除挂起；到期后不再累计挂起补偿
        return replace(state, valid_until=event.occurred_at, status="active", suspended_at=None, applied_version=next_version)
    raise LedgerRuleError("unknown_event_type", f"未知事件类型 {event.event_type}")


def fold_events(
    events: Iterable[LedgerEvent],
    *,
    order_id: str,
    subscriber_hash: str,
    scenario_code: str,
    product_code: str,
) -> OrderState | None:
    """按业务版本顺序归并一组事件，得到订单的规范状态。"""
    state: OrderState | None = None
    for event in sorted(events, key=lambda item: item.business_version):
        state = apply_event(
            state,
            event,
            order_id=order_id,
            subscriber_hash=subscriber_hash,
            scenario_code=scenario_code,
            product_code=product_code,
        )
    return state


def merge_windows(windows: Iterable[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """把 [start, end) 窗口列表归并为不重叠的有序并集。"""
    merged: list[list[datetime]] = []
    for start, end in sorted(windows, key=lambda item: (item[0], item[1])):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [(start, end) for start, end in merged]


def window_crossings(states: Iterable[OrderState]) -> list[tuple[str, str]]:
    """找出有效期互相交叉的订单对（用于说明有效期交叉规则）。"""
    live = [state for state in states if state.status != TERMINAL_STATUS]
    crossings: list[tuple[str, str]] = []
    for index, left in enumerate(live):
        for right in live[index + 1 :]:
            if left.valid_from < right.valid_until and right.valid_from < left.valid_until:
                crossings.append((left.order_id, right.order_id))
    return crossings


@dataclass(frozen=True, slots=True)
class EntitlementView:
    """某用户在某场景下、某评估时刻的权益视图。"""

    eligible: bool
    eligible_order_ids: tuple[str, ...]
    coverage_windows: tuple[tuple[datetime, datetime], ...]
    active_windows: tuple[tuple[datetime, datetime], ...]
    crossings: tuple[tuple[str, str], ...]


def entitlement_view(states: Iterable[OrderState], at: datetime) -> EntitlementView:
    """按并集规则汇总多个订单：任一有效订单即提供权益。"""
    orders = list(states)
    live = [state for state in orders if state.status != TERMINAL_STATUS]
    coverage = merge_windows((state.valid_from, state.valid_until) for state in live)
    eligible_orders = [state for state in live if state.eligible_at(at)]
    active = merge_windows((state.valid_from, state.valid_until) for state in eligible_orders)
    return EntitlementView(
        eligible=bool(eligible_orders),
        eligible_order_ids=tuple(state.order_id for state in eligible_orders),
        coverage_windows=tuple(coverage),
        active_windows=tuple(active),
        crossings=tuple(window_crossings(orders)),
    )
