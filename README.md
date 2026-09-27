# 5G-A 场景加速运营服务

这是一个面向运营商网络优化与产品运营团队的 Python 后端，用于管理高铁、地铁、演唱会和大型场馆中的应用体验采样、质差识别、动态加速、用户权益、策略版本、容量预留与运营审计。服务使用 FastAPI 与 SQLite，所有运行数据保存在单个本地数据库文件中，不依赖另行部署的数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 场景与区段：维护高铁线路、地铁区段、演唱会和场馆的容量、顺序、停留时间及运行状态。
- 应用画像：按游戏、直播、视频通话、视频和办公类别保存时延、丢包、上下行速率与优先级目标。
- 策略版本：校验质差评分权重、严重度阈值、资源倍数和会话时长，支持草稿、发布、生效与退役状态。
- 体验样本：使用业务采样键进行幂等写入，保存脱敏用户标识、终端类型、速度和网络指标。
- 质差事件：将应用目标与实测指标进行确定性比较，记录原因、严重度和处理状态。
- 动态加速：校验用户权益与生效策略，按区段容量预留上下行资源，支持完成、取消和超时释放。
- 权益账本：购买、延期、暂停、恢复、退款与到期事件写入不可变账本，按来源事件号去重、按订单业务版本归并投影，支持时间线、当前权益、对账差异查询与投影重建。
- 运营分析：提供场景与应用质差率、容量利用率、会话成效和可恢复事件游标。
- 身份与审计：提供管理员初始化、用户、角色、会话、权限、操作审计和后台维护能力。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/network-acceleration.db`。可以复制 `.env.example` 并通过 `NETWORK_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

5G-A 运营接口统一使用 `/api/network` 前缀，场景、应用、策略、权益、样本、事件、会话和分析报告都在该路径下。

## 用户权益事件账本

运营商 App 的购买、退款、暂停和续期事件可能重复或乱序到达。权益事件统一写入 `/api/network/ledger` 下的不可变账本（`entitlement_event_log`，数据库触发器禁止更新与删除），投影表 `subscriber_entitlements` 只是账本中已接受事件按订单业务版本折叠的结果，可以随时删除重建。

事件归并规则：

- 每个事件携带 `source_system + source_event_no`（来源事件号）与 `business_version`（订单内业务版本，从 1 开始连续递增）。
- 同一来源事件号重复送达按去重处理（200）；内容摘要不一致返回 409。
- 业务版本小于等于已归并版本视为过时写入（stale，409）；跳号视为版本间隙（version_gap，409）。被拒送达会留下审计行，但补齐缺失版本后重放同一事件仍可归并，重放安全且幂等。
- 暂停（suspend）记录挂起时刻；恢复（resume）把有效期顺延整个挂起时长。
- 退款（refund）是终态：退款后的续期、暂停、恢复与到期事件一律拒绝（order_refunded），退款后如需继续提供权益必须创建新订单。
- 到期（expire）把有效期截断到事件发生时间并解除挂起；到期不是终态，到期后续期（extend）允许。
- 同一用户同一场景的多个订单有效期允许交叉：覆盖区间取并集，任一订单在评估时刻有效即视为有权益。

接口：

- `POST /api/network/ledger/events`、`POST /api/network/ledger/events/batch`：接收单个或批量事件（201 归并 / 200 去重 / 409 拒绝并留痕）。
- `GET /api/network/ledger/orders/{order_id}/timeline`：订单完整时间线（含被拒绝的送达与原因），用于解释旅客为何在某时刻无资格。
- `GET /api/network/ledger/orders/{order_id}`：订单当前投影与事件计数。
- `GET /api/network/ledger/entitlements/current?subscriber_hash=&scenario_code=&at=`：某用户在某场景、某时刻的权益视图（资格、覆盖并集、有效期交叉）。
- `GET /api/network/ledger/orders/{order_id}/reconcile`、`GET /api/network/ledger/reconcile`：把投影与账本规范归并结果逐字段对账。
- `POST /api/network/ledger/rebuild`：删除并按账本重建全部投影；无账本事件的旧投影保留并单独报告。

旧版 `POST /api/network/entitlements` 仍然可用，内部转写为账本购买事件（业务版本 1），重复登记幂等返回既有投影。

## 测试

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest
```

测试覆盖身份初始化、角色权限、审计脱敏、场景与应用登记、策略发布、样本幂等、质差判定、权益校验、容量拒绝、会话完成、固定时钟过期恢复、分析游标，以及权益账本的去重、版本拒绝、乱序重放收敛、投影重建一致性和对账修复。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 与 CLI 冒烟

```bash
python -m app.cli smoke
python -m app.cli network-demo
```

`smoke` 在进程内检查根路径、健康接口和运营摘要；`network-demo` 会创建高铁场景、应用画像与策略，登记有效权益，写入一条质差样本并启动加速会话。

## 目录结构

```text
app/
  network/         场景、应用、策略、采样、质差、加速、容量、分析和权益账本
  api/             用户、角色、认证、审计、系统与维护接口
  core/            时钟、安全、异常、隐私和分页能力
  repositories/    通用 SQLite 查询与身份持久化
  schemas/         身份与管理接口输入模型
  services/        认证、审计、用户、后台任务和维护服务
  cli.py           初始化、检查和业务冒烟入口
  database.py      SQLite 连接、基础表结构与权限初始化
tests/             核心、身份、网络运营、分析和权益账本回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。策略发布、样本与事件创建、容量预留、会话终止和过期恢复使用即时事务。体验样本保存脱敏用户标识，登录令牌只保存摘要，审计和会话事件不会记录明文密码或令牌。
