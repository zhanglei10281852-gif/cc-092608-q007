# 5G-A 场景加速运营服务

这是一个面向运营商网络优化与产品运营团队的 Python 后端，用于管理高铁、地铁、演唱会和大型场馆中的应用体验采样、质差识别、动态加速、用户权益、策略版本、容量预留与运营审计。服务使用 FastAPI 与 SQLite，所有运行数据保存在单个本地数据库文件中，不依赖另行部署的数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 场景与区段：维护高铁线路、地铁区段、演唱会和场馆的容量、顺序、停留时间及运行状态。
- 应用画像：按游戏、直播、视频通话、视频和办公类别保存时延、丢包、上下行速率与优先级目标。
- 策略版本：校验质差评分权重、严重度阈值、资源倍数和会话时长，支持草稿、发布、生效与退役状态。
- 体验样本：使用业务采样键进行幂等写入，保存脱敏用户标识、终端类型、速度和网络指标。
- 质差事件：将应用目标与实测指标进行确定性比较，记录原因、严重度和处理状态。
- 动态加速：校验用户权益与生效策略，按区段容量预留上下行资源，支持完成、取消和超时释放。
- 权益账本：购买、延期、暂停、恢复、退款与到期事件写入不可变账本，按来源事件号去重、按业务版本拒绝过时写入，投影可安全重放重建，并提供订单时间线、当前权益与对账差异查询。
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

## 权益事件账本

运营商 App 的权益事件通过 `POST /api/network/entitlement-events` 写入不可变账本（`/batch` 支持批量），事件类型为 `purchase`、`extend`、`pause`、`resume`、`refund`、`expire`。接收闸门依次生效，每一次到达都会留在账本里并标记决策结果：

1. 来源事件号去重：`(order_id, event_seq)` 唯一，相同内容重复投递返回 `duplicate`（幂等），相同事件号不同内容返回 409。
2. 业务版本闸门：`business_version` 不大于订单当前版本时记为 `stale`，只记录不生效。
3. 生命周期校验：不合法迁移记为 `rejected`（如未购买先暂停、退款后续期 `order_closed`）。

投影归并规则（在线处理与重建共用同一纯函数，结果必然一致）：

- `purchase` 只能作为订单首个事件；`extend` 有效期只延不缩，交叉区间归并为连续有效期；订单已到期时 `extend` 视为续期并重新激活；订单退款后为终态，续期必须换新订单。
- `pause` 仅生效中可暂停；`resume` 按暂停时长等额顺延有效期（暂停补偿）；暂停期间到期不发生补偿。
- `refund` 是终态，生效、暂停、已到期状态都可退款；`expire` 仅生效或暂停中可到期。

投影每应用一个事件都会同步既有权益最终行（`subscriber_entitlements`），加速资格检查立即反映账本结果。查询接口：

- `GET /api/network/entitlement-orders/{order_id}`：当前权益投影与实时资格判定。
- `GET /api/network/entitlement-orders/{order_id}/timeline`：订单全部事件到达记录与决策。
- `GET /api/network/entitlement-orders/{order_id}/reconciliation`：投影与账本重放的字段级差异、缺失业务版本、决策分布，以及权益最终行差异。
- `POST /api/network/entitlement-orders/{order_id}/rebuild`：从账本重放重建投影（全量重建用 `POST /api/network/entitlement-projections/rebuild`，全量对账用 `GET /api/network/entitlement-reconciliation`）。

## 测试

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest
```

测试覆盖身份初始化、角色权限、审计脱敏、场景与应用登记、策略发布、样本幂等、质差判定、权益校验、容量拒绝、会话完成、固定时钟过期恢复、分析游标，以及权益账本的去重、版本闸门、生命周期规则、打乱顺序投递下在线处理与重放重建的一致性。

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
  network/         场景、应用、策略、采样、质差、加速、容量、分析、发布维护与权益账本
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
