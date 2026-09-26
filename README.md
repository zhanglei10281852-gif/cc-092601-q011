# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 维护窗口：可按算法、参数模板或项目定义窗口，经历预告、排空、强制停止、恢复和取消状态；排空阶段只拦截匹配任务的新领取，截止后按策略取消或重新排队持有租约的任务，重叠窗口采用更严格规则。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

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

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

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

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

### 维护窗口

求解器升级前可创建维护窗口，按算法（`algorithm`）、参数模板编码（`template`）或项目（`project`）圈定受影响任务，并指定排空开始、截止、恢复三个时间点以及截止策略（`cancel` 取消或 `requeue` 重新排队）：

- `POST /api/compute/maintenance/windows?actor=...` 创建窗口，初始为预告状态，不影响领取。
- `POST /api/compute/maintenance/windows/{id}/advance` 按时间推进窗口（可由调度器重复调用，幂等）。
- `POST /api/compute/maintenance/windows/{id}/cancel` 在预告或排空阶段取消窗口。
- `POST /api/compute/maintenance/windows/{id}/recover` 升级提前结束时人工恢复。
- `GET /api/compute/maintenance/windows[?state=&scope_type=&scope_value=]` 与 `GET /api/compute/maintenance/windows/{id}` 返回窗口状态、阻塞原因、受影响任务和执行进度。
- `POST /api/compute/maintenance/claim-check` 查询某组工作者能力下队首任务被拦截的原因；`POST /api/compute/tasks/claim` 在无任务可领时同样返回 `blocked` 信息。

排空（`draining`）阶段只阻止匹配任务被新领取，其他算法、模板和项目的队列照常分配；短任务可自然完成，进度中的 `running_leases` 归零、`safe_to_upgrade` 为真即表示可以安全升级。截止后进入强制停止（`enforcing`）：取消策略会释放租约并将匹配任务置为已取消，重新排队策略会把仍持有租约的运行任务放回原优先级队列，排队任务保持不动。重叠窗口采用更严格规则——只要同时到点的重叠窗口要求取消，就执行取消，干预记录归属更严格的窗口；窗口恢复（`recovered`）后任务仍按原有优先级与配额竞争。每次截止干预使用确定性批次键，重复推进同一窗口不会产生二次干预记录。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作、租约恢复，以及维护窗口的预告/排空/强制停止/恢复/取消流转、阻塞原因、截止取消与重排、重叠窗口取严、重复推进幂等和恢复后原优先级竞争，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
