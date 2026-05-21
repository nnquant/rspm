# rspm design

## 1. 背景

`rspm` 是一个基于 Rust 的跨平台本机进程管理工具，用于替代日常使用中的
PM2。它面向研究环境、交易基础设施、本地服务栈和轻量级生产进程编排场景。

PM2 的核心价值在于把进程启动、停止、重启、日志、状态查看、开机恢复和定时
重启收敛到一套简单 CLI 中。但 PM2 对 Node.js 生态绑定较深，ecosystem 文件
使用 JavaScript，声明式审计和依赖编排能力不足。`rspm` 继承 PM2 的易用性，
但以 TOML、DAG、health probe 和 SDK 控制面作为核心设计。

## 2. 目标

1. 提供跨平台的本机进程管理能力。
2. 支持 PM2 的主要日常功能：start、stop、restart、status、logs、monit、
   restart policy、watch restart、memory restart、startup。
3. 使用 TOML 替代 PM2 ecosystem 文件，配置必须可静态校验、可审计、可复现。
4. 支持基于 DAG 的 task 编排，允许 task 依赖其他 task 的启动或健康状态。
5. 支持定时启动、定时关闭和周期性 cron-like 动作。
6. 提供 Docker 风格的 CLI 表格输出。
7. 提供 Rust SDK 和 Python SDK，通过同一套 daemon API 操作 tasks。
8. 将配置作为事实来源，避免隐式运行态覆盖声明配置。

## 3. 非目标

1. 不支持 cluster。
2. 不做远程多机部署。
3. 不做 Kubernetes、Nomad 或 systemd 的完整替代。
4. 不承诺通用零停机 reload。
5. 第一阶段不做 Web UI。
6. 第一阶段不做插件系统。

这些非目标可以显著降低复杂度，使第一版集中在本机进程生命周期、DAG 编排、
日志和 SDK 控制面上。

## 4. PM2 对标

### 4.1 PM2 优点

| 能力 | 价值 |
| --- | --- |
| 简洁 CLI | `pm2 start`、`pm2 ls`、`pm2 logs` 很容易记忆 |
| 自动重启 | 进程崩溃后自动恢复 |
| 日志接管 | 自动保存 stdout 和 stderr |
| ecosystem 文件 | 一次声明多个进程 |
| 状态表格 | 快速查看 name、pid、uptime、restart count |
| startup/resurrect | 支持机器重启后恢复 |
| restart strategies | 支持 cron、watch、memory、delay、backoff 等策略 |

### 4.2 PM2 缺点

| 问题 | rspm 优化方向 |
| --- | --- |
| Node.js 生态绑定较深 | 语言无关，Rust、Python、C++、shell 进程一视同仁 |
| ecosystem 是 JavaScript | 使用 TOML，避免可执行配置带来的审计问题 |
| 依赖编排弱 | 原生 DAG，显式 depends_on 和 start_when |
| PID online 不等于服务可用 | 引入 health probe 和 readiness 语义 |
| save/resurrect 混合声明和运行态 | 配置即事实来源，state 只保存运行时状态 |
| 跨平台启动服务差异大 | 抽象 service install，分别适配 systemd、launchd、Windows |
| reload 语义偏 Node.js | 只支持 task 声明的 signal 或 command reload |

## 5. 核心概念

### 5.1 Task

`task` 是 `rspm` 的最小管理单元。一个 task 对应一个由 `rspmd` 启动和监管的
本机子进程。

task 包含：

1. 启动命令、参数、工作目录和环境变量。
2. restart policy。
3. health probe。
4. DAG 依赖。
5. schedule 和 cron-like actions。
6. 日志路径和日志保留策略。
7. 当前运行状态和最近退出信息。

### 5.2 Project

一个 TOML 文件描述一个 project。project 是 task 的命名空间，也是 apply、
validate、graph、status 的默认边界。

### 5.3 Daemon

`rspmd` 是本机常驻 daemon，负责真正的进程生命周期管理。CLI、Rust SDK 和
Python SDK 都通过 daemon API 操作 task，不直接杀进程或读取 PID 文件。

## 6. 总体架构

```text
rspm.toml
   |
   v
config parser + validator
   |
   v
DAG planner
   |
   v
rspmd daemon
   |
   +--> supervisor
   +--> scheduler
   +--> health checker
   +--> log manager
   +--> event store
   |
   v
CLI / Rust SDK / Python SDK
```

建议 Rust workspace：

```text
rspm/
├── Cargo.toml
├── crates/
│   ├── rspm-core/
│   ├── rspm-daemon/
│   ├── rspm/
│   └── rspm-sdk/
├── python/
│   └── rspm/
├── docs/
└── examples/
```

| crate | 职责 |
| --- | --- |
| `rspm-core` | 配置模型、状态机、DAG、调度语义、错误类型 |
| `rspm-daemon` | daemon、supervisor、scheduler、health checker、log manager |
| `rspm` | 命令行入口、表格输出、用户交互、daemon 自举 |
| `rspm-sdk` | Rust SDK 和本地 RPC client |
| `python/rspm` | Python SDK，封装同一套 RPC API |

## 7. 本地控制面

CLI 和 SDK 使用同一套本地 RPC API。

| 平台 | 默认通道 |
| --- | --- |
| Linux | Unix domain socket |
| macOS | Unix domain socket |
| Windows | named pipe |
| fallback | `127.0.0.1` TCP，仅本机绑定 |

第一版建议使用 JSON-RPC 2.0。理由：

1. Rust 和 Python 都容易实现。
2. 方便人工调试。
3. 进程管理 API 请求频率低，不需要过早优化协议性能。
4. 后续可以平滑增加 HTTP bridge 或 gRPC bridge。

## 8. TOML 配置

示例：

```toml
[project]
name = "trading-stack"
timezone = "Asia/Shanghai"
display_timezone = "local"

[defaults]
restart = "on-failure"
restart_delay = "3s"
max_restarts = 10
kill_timeout = "10s"

[tasks.master]
cmd = "uv"
args = ["run", "ldc-master"]
cwd = "/home/jiangda/services/local-market-data-center"
autostart = true
restart = "always"

[tasks.master.env]
RUST_LOG = "info"

[tasks.master.health]
type = "tcp"
address = "127.0.0.1:17690"
interval = "1s"
timeout = "500ms"
success_after = 2
failure_after = 3

[tasks.ctp_md]
cmd = "uv"
args = ["run", "ldc-ctp-md"]
depends_on = ["master"]
start_when = "dependencies_healthy"
restart = "on-failure"

[tasks.ctp_md.schedule]
start = "0 8 * * 1-5"
stop = "0 16 * * 1-5"

[tasks.strategy]
cmd = "uv"
args = ["run", "python", "scripts/run_strategy.py"]
depends_on = ["ctp_md"]
restart = "on-failure"

[tasks.strategy.cron.daily_restart]
expr = "30 8 * * 1-5"
action = "restart"
```

### 8.1 配置原则

1. 配置必须静态可解析，不执行用户代码。
2. task name 是全局唯一主键。
3. 默认值只能来自 `[defaults]` 或内置默认。
4. 所有时间相关配置都必须落到明确 timezone。
5. 所有路径在 validate 阶段规范化，但保留原始配置用于展示。

## 9. DAG 编排语义

task 通过 `depends_on` 声明依赖关系。

启动时：

1. 校验 DAG 无环。
2. 计算拓扑顺序。
3. 先启动上游 task。
4. 根据 `start_when` 决定下游 task 是否可以启动。

关闭时：

1. 计算反向拓扑顺序。
2. 先停止依赖方。
3. 再停止被依赖方。

示例：

```text
master -> ctp_md -> strategy
```

启动顺序：

```text
master, ctp_md, strategy
```

关闭顺序：

```text
strategy, ctp_md, master
```

### 9.1 start_when

| 值 | 含义 |
| --- | --- |
| `dependencies_started` | 上游进程 spawn 成功即可启动 |
| `dependencies_healthy` | 上游 health probe 成功后才启动 |
| `manual` | 不随依赖自动启动，只能手动启动 |

默认值建议为 `dependencies_healthy`。如果上游没有配置 health probe，则上游进入
`online` 后视为满足条件，但 validate 应该给出 warning。

## 10. 状态机

状态集合：

```text
defined
scheduled
waiting_dependency
starting
online
healthy
unhealthy
stopping
stopped
failed
backoff
disabled
```

| 状态 | 含义 |
| --- | --- |
| `defined` | 配置存在，但尚未启动 |
| `scheduled` | 当前不在运行窗口内，等待 schedule |
| `waiting_dependency` | 依赖未满足 |
| `starting` | 正在 spawn 或等待初次 probe |
| `online` | 子进程已存在 |
| `healthy` | health probe 成功 |
| `unhealthy` | 子进程存在，但 health probe 失败 |
| `stopping` | 正在优雅关闭 |
| `stopped` | 已停止 |
| `failed` | 退出且不再自动重启 |
| `backoff` | 失败后等待下一次重启 |
| `disabled` | 被配置或命令禁用 |

关键原则：

1. `online` 只代表 PID 存在。
2. `healthy` 才代表服务可供依赖方使用。
3. DAG 依赖不应该默认绑定到 PID 状态。

## 11. Health probe

第一版建议支持：

| 类型 | 说明 |
| --- | --- |
| `tcp` | 尝试连接 host:port |
| `http` | 请求 URL 并检查 status code |
| `command` | 执行命令，退出码 0 表示成功 |
| `file` | 检查文件存在或 mtime |

示例：

```toml
[tasks.api.health]
type = "http"
url = "http://127.0.0.1:8060/healthz"
interval = "1s"
timeout = "500ms"
success_after = 2
failure_after = 3
```

probe 失败不一定立即杀进程。是否重启由 task 的 health failure policy 决定。

## 12. Restart policy

建议支持：

| 策略 | 含义 |
| --- | --- |
| `never` | 不自动重启 |
| `on-failure` | 非 0 退出码或信号退出时重启 |
| `always` | 任何退出都重启，除非是用户主动 stop |

补充参数：

```toml
restart_delay = "3s"
max_restarts = 10
backoff = "exponential"
max_backoff = "60s"
```

重启计数必须区分：

1. 用户主动 restart。
2. 崩溃自动 restart。
3. schedule 或 cron 触发的 restart。

这三类原因都需要进入 event log。

## 13. Schedule 和 cron-like actions

`schedule` 表达运行窗口：

```toml
[tasks.ctp_md.schedule]
start = "0 8 * * 1-5"
stop = "0 16 * * 1-5"
```

`cron` 表达周期动作：

```toml
[tasks.strategy.cron.daily_restart]
expr = "30 8 * * 1-5"
action = "restart"
```

支持的 action：

| action | 含义 |
| --- | --- |
| `start` | 启动 task |
| `stop` | 停止 task |
| `restart` | 重启 task |
| `reload` | 执行 task 声明的 reload 动作 |
| `command` | 执行一次性命令，不改变 task 主进程 |

时间语义：

1. 所有 cron 表达式按 `[project].timezone` 解释。
2. CLI 表格时间按 `[project].display_timezone` 格式化，默认 `local`；表格行内不展示 offset，表格后输出 `Timezone: ...` 附注。
3. 如果系统休眠错过触发时间，第一版默认不补跑。
4. daemon 重启后重新计算下一次触发时间。
5. schedule 触发的 start 仍需遵守 DAG 依赖。

## 14. 日志

每个 task 默认接管 stdout 和 stderr。

建议目录：

```text
~/.rspm/
├── run/
│   └── rspmd.sock
├── logs/
│   └── <project>/
│       └── <task>.log
├── state/
│   └── <project>.json
└── events/
    └── <project>.jsonl
```

日志能力：

1. `rspm logs <task>` 查看历史日志。
2. `rspm logs <task> -f` tail 实时日志。
3. stdout 和 stderr 可合并，也可分别保存。
4. 日志轮转按 size 和时间配置。

## 15. CLI

核心命令：

```bash
rspm validate -f rspm.toml
rspm apply -f rspm.toml
rspm start <task|group|all>
rspm start <task_id...>
rspm stop <task|group|all>
rspm restart <task|group|all>
rspm ls
rspm status
rspm describe <task>
rspm log <task|task_id>
rspm log <task|task_id> --no-follow
rspm logs <task> -f
rspm graph
rspm events
rspm doctor
rspm service install
rspm service uninstall
```

`rspm ls` / `rspm status` 示例：

```text
NAME       PID     STATUS       HEALTH   RESTARTS   UPTIME   CPU    MEM    NEXT
master     18321   healthy      ok       0          2h14m    0.3%   41MB   -
ctp_md     18389   healthy      ok       1          2h10m    1.8%   96MB   stop 16:00
strategy   -       waiting      -        0          -        -      -      after ctp_md
```

CLI 表格是展示层。SDK 不解析 CLI 输出，而是使用结构化 API。

## 16. Rust SDK

Rust SDK 示例：

```rust
use rspm_sdk::RspmClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = RspmClient::connect_default().await?;

    client.apply_file("rspm.toml").await?;
    client.start("master").await?;
    client.wait_healthy("master").await?;

    let tasks = client.list_tasks().await?;
    for task in tasks {
        println!("{} {:?}", task.name, task.status);
    }

    Ok(())
}
```

核心 API：

```rust
client.validate_config(path)
client.apply_file(path)
client.list_tasks()
client.describe_task(name)
client.start(name)
client.stop(name)
client.restart(name)
client.reload(name)
client.tail_logs(name)
client.watch_events()
client.wait_status(name, status)
client.wait_healthy(name)
```

## 17. Python SDK

同步 API：

```python
from rspm import RspmClient

client = RspmClient.connect_default()
client.apply_file("rspm.toml")
client.start("master")
client.wait_healthy("master", timeout=30)

for task in client.list_tasks():
    print(task.name, task.status, task.pid)
```

异步 API：

```python
from rspm.aio import AsyncRspmClient

async with AsyncRspmClient.connect_default() as client:
    await client.start("strategy")
    async for event in client.watch_events(task="strategy"):
        print(event)
```

Python SDK 返回结构化对象：

```text
TaskInfo
- name
- pid
- status
- health
- uptime_ms
- restart_count
- last_exit_code
- cwd
- cmd
- dependencies
- dependents
- schedule_state
```

## 18. Event log

所有状态变化必须写入 event log，便于排查和审计。

事件字段：

```text
timestamp
project
task
event_type
status_before
status_after
reason
pid
exit_code
signal
message
```

典型事件：

```text
task_started
task_healthy
task_unhealthy
task_exited
task_restarted
task_stopped
dependency_waiting
schedule_triggered
cron_triggered
config_applied
```

## 19. 开机自启

`rspm` 的日常控制命令会在本地 control plane 不可达时自动拉起 `rspm daemon`。`rspm service install`
仍可用于需要开机自启或登录后常驻的部署场景。

| 平台 | 后端 |
| --- | --- |
| Linux | systemd user service 或 system service |
| macOS | launchd |
| Windows | Windows Service 或 Task Scheduler |

daemon 启动后读取已 apply 的 project 配置，并启动 autostart task。运行态 state 不应覆盖
TOML 声明。

## 20. 技术选型

| 领域 | 建议 |
| --- | --- |
| CLI | `clap` |
| async runtime | `tokio` |
| TOML | `serde` + `toml` |
| 表格输出 | `comfy-table` 或 `tabled` |
| DAG | `petgraph` |
| cron | `croner` 或 `cron` |
| 文件 watch | `notify` |
| 进程信息 | `sysinfo` |
| 日志 | `tracing` + `tracing-subscriber` |
| 错误处理 | `thiserror` + `anyhow` |
| Python binding | 纯 Python JSON-RPC client，必要时再引入 `pyo3` |

## 21. 阶段验收

### P0: 基础 task manager

验收标准：

1. 可以解析、校验和 apply `rspm.toml`。
2. 可以启动、停止、重启单个 task。
3. `rspm ls` 和 `rspm status` 能输出带 `TASK_ID` 的 task 表格，`STATUS`/`HEALTH` 带颜色。
4. `rspm start`、`rspm stop`、`rspm restart` 支持多个 task name 或 task id，并在执行后刷新一次表格。
5. `rspm log` 默认持续跟随单个任务日志；每行日志前缀为 `<task_name> | `，并保留原始 ANSI 样式。
4. `rspm logs <task> -f` 能 tail 日志。
5. daemon 重启后能恢复已 apply 配置。

### P1: DAG 和 health

验收标准：

1. `depends_on` 能按拓扑顺序启动。
2. stop 能按反向拓扑顺序关闭。
3. 支持 tcp/http/command/file health probe。
4. 下游 task 可以等待上游 `healthy`。
5. 循环依赖能在 validate 阶段报错。

### P2: 调度和 restart 策略

验收标准：

1. 支持定时 start 和 stop。
2. 支持 cron-like restart。
3. 支持 `never`、`on-failure`、`always`。
4. 支持 restart delay、max restarts 和 backoff。
5. 支持 watch restart 和 memory restart。

### P3: SDK

验收标准：

1. Rust SDK 能完成 apply、start、stop、restart、list、describe、wait_healthy。
2. Python SDK 提供同步 API。
3. Python SDK 提供异步 event stream。
4. CLI 基于同一套 SDK 或同一套 RPC client 实现。

### P4: 平台集成和诊断

验收标准：

1. 支持 Linux systemd service install。
2. 支持 macOS launchd install。
3. 支持 Windows service 或 Task Scheduler install。
4. `rspm doctor` 能检查 daemon、socket、权限、日志目录和配置状态。
5. `rspm graph` 能输出 text、dot 或 json 格式依赖图。

## 22. 设计原则

1. 配置即事实来源。
2. SDK 和 CLI 使用同一条控制路径。
3. PID 存在不代表服务可用。
4. 所有时间、路径、退出原因和重启原因都要可追踪。
5. 第一版优先保证确定性和可观测性，不追求复杂平台能力。
