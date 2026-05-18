# rspm

`rspm` is a Rust task process manager designed as a deterministic, TOML-based alternative to PM2.
It focuses on local process supervision, DAG startup order, health checks, scheduling, structured
events, and SDK-driven automation.

## Current Workspace

```text
crates/rspm-core     configuration, state, DAG, schedule, events, RPC payloads
crates/rspm-daemon   process runtime, health checks, orchestration, scheduler, daemon API, TCP/Unix server
crates/rspm-sdk      Rust SDK, in-process client, TCP fallback client, Unix socket client
crates/rspm          CLI package and daemon bootstrap entrypoint
python/rspm          Python SDK
```

## CLI

Validate a config:

```bash
cargo run -p rspm -- validate -f examples/rspm.toml
```

Dry-run apply and inspect the DAG plan:

```bash
cargo run -p rspm -- apply -f examples/rspm.toml --dry-run
```

Print the dependency graph:

```bash
cargo run -p rspm -- graph -f examples/rspm.toml
```

List tasks in a daemon-backed table. If the local daemon is not running, `rspm` starts it:

```bash
cargo run -p rspm -- ls -f examples/rspm.toml
```

Apply and operate tasks without starting `rspmd` manually:

```bash
cargo run -p rspm -- apply -f examples/rspm.toml
cargo run -p rspm -- start all
cargo run -p rspm -- ls
cargo run -p rspm -- start 1 2 3
cargo run -p rspm -- describe master
cargo run -p rspm -- log 1
cargo run -p rspm -- log 1 --no-follow
cargo run -p rspm -- logs master
cargo run -p rspm -- logs master -f
cargo run -p rspm -- events
cargo run -p rspm -- doctor
```

## Rust SDK

```rust
use rspm_core::config::ProjectConfig;
use rspm_daemon::runtime::TaskRuntime;
use rspm_sdk::RspmClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ProjectConfig::from_toml_str(std::fs::read_to_string("examples/rspm.toml")?.as_str())?;
    let runtime = TaskRuntime::new(config, ".rspm/logs")?;
    let mut client = RspmClient::from_runtime(runtime);

    client.start("master").await?;
    client.wait_healthy("master", std::time::Duration::from_secs(30)).await?;
    let tasks = client.list_tasks().await?;
    println!("{tasks:#?}");

    Ok(())
}
```

## Python SDK

```python
from rspm import RspmClient

client = RspmClient.connect_tcp("127.0.0.1", 27691)
task = client.start("master")
client.wait_healthy("master", timeout=30)
print(task.name, task.status, task.pid)
```

Async:

```python
from rspm.aio import AsyncRspmClient

async with AsyncRspmClient.connect_tcp("127.0.0.1", 27691) as client:
    task = await client.start("strategy")
    print(task.name, task.status)
```

## Verification

```bash
cargo test --workspace
cd python && uv run python -m pytest -q
```

## Design Status

Implemented:

- TOML config parsing and validation.
- Static rejection of empty/non-task configs.
- 5-field and 6-field cron parsing.
- Cron expressions interpreted through the project timezone for UTC, Asia/Shanghai, and fixed
  UTC/GMT offsets.
- DAG validation and start/stop ordering.
- Task state and event payloads.
- Real process spawn, stop, restart, log capture, configured log rotation, and status reporting.
- Crash restart for `always` and `on-failure` policies, including restart delay, exponential
  backoff, max backoff, and max restart limits.
- Watch restart using deterministic file mtime reconciliation.
- Memory restart using configured byte limits and process RSS sampling on Linux.
- Command, file, TCP, and minimal HTTP health checks.
- DAG start orchestration with health gating and healthy/unhealthy task status updates.
- Schedule and cron due-action collection plus daemon tick execution for start, stop, restart,
  reload, and one-shot command actions.
- `rspmd` background maintenance loop for scheduled actions, restart reconciliation, watch restart,
  memory restart, and health checks.
- Applied config persistence under the configured state directory. On restart, `rspmd` prefers the
  previously applied config and starts autostart tasks from that declared config.
- JSONL event persistence when an event log path is configured.
- JSON-RPC-style request/response payloads.
- Daemon API handler for task start, stop, restart, wait, logs, events, describe, list, start_all,
  stop_all, reload, and config.apply.
- Reload via configured command or Unix signal, with explicit errors for unconfigured reload.
- TCP JSON-line fallback daemon transport, Unix socket daemon transport on Unix platforms, and
  Windows named-pipe transport behind `cfg(windows)`.
- Rust SDK in-process client, TCP fallback client, Unix socket client on Unix platforms, and
  Windows named-pipe client behind `cfg(windows)`.
- Python SDK request builders plus sync/async TCP fallback clients and structured `TaskInfo`
  results for daemon-backed calls.
- CLI validate, apply, graph text/dot/json, ls, status, monit, describe, start, stop, restart, reload,
  logs with follow mode, events, doctor, and service install/uninstall commands. `ls` exposes a
  stable `TASK_ID` for the current applied project, and `start`/`stop`/`restart` accept multiple
  task names or IDs. Control commands print a fresh task table after execution. `log`/`logs`
  accept task names or IDs, prefix each line with `task_name | `, and preserve ANSI terminal
  styling from task output. `log` follows by default; use `--no-follow` for a one-shot view.
- CLI-managed daemon bootstrap: daemon-backed commands start `rspm daemon` automatically when the
  local control plane is not already reachable.

Operational notes:

- `rspm service install` writes platform service templates to the default user-service path or an
  explicit `--output` path. With `--dry-run --activate`, it prints platform activation commands;
  with `--activate` in normal mode, it executes those commands.
- Cron scheduling uses the `chrono-tz` IANA timezone database and also accepts fixed
  `UTC/GMT±offset` strings.
- Windows named-pipe transport is covered by `cargo check --target x86_64-pc-windows-gnu`;
  runtime named-pipe behavior still needs to be exercised on a Windows host.
