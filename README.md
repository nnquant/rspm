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
cargo run -p rspm -- log
cargo run -p rspm -- log --no-history
cargo run -p rspm -- log 1
cargo run -p rspm -- log 1 --no-follow
cargo run -p rspm -- logs
cargo run -p rspm -- logs master
cargo run -p rspm -- logs master --lines 100
cargo run -p rspm -- logs --grep ERROR
cargo run -p rspm -- logs --since 2026-05-19T09:30:00Z
cargo run -p rspm -- logs --merge
cargo run -p rspm -- logs master -f
cargo run -p rspm -- logs all -f --no-history
cargo run -p rspm -- events
cargo run -p rspm -- doctor
cargo run -p rspm -- monit --once
cargo run -p rspm -- daemon status -f examples/rspm.toml
cargo run -p rspm -- daemon restart -f examples/rspm.toml
RSPM_TOKEN=local-secret cargo run -p rspm -- --token local-secret ls
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
    let logs = client.logs_all().await?;
    println!("{tasks:#?}");
    println!("{logs:#?}");

    Ok(())
}
```

## Python SDK

```python
from rspm import RspmClient

client = RspmClient.connect_tcp("127.0.0.1", 27691)
# Optional when rspmd is started with an auth token.
# client = client.with_token("local-secret")
task = client.start("master")
client.wait_healthy("master", timeout=30)
logs = client.logs_all()
print(task.name, task.status, task.pid)
print(logs)
```

Async:

```python
from rspm.aio import AsyncRspmClient

async with AsyncRspmClient.connect_tcp("127.0.0.1", 27691) as client:
    # Optional when rspmd is started with an auth token.
    # client = client.with_token("local-secret")
    task = await client.start("strategy")
    logs = await client.logs_all()
    print(task.name, task.status)
    print(logs)
```

## Verification

```bash
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo check --workspace --target x86_64-pc-windows-gnu
cd python && uv run python -m pytest -q
```

See `docs/validation.md`, `docs/completion-audit.md`, and `examples/README.md` for the
cross-platform gate matrix, requirement audit, and daemon-backed example smoke procedure.

## Local Install

Install the CLI from this workspace:

```bash
cargo install --path crates/rspm --locked
```

If the dependency index is already cached and the network is unavailable, use:

```bash
cargo install --path crates/rspm --locked --offline
```

After installation, use `rspm` directly instead of `cargo run -p rspm --`:

```bash
rspm apply -f examples/tasks.rspm.toml
rspm ls
rspm log all --no-follow --lines 20 --merge
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
- Daemon startup compensation for tasks whose schedule start/stop window is already active,
  including DAG dependency startup.
- `rspmd` background maintenance loop for scheduled actions, restart reconciliation, watch restart,
  memory restart, and health checks.
- Applied config persistence under the configured state directory. On restart, `rspmd` prefers the
  previously applied config and starts autostart tasks from that declared config.
- JSONL event persistence when an event log path is configured. Runtime startup restores lifecycle
  state from the event log and reattaches still-running task PIDs for describe/stop/restart flows.
- JSON-RPC-style request/response payloads.
- Optional local control-plane token authentication. When a daemon is started with a token, CLI and
  SDK requests must include the same token through `--token`, `RSPM_TOKEN`, or SDK `with_token`.
- Daemon API handler for task start, stop, restart, wait, logs, events, describe, list, start_all,
  stop_all, reload, and config.apply.
- Reload via configured command or Unix signal, with explicit errors for unconfigured reload.
- TCP JSON-line fallback daemon transport, Unix socket daemon transport on Unix platforms, and
  Windows named-pipe transport behind `cfg(windows)`.
- Rust SDK in-process client, TCP fallback client, Unix socket client on Unix platforms, and
  Windows named-pipe client behind `cfg(windows)`.
- Python SDK request builders plus sync/async TCP fallback clients, aggregate log helpers, and
  structured `TaskInfo` results for daemon-backed calls.
- CLI validate, apply, graph text/dot/json, ls, status, monit, describe, start, stop, restart, reload,
  logs with follow mode, events, doctor, daemon start/stop/restart/status, and service
  install/uninstall/start/stop/restart/status commands. `ls` exposes a stable `TASK_ID` for the current applied project, and
  `start`/`stop`/`restart` accept multiple task names or IDs. Control commands print a fresh task
  table after execution. `log`/`logs` accept task names, IDs, `all`, or no task for aggregate
  output. They prefix each line with `task_name | ` and preserve ANSI terminal styling from task
  output. `log` follows by default; use `--no-follow` for a one-shot view. Follow mode prints
  existing history first by default; use `--no-history` to show only newly appended log lines.
  `--lines N` tails each selected task log and `--grep TEXT` filters matching log lines before
  prefixing. `--since RFC3339` keeps timestamped lines at or after the given instant and drops
  untimestamped lines for that query. `--merge` orders aggregate output by RFC3339 timestamps when
  log lines contain them, while preserving task-local order for lines without parseable timestamps.
- CLI-managed daemon bootstrap: daemon-backed commands start `rspm daemon` automatically when the
  local control plane is not already reachable. `rspm daemon stop` first stops managed tasks through
  the daemon API, then stops the daemon process recorded in the configured state directory.
- Daemon bootstrap detaches the background process on Unix and starts the control transport before
  running autostart/scheduled startup tasks, so slow task health checks do not make the daemon look
  unreachable during CLI startup.
- `rspm doctor` reports daemon reachability plus config, log directory, state directory, pid-file,
  applied config, event-log, socket path, cwd write permission, and task-count diagnostics.
- `rspm monit` refreshes the task monitor table; `rspm monit --once` prints a script-friendly
  monitor snapshot and exits.

Operational notes:

- `rspm service install` writes platform service templates to the default user-service path or an
  explicit `--output` path. With `--dry-run --activate`, it prints platform activation commands;
  with `--activate` in normal mode, it executes those commands.
- `rspm service status --dry-run` prints the platform status command. Without `--dry-run`, it runs
  the read-only service status command for systemd user services, launchd agents, or Windows
  scheduled tasks.
- `rspm service start|stop|restart --dry-run` prints the platform control command. Without
  `--dry-run`, rspm executes the corresponding systemd user, launchd, or Windows scheduled-task
  command.
- Cron scheduling uses the `chrono-tz` IANA timezone database and also accepts fixed
  `UTC/GMT±offset` strings.
- CI runs Rust format, tests, clippy, and daemon-backed smoke tests on Linux, Rust workspace/test-binary
  compilation plus service dry-run CLI tests and daemon-backed smoke tests on macOS and Windows,
  Windows named-pipe transport tests, and Python SDK tests on Linux. Local smoke testing remains
  useful before release cuts.
- Tag pushes matching `v*` build release binaries on Linux, macOS, and Windows and upload them as
  workflow artifacts.
- Windows named-pipe transport has Windows-only daemon/server and SDK transport tests in CI.
