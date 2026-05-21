# rspm

`rspm` is a local-first process manager for deterministic task orchestration.
It is written in Rust, configured with TOML, and designed for research
workstations, trading infrastructure, local service stacks, and other systems
where process state must be explicit, auditable, and scriptable.

`rspm` keeps the day-to-day ergonomics of tools such as PM2 while avoiding a
Node.js-specific runtime model. Tasks are language-agnostic child processes,
configuration is declarative, startup order is modeled as a DAG, and the same
daemon control plane is available through the CLI, Rust SDK, and Python SDK.

## Highlights

- **Declarative TOML**: define projects, tasks, defaults, schedules, probes,
  restart policies, environment variables, and log retention in one file.
- **Task DAGs**: start dependencies first, wait for health when configured, and
  stop dependents before upstream services.
- **Real process supervision**: handle start, stop, restart, reload, crash
  recovery, restart backoff, watch restarts, memory restarts, and log capture.
- **Local daemon control plane**: operate through `rspmd` over Unix sockets,
  Windows named pipes, or local TCP fallback.
- **Operator-friendly CLI**: validate configs, apply projects, inspect status,
  stream logs, view events, run doctor checks, and bootstrap the daemon from one
  command surface.
- **Rust and Python SDKs**: embed `rspm` behind host-facing tools without making
  users manage the daemon manually.
- **Cross-platform focus**: Linux, macOS, and Windows are covered by CI smoke
  tests and platform-specific transport checks.

## Status

`rspm` is an early `0.1.x` project, but the core workflow is already usable:

- config parsing and validation
- DAG planning and execution order
- daemon-backed task lifecycle controls
- restart policies, health probes, schedules, and cron actions
- structured task state and event records
- CLI table/log/event rendering
- Rust SDK and Python SDK clients
- detached sidecar supervisor for host integration
- CI, release artifact workflow, and local smoke scripts

See [docs/design.md](docs/design.md) for the full design rationale and
[docs/validation.md](docs/validation.md) for the validation matrix.

## Install

Install the CLI from this repository:

```bash
cargo install --path crates/rspm --locked
```

If your dependency cache is already warm and the network is unavailable:

```bash
cargo install --path crates/rspm --locked --offline
```

During development, run the CLI directly from the workspace:

```bash
cargo run -p rspm -- --help
```

The Python SDK lives in `python/` and uses `uv` for local development:

```bash
cd python
uv run python -m pytest -q
```

## Quick Start

Validate the example project:

```bash
cargo run -p rspm -- validate -f examples/tasks.rspm.toml
```

Apply it. Daemon-backed commands automatically start a local `rspmd` sidecar
when one is not already reachable.

```bash
cargo run -p rspm -- apply -f examples/tasks.rspm.toml
```

Inspect tasks:

```bash
cargo run -p rspm -- ls
cargo run -p rspm -- status
cargo run -p rspm -- describe long_watcher
```

Start tasks by name or stable task id:

```bash
cargo run -p rspm -- start long_watcher
cargo run -p rspm -- start 1 3
```

Read logs and events:

```bash
cargo run -p rspm -- log all --no-follow --lines 20 --merge
cargo run -p rspm -- logs long_watcher -f
cargo run -p rspm -- events
```

Stop managed tasks and the daemon:

```bash
cargo run -p rspm -- stop all
cargo run -p rspm -- daemon stop
```

For an end-to-end smoke procedure, run:

```bash
scripts/smoke-posix.sh
```

On Windows PowerShell:

```powershell
.\scripts\smoke-windows.ps1
```

## Configuration

A project is a TOML file with a `[project]` section, optional `[defaults]`, and
one or more `[tasks.<name>]` entries.

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
cwd = "/srv/trading"
autostart = true
restart = "always"

[tasks.master.health]
type = "tcp"
address = "127.0.0.1:17690"
interval = "1s"
timeout = "500ms"
success_after = 2
failure_after = 3

[tasks.gateway]
cmd = "uv"
args = ["run", "ldc-ctp-md"]
cwd = "/srv/trading"
depends_on = ["master"]
start_when = "dependencies_healthy"
restart = "on-failure"

[tasks.gateway.schedule]
start = "0 8 * * 1-5"
stop = "0 16 * * 1-5"
```

Important configuration rules:

- TOML is the source of truth; runtime state does not silently rewrite declared
  task definitions.
- Task names are the stable logical identifiers.
- Timezone-sensitive scheduling uses explicit project timezones.
- A five-field cron expression is normalized with a zero seconds field; six-field
  expressions are also supported.
- Health checks can gate dependent startup when `start_when =
  "dependencies_healthy"` is set.

## CLI Reference

Common commands:

```bash
rspm validate -f examples/tasks.rspm.toml
rspm apply -f examples/tasks.rspm.toml --dry-run
rspm graph -f examples/tasks.rspm.toml
rspm apply -f examples/tasks.rspm.toml
rspm ls
rspm monit --once
rspm start all
rspm stop all
rspm restart long_watcher
rspm reload gateway
rspm describe gateway
rspm logs gateway --lines 100
rspm logs all --grep ERROR --merge
rspm events
rspm doctor
rspm daemon status -f examples/tasks.rspm.toml
```

Use `--no-daemon` for local config editing or validation flows that should not
auto-start a sidecar:

```bash
rspm --no-daemon add --name worker --cwd /srv/app --env RUST_LOG=info "uv run worker.py"
```

Use `RSPM_TOKEN` or `--token` when a daemon is started with local control-plane
authentication:

```bash
RSPM_TOKEN=local-secret rspm --token local-secret ls
```

## Rust SDK

Use `RspmSupervisor` when embedding `rspm` in another Rust application. The host
owns the sidecar daemon lifecycle; `rspmd` owns managed task lifecycles.

```rust
use rspm_sdk::RspmSupervisor;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = RspmSupervisor::new()
        .state_dir(".myapp/rspm/state")
        .log_dir(".myapp/rspm/logs")
        .socket_path(".myapp/rspm/run/rspmd.sock")
        .ensure_daemon("examples/tasks.rspm.toml")
        .await?;

    client.apply_file("examples/tasks.rspm.toml").await?;
    client.start("long_watcher").await?;
    client
        .wait_healthy("long_watcher", std::time::Duration::from_secs(30))
        .await?;

    let tasks = client.list_tasks().await?;
    print!("{}", rspm_sdk::render::format_task_table(&tasks));

    Ok(())
}
```

## Python SDK

The Python package exposes sync and async clients plus the same detached sidecar
supervisor model.

```python
from rspm import RspmSupervisor
from rspm.render import format_task_table

client = RspmSupervisor(
    state_dir=".myapp/rspm/state",
    log_dir=".myapp/rspm/logs",
    socket_path=".myapp/rspm/run/rspmd.sock",
).ensure_daemon("examples/tasks.rspm.toml")

client.apply_file("examples/tasks.rspm.toml")
client.start("long_watcher")
client.wait_healthy("long_watcher", timeout=30)

print(format_task_table(client.list_tasks()), end="")
```

Async TCP fallback:

```python
from rspm.aio import AsyncRspmClient

async with AsyncRspmClient.connect_tcp("127.0.0.1", 27691) as client:
    task = await client.start("long_watcher")
    print(task.name, task.status)
```

## Architecture

```text
rspm.toml
   |
   v
rspm-core     config, DAG, schedule, state, event, RPC types
   |
   v
rspm-daemon   runtime supervision, health, logs, scheduler, RPC server
   |
   +--> rspm CLI
   +--> rspm-sdk Rust client and supervisor
   +--> python/rspm sync and async clients
```

Workspace layout:

```text
crates/rspm-core     shared config, state, scheduling, DAG, and API types
crates/rspm-daemon   daemon runtime and local transports
crates/rspm-sdk      Rust client, supervisor, transport, and render helpers
crates/rspm          CLI and daemon bootstrap entrypoint
python/rspm          Python SDK
docs/                design notes, validation matrix, and completion audit
examples/            runnable example projects and smoke-task scripts
```

## Development

Required tools:

- Rust stable toolchain
- Python 3.10+
- [`uv`](https://docs.astral.sh/uv/) for Python package and virtual environment
  management

Run the main verification gates:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --target x86_64-pc-windows-gnu
cd python && uv run python -m pytest -q
```

Release tags matching `v*` build Linux, macOS, and Windows binaries through
[.github/workflows/release.yml](.github/workflows/release.yml).

## Contributing

Issues and pull requests are welcome. For changes that affect runtime behavior,
configuration semantics, daemon transports, SDK contracts, or CLI output, include
tests that exercise the real boundary rather than only mocking the call site.

Before opening a pull request:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cd python && uv run python -m pytest -q
```

Please keep public APIs explicit and deterministic. Timezones, paths, restart
rules, and health semantics should be visible in configuration or SDK calls.

## Security

`rspm` is a local process manager. Treat config files as privileged inputs: they
define executable commands and environment variables. Do not commit secrets in
TOML files, logs, examples, or CI configuration.

When exposing a daemon beyond the default local transports, use token
authentication and bind only to trusted interfaces.

## License

`rspm` is licensed under the [MIT License](LICENSE).
