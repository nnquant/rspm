# rspm completion audit

Date: 2026-05-19

## Scope

This audit maps the requested rspm requirements to concrete project artifacts and verification
commands. It separates local evidence from platform checks that require macOS or Windows hosts.

## Checklist

| Requirement | Evidence |
| --- | --- |
| Rust-based process manager to replace PM2 | Rust workspace in `crates/`; design tradeoffs in `docs/design.md`; CLI binary `rspm`. |
| PM2 major daily features | `start`, `stop`, `restart`, `ls/status`, `log/logs`, `monit`, restart policy, watch restart, memory restart, service startup commands in `crates/rspm/src/main.rs` and daemon runtime tests. |
| No cluster support | Non-goal documented in `docs/design.md`; no cluster command/config exists. |
| Cross-platform support | TCP transport, Unix socket, Windows named pipe cfg/tests, systemd/launchd/schtasks command generation, Windows target compile gate. |
| TOML orchestration replacing ecosystem files | `rspm-core` config model and validation; `examples/tasks.rspm.toml`; config tests. |
| DAG task orchestration | `rspm-core/src/dag.rs`; start/stop order tests; daemon orchestration tests. |
| Scheduled start/stop | `rspm-core/src/schedule.rs`; scheduled active startup compensation; schedule tests and daemon scheduler tests. |
| Cron-like periodic schedule | cron action model, parser, scheduler tests, `periodic_pulse` example task. |
| Docker-style CLI table | `print_task_status` and CLI tests for `TASK_ID`, `NAME`, `PID`, `STATUS`, `START_TIME`, `STOP_TIME`, `NEXT`. |
| `TASK_ID` multi-target operations | CLI target resolution and `cli_start_accepts_multiple_task_ids` test. |
| `rspm-cli -> rspm` | CLI package and binary are named `rspm`; `rspm --version` test. |
| CLI-managed daemon | `DaemonLaunch`, `rspm daemon start/stop/restart/status`; daemon lifecycle CLI tests. |
| Default port not 17691 | `127.0.0.1:27691` default in CLI and daemon command defaults. |
| `ls` command | `Command::Ls`; CLI table tests; real smoke output. |
| Post-action table after start/stop/restart | Control command handlers call `print_daemon_status`; CLI tests assert table output. |
| `log` and aggregate `logs` | CLI `log`/`logs`, aggregate targets, `--no-history`, `--lines`, `--grep`, `--since`, `--merge`; CLI tests and smoke script. |
| Preserve ANSI terminal styles | Log path preserves raw bytes; CLI test checks ANSI output. |
| Colored status/health/restarts/cpu/mem | CLI color helpers and tests. |
| START_TIME/STOP_TIME/MODE/UPTIME | `TaskInfo` display and tests; real smoke output. |
| Rust SDK task operations | `rspm-sdk` in-process/TCP/Unix/Windows client code and SDK tests. |
| Python SDK task operations | `python/rspm/client.py`, `python/rspm/aio.py`, Python tests. |
| Simulated example tasks | `examples/tasks/*.py`, `examples/tasks.rspm.toml`, `examples/README.md`. |
| Service lifecycle commands | `rspm service install/uninstall/status/start/stop/restart`; dry-run tests locally and in macOS/Windows CI. |
| Release/install path | `cargo install --path crates/rspm --locked --offline` verified locally; release workflow artifacts in `.github/workflows/release.yml`. |
| CI gates | `.github/workflows/ci.yml`; local gate list in `docs/validation.md`. |
| Smoke scripts | `scripts/smoke-posix.sh` verified locally; `scripts/smoke-windows.ps1` added for Windows host validation. |
| Windows named-pipe runtime | Windows-only daemon/server and SDK named-pipe tests in `server_tests.rs` and `transport_tests.rs`; CI runs them on Windows. |

## Verified locally

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo check --workspace --target x86_64-pc-windows-gnu
cargo test --workspace --no-run
cargo build --release -p rspm --locked
target/release/rspm --version
cd python && uv run python -m pytest -q
RSPM_SMOKE_ADDR=127.0.0.1:27794 \
  RSPM_SMOKE_ROOT=/tmp/rspm-smoke-script2-20260519 \
  scripts/smoke-posix.sh
RSPM_SMOKE_ADDR=127.0.0.1:27795 \
  RSPM_SMOKE_ROOT=/tmp/rspm-smoke-assert-20260519 \
  scripts/smoke-posix.sh
RSPM_SMOKE_ADDR=127.0.0.1:27796 \
  RSPM_SMOKE_ROOT=/tmp/rspm-smoke-final-20260519 \
  scripts/smoke-posix.sh
```

The Linux smoke run validates automatic daemon startup, `apply`, `ls`, task-id `start 1 3`,
aggregate logs, `stop all`, daemon shutdown, and no leftover daemon/example task processes. The
smoke scripts assert key output markers such as `TASK_ID`, `START_TIME`, task ids, and
`task_name | ` log prefixes. The final smoke run also asserts `doctor` diagnostics and service
dry-run command output.

## Accepted external validation skips

These items cannot be proven inside the current Linux workspace and are accepted skips for this
turn because target-platform execution is unavailable.

| Item | Required environment | Prepared artifact |
| --- | --- | --- |
| macOS runtime smoke | macOS host or GitHub macOS runner | `scripts/smoke-posix.sh`, CI `platform-smoke-macos-latest`. |
| Windows runtime smoke | Windows host or GitHub Windows runner | `scripts/smoke-windows.ps1`, CI `platform-smoke-windows-latest`. |
| launchd activation | macOS host with user launchd | `rspm service install --activate`, dry-run/status command coverage. |
| Windows scheduled-task activation | Windows host | `rspm service install --activate`, dry-run/status command coverage. |
