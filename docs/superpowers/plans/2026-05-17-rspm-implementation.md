# rspm Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `docs/design.md` process manager as a working Rust workspace with CLI,
daemon APIs, task configuration, DAG orchestration, scheduling primitives, logs, Rust SDK, and
Python SDK.

**Architecture:** The project is a Rust workspace split into focused crates. `rspm-core` owns
configuration, validation, state, DAG planning, schedules, events, and API payloads. `rspm-daemon`
owns runtime process supervision and local state. `rspm-sdk` is the Rust client used by `rspm`;
the Python SDK mirrors the same JSON-RPC-style API surface.

**Tech Stack:** Rust 2021, Cargo workspace, `clap`, `serde`, `toml`, `tokio`, `thiserror`,
`anyhow`, `petgraph`, `chrono`, `cron`, `sysinfo`, `notify`, `tabled`; Python package managed by
`uv` using stdlib HTTP/JSON clients.

---

## Task 1: Workspace and Core Contract

**Files:**
- Create: `Cargo.toml`
- Create: `crates/rspm-core/Cargo.toml`
- Create: `crates/rspm-core/src/lib.rs`
- Create: `crates/rspm-core/src/config.rs`
- Create: `crates/rspm-core/src/error.rs`
- Create: `crates/rspm-core/src/state.rs`
- Test: `crates/rspm-core/tests/config_tests.rs`

- [ ] Write failing config parsing tests for defaults, task env, health, dependencies, schedule,
      and cron actions.
- [ ] Run `cargo test -p rspm-core config_tests -- --nocapture` and confirm the crate or tests fail
      because the workspace is not implemented yet.
- [ ] Implement config structs, enums, defaults, TOML parsing, and typed errors.
- [ ] Re-run `cargo test -p rspm-core config_tests -- --nocapture` and confirm the tests pass.

## Task 2: DAG Validation and Planning

**Files:**
- Create: `crates/rspm-core/src/dag.rs`
- Test: `crates/rspm-core/tests/dag_tests.rs`

- [ ] Write failing tests for valid topological start order, reverse stop order, unknown
      dependency, and cycle detection.
- [ ] Run `cargo test -p rspm-core dag_tests -- --nocapture` and confirm the tests fail for missing
      DAG implementation.
- [ ] Implement DAG validation and `Plan { start_order, stop_order }`.
- [ ] Re-run `cargo test -p rspm-core dag_tests -- --nocapture` and confirm the tests pass.

## Task 3: Schedule, Restart, and Events

**Files:**
- Create: `crates/rspm-core/src/schedule.rs`
- Create: `crates/rspm-core/src/event.rs`
- Test: `crates/rspm-core/tests/schedule_tests.rs`
- Test: `crates/rspm-core/tests/event_tests.rs`

- [ ] Write failing tests for cron expression parsing, schedule windows, restart policy parsing,
      and event JSON serialization.
- [ ] Run the targeted core tests and confirm they fail for missing schedule/event modules.
- [ ] Implement schedule models, restart policy models, and event payloads.
- [ ] Re-run the targeted core tests and confirm they pass.

## Task 4: Runtime Supervisor

**Files:**
- Create: `crates/rspm-daemon/Cargo.toml`
- Create: `crates/rspm-daemon/src/lib.rs`
- Create: `crates/rspm-daemon/src/runtime.rs`
- Create: `crates/rspm-daemon/src/supervisor.rs`
- Create: `crates/rspm-daemon/src/logs.rs`
- Test: `crates/rspm-daemon/tests/supervisor_tests.rs`

- [ ] Write failing tests that spawn short-lived commands, capture stdout/stderr logs, stop a long
      running task, and report `TaskInfo`.
- [ ] Run `cargo test -p rspm-daemon supervisor_tests -- --nocapture` and confirm failures.
- [ ] Implement process spawn, stop, restart, status, log file capture, and event recording.
- [ ] Re-run daemon supervisor tests and confirm they pass.

## Task 5: Health Checks and Dependency Startup

**Files:**
- Create: `crates/rspm-daemon/src/health.rs`
- Create: `crates/rspm-daemon/src/orchestrator.rs`
- Test: `crates/rspm-daemon/tests/orchestrator_tests.rs`

- [ ] Write failing tests for command health success/failure and dependency startup order.
- [ ] Run targeted daemon tests and confirm failures.
- [ ] Implement command/file/tcp/http health checks and orchestrated start/stop order.
- [ ] Re-run targeted daemon tests and confirm they pass.

## Task 6: Local API and Rust SDK

**Files:**
- Create: `crates/rspm-sdk/Cargo.toml`
- Create: `crates/rspm-sdk/src/lib.rs`
- Create: `crates/rspm-sdk/src/api.rs`
- Create: `crates/rspm-daemon/src/api.rs`
- Test: `crates/rspm-sdk/tests/api_tests.rs`

- [ ] Write failing tests for request/response serialization and in-process client operations.
- [ ] Run `cargo test -p rspm-sdk api_tests -- --nocapture` and confirm failures.
- [ ] Implement JSON-RPC-like request/response types and an in-process client abstraction.
- [ ] Re-run SDK tests and confirm they pass.

## Task 7: CLI

**Files:**
- Create: `crates/rspm/Cargo.toml`
- Create: `crates/rspm/src/main.rs`
- Create: `crates/rspm/tests/cli_tests.rs`

- [ ] Write failing CLI tests for `validate`, `graph`, and table status formatting.
- [ ] Run `cargo test -p rspm cli_tests -- --nocapture` and confirm failures.
- [ ] Implement CLI commands with `clap` and table output.
- [ ] Re-run CLI tests and confirm they pass.

## Task 8: Python SDK

**Files:**
- Create: `python/pyproject.toml`
- Create: `python/rspm/__init__.py`
- Create: `python/rspm/client.py`
- Create: `python/rspm/aio.py`
- Test: `python/tests/test_client.py`

- [ ] Write failing Python tests for sync client request payloads and async client context manager.
- [ ] Run `uv run pytest python/tests -q` and confirm failures.
- [ ] Implement sync and async Python SDK.
- [ ] Re-run Python tests and confirm they pass.

## Task 9: Integration, Docs, and Completion Audit

**Files:**
- Create: `examples/rspm.toml`
- Create: `README.md`
- Modify: `docs/design.md`

- [ ] Add an example config covering DAG, health, schedule, cron, and restart policies.
- [ ] Add README usage for CLI, Rust SDK, and Python SDK.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo test --workspace`.
- [ ] Run `uv run pytest python/tests -q`.
- [ ] Build a prompt-to-artifact completion audit against `docs/design.md`.
