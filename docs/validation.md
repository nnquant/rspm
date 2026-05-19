# Validation

This document records the validation gates for rspm.

## Local Gates

Run these before committing behavior changes:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo check --workspace --target x86_64-pc-windows-gnu
cd python && uv run python -m pytest -q
```

## Example Smoke

Use `examples/README.md` for a daemon-backed smoke test with:

- automatic daemon startup,
- TOML apply,
- `ls` table output,
- task-id based `start`,
- aggregate logs,
- `stop all`,
- daemon shutdown.

Script entrypoints:

```bash
scripts/smoke-posix.sh
```

```powershell
.\scripts\smoke-windows.ps1
```

## Platform Coverage

| Platform | Gate | Notes |
| --- | --- | --- |
| Linux | local tests, clippy, Python tests, and `scripts/smoke-posix.sh` | Runtime daemon smoke has been exercised locally. |
| macOS | CI Rust check, test-binary compilation, service dry-run CLI tests, and `scripts/smoke-posix.sh` | launchd service activation still needs host-level validation. |
| Windows | CI Rust check, test-binary compilation, service dry-run CLI tests, named-pipe transport tests, and `scripts/smoke-windows.ps1` | scheduled-task service activation still needs host-level validation. |

The GitHub Actions workflow in `.github/workflows/ci.yml` runs Rust format, tests, clippy, and
daemon-backed smoke tests on Linux. It runs Rust workspace compilation, test-binary compilation,
service dry-run CLI tests, named-pipe transport tests, and daemon-backed smoke tests on Windows; it
runs Rust workspace compilation, test-binary compilation, service dry-run CLI tests, and
daemon-backed smoke tests on macOS; and it runs Python SDK tests on Linux.

## Release Artifacts

Tag pushes matching `v*` run `.github/workflows/release.yml`. The workflow builds `rspm` in release
mode on Linux, macOS, and Windows, then uploads the platform binary as a workflow artifact.

Local release-build check:

```bash
cargo build --release -p rspm --locked
target/release/rspm --version
```
