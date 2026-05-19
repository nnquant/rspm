# rspm examples

This directory contains simulated tasks for local rspm validation.

## Files

- `tasks.rspm.toml`: end-to-end task orchestration config.
- `tasks/long_running_task.py`: long-running task. It prints JSON ticks, writes a ready file,
  watches `tasks/watch_trigger.txt`, and can exit with simulated failures.
- `tasks/oneshot_task.py`: one-shot task. It prints one message and exits.
- `tasks/periodic_task.py`: cron-driven task. It starts periodically, prints a message, and exits.
- `tasks/market_feed_task.py`: scheduled market-feed task. It simulates a business process that
  starts and stops on a trading schedule.

## Smoke Test

Use an explicit address and state directory to avoid interfering with a default local daemon:

```bash
scripts/smoke-posix.sh
```

On Windows PowerShell:

```powershell
.\scripts\smoke-windows.ps1
```

Both scripts create a temporary config under `RSPM_SMOKE_ROOT` and replace `cmd = "python3"` with
the detected Python interpreter. Set `RSPM_SMOKE_PYTHON` when you need a specific interpreter.

The script expands to these steps:

```bash
mkdir -p /tmp/rspm-smoke/logs /tmp/rspm-smoke/state /tmp/rspm-smoke/run

cargo run -p rspm -- \
  --addr 127.0.0.1:27792 \
  --log-dir /tmp/rspm-smoke/logs \
  --state-dir /tmp/rspm-smoke/state \
  --socket-path /tmp/rspm-smoke/run/rspmd.sock \
  apply -f examples/tasks.rspm.toml

cargo run -p rspm -- \
  --addr 127.0.0.1:27792 \
  --log-dir /tmp/rspm-smoke/logs \
  --state-dir /tmp/rspm-smoke/state \
  --socket-path /tmp/rspm-smoke/run/rspmd.sock \
  ls

cargo run -p rspm -- \
  --addr 127.0.0.1:27792 \
  --log-dir /tmp/rspm-smoke/logs \
  --state-dir /tmp/rspm-smoke/state \
  --socket-path /tmp/rspm-smoke/run/rspmd.sock \
  start 1 3

cargo run -p rspm -- \
  --addr 127.0.0.1:27792 \
  --log-dir /tmp/rspm-smoke/logs \
  --state-dir /tmp/rspm-smoke/state \
  --socket-path /tmp/rspm-smoke/run/rspmd.sock \
  log all --no-follow --lines 20 --merge

cargo run -p rspm -- \
  --addr 127.0.0.1:27792 \
  --log-dir /tmp/rspm-smoke/logs \
  --state-dir /tmp/rspm-smoke/state \
  --socket-path /tmp/rspm-smoke/run/rspmd.sock \
  stop all

cargo run -p rspm -- \
  --addr 127.0.0.1:27792 \
  --log-dir /tmp/rspm-smoke/logs \
  --state-dir /tmp/rspm-smoke/state \
  --socket-path /tmp/rspm-smoke/run/rspmd.sock \
  daemon stop
```

Expected checks:

- `apply` starts the daemon automatically.
- `ls` shows `TASK_ID`, `MODE`, `START_TIME`, `STOP_TIME`, `UPTIME`, and `NEXT`.
- `start 1 3` starts multiple tasks by task id and prints a fresh task table.
- `log all --no-follow --merge` prefixes each line with `task_name | ` and merges timestamped
  logs.
- `long_watcher` may fail and restart according to the configured policy.
- Editing `tasks/watch_trigger.txt` triggers the watch restart path for `long_watcher`.
