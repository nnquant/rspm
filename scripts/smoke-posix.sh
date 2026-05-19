#!/usr/bin/env sh
set -eu

ADDR="${RSPM_SMOKE_ADDR:-127.0.0.1:27792}"
ROOT="${RSPM_SMOKE_ROOT:-/tmp/rspm-smoke}"
LOG_DIR="$ROOT/logs"
STATE_DIR="$ROOT/state"
SOCKET_PATH="$ROOT/run/rspmd.sock"
CONFIG_PATH="$ROOT/tasks.rspm.toml"
PYTHON_BIN="${RSPM_SMOKE_PYTHON:-}"

run_rspm() {
  cargo run -p rspm -- \
    --addr "$ADDR" \
    --log-dir "$LOG_DIR" \
    --state-dir "$STATE_DIR" \
    --socket-path "$SOCKET_PATH" \
    "$@"
}

assert_contains() {
  haystack="$1"
  needle="$2"
  label="$3"
  if ! printf '%s' "$haystack" | grep -F "$needle" >/dev/null 2>&1; then
    echo "missing expected output [$needle] while checking [$label]" >&2
    exit 1
  fi
}

cleanup() {
  run_rspm daemon stop >/dev/null 2>&1 || true
}

trap cleanup EXIT

if [ -z "$PYTHON_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "python3 or python is required for examples/tasks.rspm.toml" >&2
    exit 1
  fi
fi

mkdir -p "$LOG_DIR" "$STATE_DIR" "$(dirname "$SOCKET_PATH")"
PYTHON_TOML="$(printf '%s' "$PYTHON_BIN" | sed 's/\\/\\\\/g; s/"/\\"/g')"
awk -v py="$PYTHON_TOML" '
  $0 == "cmd = \"python3\"" { print "cmd = \"" py "\""; next }
  { print }
' examples/tasks.rspm.toml > "$CONFIG_PATH"

APPLY_OUTPUT="$(run_rspm apply -f "$CONFIG_PATH")"
printf '%s\n' "$APPLY_OUTPUT"
assert_contains "$APPLY_OUTPUT" "applied [rspm-simulated-tasks] tasks=4" "apply"
assert_contains "$APPLY_OUTPUT" "task_id=2 market_feed" "apply"

DOCTOR_OUTPUT="$(run_rspm doctor --config "$CONFIG_PATH" --log-dir "$LOG_DIR")"
printf '%s\n' "$DOCTOR_OUTPUT"
assert_contains "$DOCTOR_OUTPUT" "daemon: ok" "doctor"
assert_contains "$DOCTOR_OUTPUT" "platform:" "doctor"
assert_contains "$DOCTOR_OUTPUT" "default_addr:" "doctor"
assert_contains "$DOCTOR_OUTPUT" "tasks: 4" "doctor"

SERVICE_STATUS_OUTPUT="$(run_rspm service status --dry-run)"
printf '%s\n' "$SERVICE_STATUS_OUTPUT"
assert_contains "$SERVICE_STATUS_OUTPUT" "status command:" "service status dry-run"

SERVICE_START_OUTPUT="$(run_rspm service start --dry-run)"
printf '%s\n' "$SERVICE_START_OUTPUT"
assert_contains "$SERVICE_START_OUTPUT" "start command:" "service start dry-run"

SERVICE_RESTART_OUTPUT="$(run_rspm service restart --dry-run)"
printf '%s\n' "$SERVICE_RESTART_OUTPUT"
assert_contains "$SERVICE_RESTART_OUTPUT" "restart command:" "service restart dry-run"

LS_OUTPUT="$(run_rspm ls)"
printf '%s\n' "$LS_OUTPUT"
assert_contains "$LS_OUTPUT" "TASK_ID" "ls header"
assert_contains "$LS_OUTPUT" "START_TIME" "ls header"
assert_contains "$LS_OUTPUT" "STOP_TIME" "ls header"
assert_contains "$LS_OUTPUT" "market_feed" "ls task"

START_OUTPUT="$(run_rspm start 1 3)"
printf '%s\n' "$START_OUTPUT"
assert_contains "$START_OUTPUT" "task_id=1 long_watcher" "start"
assert_contains "$START_OUTPUT" "task_id=3 oneshot_message" "start"
assert_contains "$START_OUTPUT" "TASK_ID" "post-start table"

LOG_OUTPUT="$(run_rspm log all --no-follow --lines 20 --merge)"
printf '%s\n' "$LOG_OUTPUT"
assert_contains "$LOG_OUTPUT" "long_watcher |" "aggregate log prefix"
assert_contains "$LOG_OUTPUT" "oneshot_message |" "aggregate log prefix"

STOP_OUTPUT="$(run_rspm stop all)"
printf '%s\n' "$STOP_OUTPUT"
assert_contains "$STOP_OUTPUT" "task_id=1 long_watcher stopped" "stop"
assert_contains "$STOP_OUTPUT" "task_id=2 market_feed stopped" "stop"
assert_contains "$STOP_OUTPUT" "TASK_ID" "post-stop table"

run_rspm daemon stop
trap - EXIT
