"""Long-running rspm task that prints data and sometimes exits with failure."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import random
import time

from common import emit, exit_with_error, install_signal_handlers, remove_ready_file, touch_ready_file


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the long-running simulation task.

    :returns: Parsed arguments.
    :rtype: argparse.Namespace
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="long-watcher")
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--ready-file", default=".rspm/example-state/long-watcher.ready")
    parser.add_argument("--watch-file", default=os.getenv("RSPM_WATCH_FILE"))
    return parser.parse_args()


def read_watch_file(path: str | None) -> dict[str, object]:
    """
    Read basic metadata from the configured watch file.

    :param path: Watched file path.
    :type path: str | None
    :returns: Watch file metadata.
    :rtype: dict[str, object]
    """

    if path is None:
        return {"watch_file": None}
    watch_path = Path(path)
    if not watch_path.exists():
        return {"watch_file": path, "exists": False}
    text = watch_path.read_text(encoding="utf-8").strip()
    stat = watch_path.stat()
    return {
        "watch_file": path,
        "exists": True,
        "size": stat.st_size,
        "value": text,
    }


def main() -> None:
    """Run the long-running simulated worker."""

    args = parse_args()
    fail_probability = float(os.getenv("RSPM_FAIL_PROBABILITY", "0.08"))
    seed = os.getenv("RSPM_RANDOM_SEED")
    random_source = random.Random(seed) if seed is not None else random.Random()
    shutdown = install_signal_handlers()
    tick = 0

    touch_ready_file(args.ready_file, args.name)
    emit(
        "task_started",
        task=args.name,
        interval_seconds=args.interval_seconds,
        fail_probability=fail_probability,
        random_seed=seed,
        **read_watch_file(args.watch_file),
    )

    try:
        while not shutdown.requested:
            tick += 1
            emit(
                "task_tick",
                task=args.name,
                tick=tick,
                simulated_value=round(random_source.uniform(100, 200), 4),
                **read_watch_file(args.watch_file),
            )
            if random_source.random() < fail_probability:
                exit_with_error(f"simulated failure task=[{args.name}] tick=[{tick}]")
            time.sleep(args.interval_seconds)
    finally:
        remove_ready_file(args.ready_file)
        emit("task_stopped", task=args.name, tick=tick)


if __name__ == "__main__":
    main()
