"""Short-lived rspm task intended to be started by a cron action."""

from __future__ import annotations

import argparse
import time

from common import emit


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the periodic task.

    :returns: Parsed arguments.
    :rtype: argparse.Namespace
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="periodic-pulse")
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    return parser.parse_args()


def main() -> None:
    """Emit one pulse event, wait briefly, and exit."""

    args = parse_args()
    emit("periodic_pulse_started", task=args.name)
    time.sleep(args.sleep_seconds)
    emit("periodic_pulse_finished", task=args.name)


if __name__ == "__main__":
    main()
