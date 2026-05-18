"""One-shot rspm task that prints a message and exits successfully."""

from __future__ import annotations

import argparse

from common import emit


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the one-shot task.

    :returns: Parsed arguments.
    :rtype: argparse.Namespace
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", default="oneshot-message")
    parser.add_argument("--message", default="hello from rspm one-shot task")
    return parser.parse_args()


def main() -> None:
    """Print a single structured message and exit."""

    args = parse_args()
    emit("oneshot_message", task=args.name, message=args.message)


if __name__ == "__main__":
    main()
