"""Scheduled market-data style task that runs until rspm stops it."""

from __future__ import annotations

import argparse
import random
import time

from common import emit, install_signal_handlers, remove_ready_file, touch_ready_file


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the market feed simulation.

    :returns: Parsed arguments.
    :rtype: argparse.Namespace
    """

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", default="sim-ctp")
    parser.add_argument("--interval-seconds", type=float, default=3.0)
    parser.add_argument("--ready-file", default=".rspm/example-state/market-feed.ready")
    parser.add_argument("--seed", default="20260518")
    return parser.parse_args()


def main() -> None:
    """Run a simulated market-data feed until SIGTERM/SIGINT arrives."""

    args = parse_args()
    random_source = random.Random(args.seed)
    shutdown = install_signal_handlers()
    sequence = 0

    touch_ready_file(args.ready_file, args.market)
    emit(
        "market_feed_started",
        market=args.market,
        interval_seconds=args.interval_seconds,
        random_seed=args.seed,
    )

    try:
        while not shutdown.requested:
            sequence += 1
            emit(
                "market_tick",
                market=args.market,
                sequence=sequence,
                instrument=random_source.choice(["IF2606", "IH2606", "au2606"]),
                price=round(random_source.uniform(3_000, 6_000), 2),
                volume=random_source.randint(1, 20),
            )
            time.sleep(args.interval_seconds)
    finally:
        remove_ready_file(args.ready_file)
        emit("market_feed_stopped", market=args.market, sequence=sequence)


if __name__ == "__main__":
    main()
