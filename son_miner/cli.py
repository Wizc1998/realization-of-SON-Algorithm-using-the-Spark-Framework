"""Command-line interface for the distributed miner."""

from __future__ import annotations

import argparse
from pathlib import Path

from .io import write_result
from .spark_job import create_context, mine


def main() -> None:
    parser = argparse.ArgumentParser(description="Mine frequent itemsets with SON and PySpark")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--support", type=int, required=True)
    parser.add_argument("--orientation", choices=("left-baskets", "right-baskets"), default="left-baskets")
    parser.add_argument("--min-basket-size", type=int, default=1)
    parser.add_argument("--master", default=None)
    args = parser.parse_args()

    context = create_context(args.master)
    try:
        candidates, frequent = mine(
            context,
            args.input,
            args.support,
            args.orientation,
            args.min_basket_size,
        )
        write_result(args.output, candidates, frequent)
    finally:
        context.stop()


if __name__ == "__main__":
    main()

