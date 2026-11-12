"""CLI entry point for the Medallion pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from lakehouse.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the bronze/silver/gold pipeline.")
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data" / "sample_transactions.csv",
    )
    parser.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    parser.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    args = parser.parse_args()

    result = run_pipeline(args.source, args.warehouse, engine=args.engine)
    print(f"engine           : {result.engine}")
    print(f"bronze rows      : {result.bronze_rows}")
    print(f"silver rows      : {result.silver_rows}")
    print(f"gold daily rows  : {result.gold_daily_rows}")
    print(f"gold category    : {result.gold_category_rows}")
    print(f"quality          : {'passed' if result.quality.passed else 'failed'}")
    print(f"warehouse        : {result.warehouse.root}")


if __name__ == "__main__":
    main()
