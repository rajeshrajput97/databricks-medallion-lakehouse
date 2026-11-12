"""Run the bronze / silver / gold pipeline on pandas or Spark."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from lakehouse import io as table_io
from lakehouse.paths import WarehousePaths
from lakehouse.quality import QualityReport, assert_silver_quality
from lakehouse.transforms import (
    add_bronze_metadata,
    category_summary,
    daily_revenue_by_region,
    to_silver,
)


@dataclass(frozen=True)
class PipelineResult:
    engine: str
    bronze_rows: int
    silver_rows: int
    gold_daily_rows: int
    gold_category_rows: int
    quality: QualityReport
    warehouse: WarehousePaths


def run_pipeline(
    source_csv: str | Path,
    warehouse_root: str | Path,
    *,
    engine: str = "pandas",
    ingested_at: pd.Timestamp | None = None,
) -> PipelineResult:
    engine_name = engine.lower().strip()
    if engine_name not in {"pandas", "spark"}:
        raise ValueError("engine must be 'pandas' or 'spark'")

    source = Path(source_csv)
    if not source.is_file():
        raise FileNotFoundError(source)

    stamp = ingested_at if ingested_at is not None else pd.Timestamp.now(tz="UTC")
    paths = WarehousePaths(Path(warehouse_root))
    paths.root.mkdir(parents=True, exist_ok=True)

    if engine_name == "spark":
        return _run_spark(source, paths, stamp)
    return _run_pandas(source, paths, stamp)


def _run_pandas(source: Path, paths: WarehousePaths, ingested_at: pd.Timestamp) -> PipelineResult:
    raw = pd.read_csv(source, dtype=str, keep_default_na=False)
    # Re-enable NA for truly empty cells so quality rules can see them
    raw = raw.replace({"": pd.NA})
    bronze = add_bronze_metadata(raw, source_file=source.name, ingested_at=ingested_at)
    table_io.write_parquet_table(bronze, paths.bronze_transactions)

    silver = to_silver(bronze)
    quality = assert_silver_quality(silver)
    table_io.write_parquet_table(silver, paths.silver_transactions)

    gold_daily = daily_revenue_by_region(silver)
    gold_category = category_summary(silver)
    table_io.write_parquet_table(gold_daily, paths.gold_daily_revenue)
    table_io.write_parquet_table(gold_category, paths.gold_category_summary)

    return PipelineResult(
        engine="pandas",
        bronze_rows=len(bronze),
        silver_rows=len(silver),
        gold_daily_rows=len(gold_daily),
        gold_category_rows=len(gold_category),
        quality=quality,
        warehouse=paths,
    )


def _run_spark(source: Path, paths: WarehousePaths, ingested_at: pd.Timestamp) -> PipelineResult:
    from lakehouse.spark_engine import run_spark_pipeline

    return run_spark_pipeline(source, paths, ingested_at)
