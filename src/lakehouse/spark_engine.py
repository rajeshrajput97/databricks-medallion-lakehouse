"""Optional PySpark execution path for the same Medallion pipeline."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakehouse.io import read_parquet_table, write_parquet_table
from lakehouse.paths import WarehousePaths
from lakehouse.pipeline import PipelineResult
from lakehouse.quality import assert_silver_quality
from lakehouse.transforms import category_summary, daily_revenue_by_region


def run_spark_pipeline(source: Path, paths: WarehousePaths, ingested_at: pd.Timestamp) -> PipelineResult:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = (
        SparkSession.builder.appName("medallion-lakehouse")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    raw = spark.read.option("header", True).csv(str(source))
    bronze = raw.withColumn("_source_file", F.lit(source.name)).withColumn(
        "_ingested_at", F.lit(ingested_at.isoformat())
    )
    bronze_rows = bronze.count()
    _overwrite_parquet(bronze, paths.bronze_transactions)

    prepared = (
        bronze.withColumn("transaction_id", F.nullif(F.trim(F.col("transaction_id")), F.lit("")))
        .withColumn("event_ts", F.to_timestamp(F.col("event_ts")))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn(
            "region",
            F.initcap(
                F.coalesce(
                    F.element_at(
                        F.create_map(
                            F.lit("north"),
                            F.lit("North"),
                            F.lit("south"),
                            F.lit("South"),
                            F.lit("east"),
                            F.lit("East"),
                            F.lit("west"),
                            F.lit("West"),
                        ),
                        F.lower(F.trim(F.col("region"))),
                    ),
                    F.trim(F.col("region")),
                )
            ),
        )
        .where(F.col("transaction_id").isNotNull())
        .where(F.col("event_ts").isNotNull())
        .where(F.col("quantity") > 0)
        .where(F.col("unit_price") > 0)
        .where(F.col("status").isin("completed", "pending", "cancelled"))
        .where(F.col("region").isNotNull())
    )
    latest = Window.partitionBy("transaction_id").orderBy(F.col("event_ts").desc())
    silver = (
        prepared.withColumn("_rn", F.row_number().over(latest))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )
    _overwrite_parquet(silver, paths.silver_transactions)

    silver_pd = read_parquet_table(paths.silver_transactions)
    silver_pd["event_ts"] = pd.to_datetime(silver_pd["event_ts"], utc=True)
    quality = assert_silver_quality(silver_pd)

    gold_daily = daily_revenue_by_region(silver_pd)
    gold_category = category_summary(silver_pd)
    write_parquet_table(gold_daily, paths.gold_daily_revenue)
    write_parquet_table(gold_category, paths.gold_category_summary)

    return PipelineResult(
        engine="spark",
        bronze_rows=bronze_rows,
        silver_rows=len(silver_pd),
        gold_daily_rows=len(gold_daily),
        gold_category_rows=len(gold_category),
        quality=quality,
        warehouse=paths,
    )


def _overwrite_parquet(frame, table_path: Path) -> None:
    import shutil

    if table_path.exists():
        shutil.rmtree(table_path)
    table_path.parent.mkdir(parents=True, exist_ok=True)
    frame.write.mode("overwrite").parquet(str(table_path))
