from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from lakehouse.io import read_parquet_table
from lakehouse.pipeline import run_pipeline


def test_spark_engine_matches_pandas_silver_ids(sample_csv: Path, warehouse: Path, tmp_path: Path):
    pandas_result = run_pipeline(sample_csv, tmp_path / "pandas_wh", engine="pandas")
    spark_result = run_pipeline(sample_csv, warehouse, engine="spark")

    assert spark_result.engine == "spark"
    assert spark_result.quality.passed
    assert spark_result.bronze_rows == pandas_result.bronze_rows
    assert spark_result.silver_rows == pandas_result.silver_rows

    pandas_silver = read_parquet_table(pandas_result.warehouse.silver_transactions)
    spark_silver = read_parquet_table(spark_result.warehouse.silver_transactions)
    assert set(spark_silver["transaction_id"]) == set(pandas_silver["transaction_id"])
