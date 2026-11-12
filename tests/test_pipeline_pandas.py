from pathlib import Path

import pandas as pd

from lakehouse.io import read_parquet_table
from lakehouse.pipeline import run_pipeline


def test_sample_pipeline_writes_all_layers(sample_csv: Path, warehouse: Path):
    result = run_pipeline(sample_csv, warehouse, engine="pandas")
    assert result.engine == "pandas"
    assert result.quality.passed
    assert result.bronze_rows == 30
    assert result.silver_rows == 23
    assert result.gold_daily_rows > 0
    assert result.gold_category_rows == 4

    bronze = read_parquet_table(result.warehouse.bronze_transactions)
    silver = read_parquet_table(result.warehouse.silver_transactions)
    gold_daily = read_parquet_table(result.warehouse.gold_daily_revenue)
    gold_category = read_parquet_table(result.warehouse.gold_category_summary)

    assert "_source_file" in bronze.columns
    assert "_ingested_at" in bronze.columns
    assert bronze["_source_file"].nunique() == 1
    assert "amount" in silver.columns
    assert silver["transaction_id"].is_unique
    assert set(silver["region"].unique()) <= {"North", "South", "East", "West"}
    assert set(silver["status"].unique()) <= {"completed", "pending", "cancelled"}

    # Dirty and duplicate input rows are not in Silver
    assert "TX-1021" not in set(silver["transaction_id"])
    assert "TX-1022" not in set(silver["transaction_id"])
    assert "TX-1023" not in set(silver["transaction_id"])
    assert "TX-1024" not in set(silver["transaction_id"])
    assert (silver["transaction_id"] == "TX-1008").sum() == 1

    # Region aliases are normalised
    north_ids = set(silver.loc[silver["region"] == "North", "transaction_id"])
    assert {"TX-1002", "TX-1019"}.issubset(north_ids)

    # Gold uses completed orders only and totals a known day/region
    jan3_north = gold_daily[
        (gold_daily["event_date"].astype(str) == "2026-01-03") & (gold_daily["region"] == "North")
    ]
    assert len(jan3_north) == 1
    # TX-1001 (2*499) + TX-1002 (1299.50) = 2297.50; TX-1003 is pending
    assert float(jan3_north.iloc[0]["gross_revenue"]) == 2297.50
    assert int(jan3_north.iloc[0]["order_count"]) == 2

    assert set(gold_category["product_category"]) == {"Apparel", "Electronics", "Grocery", "Home"}
    assert (gold_category["gross_revenue"] > 0).all()


def test_unknown_engine_raises(sample_csv: Path, warehouse: Path):
    import pytest

    with pytest.raises(ValueError):
        run_pipeline(sample_csv, warehouse, engine="snowflake")
