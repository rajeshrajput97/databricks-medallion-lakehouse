"""Pandas transforms shared by the pipeline and by quality tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakehouse.quality import ALLOWED_STATUSES

REGION_ALIASES = {
    "north": "North",
    "south": "South",
    "east": "East",
    "west": "West",
}


def add_bronze_metadata(frame: pd.DataFrame, *, source_file: str, ingested_at: pd.Timestamp) -> pd.DataFrame:
    bronze = frame.copy()
    bronze["_source_file"] = Path(source_file).name
    bronze["_ingested_at"] = ingested_at
    return bronze


def to_silver(bronze: pd.DataFrame) -> pd.DataFrame:
    frame = bronze.copy()
    frame["transaction_id"] = _blank_to_na(frame["transaction_id"])
    frame["event_ts"] = pd.to_datetime(frame["event_ts"], utc=True, errors="coerce")
    frame["quantity"] = pd.to_numeric(frame["quantity"], errors="coerce")
    frame["unit_price"] = pd.to_numeric(frame["unit_price"], errors="coerce")
    frame["status"] = _blank_to_na(frame["status"]).str.lower()
    frame["region"] = _blank_to_na(frame["region"]).map(_normalize_region)
    frame["product_category"] = _blank_to_na(frame["product_category"])

    frame = frame.dropna(subset=["transaction_id", "event_ts", "quantity", "unit_price", "status", "region"])
    frame = frame[(frame["quantity"] > 0) & (frame["unit_price"] > 0)]
    frame = frame[frame["status"].isin(ALLOWED_STATUSES)]

    frame = frame.sort_values(["transaction_id", "event_ts"]).drop_duplicates("transaction_id", keep="last")
    frame["amount"] = (frame["quantity"] * frame["unit_price"]).round(2)
    return frame.reset_index(drop=True)


def daily_revenue_by_region(silver: pd.DataFrame) -> pd.DataFrame:
    completed = _completed(silver)
    if completed.empty:
        return pd.DataFrame(columns=["event_date", "region", "gross_revenue", "order_count", "units_sold"])

    working = completed.copy()
    working["event_date"] = working["event_ts"].dt.tz_convert("UTC").dt.date.astype(str)
    gold = (
        working.groupby(["event_date", "region"], as_index=False)
        .agg(
            gross_revenue=("amount", "sum"),
            order_count=("transaction_id", "count"),
            units_sold=("quantity", "sum"),
        )
        .sort_values(["event_date", "region"])
        .reset_index(drop=True)
    )
    gold["gross_revenue"] = gold["gross_revenue"].round(2)
    return gold


def category_summary(silver: pd.DataFrame) -> pd.DataFrame:
    completed = _completed(silver)
    if completed.empty:
        return pd.DataFrame(columns=["product_category", "gross_revenue", "order_count", "units_sold"])
    gold = (
        completed.groupby("product_category", as_index=False)
        .agg(
            gross_revenue=("amount", "sum"),
            order_count=("transaction_id", "count"),
            units_sold=("quantity", "sum"),
        )
        .sort_values("product_category")
        .reset_index(drop=True)
    )
    gold["gross_revenue"] = gold["gross_revenue"].round(2)
    return gold


def _completed(silver: pd.DataFrame) -> pd.DataFrame:
    return silver[silver["status"] == "completed"].copy()


def _blank_to_na(series: pd.Series) -> pd.Series:
    return series.replace(r"^\s*$", pd.NA, regex=True).astype("string").str.strip()


def _normalize_region(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "<na>":
        return None
    return REGION_ALIASES.get(text.lower(), text.title())
