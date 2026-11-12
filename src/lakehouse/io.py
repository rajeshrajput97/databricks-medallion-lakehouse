"""Read and write Delta-style table directories of Parquet files."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd


def write_parquet_table(frame: pd.DataFrame, table_path: Path) -> Path:
    if table_path.exists():
        shutil.rmtree(table_path)
    table_path.mkdir(parents=True, exist_ok=True)
    output = table_path / "part-0000.parquet"
    frame.to_parquet(output, index=False)
    return output


def read_parquet_table(table_path: Path) -> pd.DataFrame:
    files = sorted(table_path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {table_path}")
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
