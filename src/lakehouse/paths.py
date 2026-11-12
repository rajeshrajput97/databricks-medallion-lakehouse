"""Delta-style warehouse layout."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

BRONZE_TRANSACTIONS = "bronze/transactions"
SILVER_TRANSACTIONS = "silver/transactions"
GOLD_DAILY_REVENUE = "gold/daily_revenue_by_region"
GOLD_CATEGORY_SUMMARY = "gold/category_summary"


@dataclass(frozen=True)
class WarehousePaths:
    root: Path

    @property
    def bronze_transactions(self) -> Path:
        return self.root / BRONZE_TRANSACTIONS

    @property
    def silver_transactions(self) -> Path:
        return self.root / SILVER_TRANSACTIONS

    @property
    def gold_daily_revenue(self) -> Path:
        return self.root / GOLD_DAILY_REVENUE

    @property
    def gold_category_summary(self) -> Path:
        return self.root / GOLD_CATEGORY_SUMMARY


def ensure_parent(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
