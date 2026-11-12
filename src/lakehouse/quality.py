"""Data quality checks applied to the Silver layer."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

ALLOWED_STATUSES = frozenset({"completed", "pending", "cancelled"})


@dataclass(frozen=True)
class QualityIssue:
    check: str
    message: str
    count: int


@dataclass(frozen=True)
class QualityReport:
    row_count: int
    issues: list[QualityIssue]

    @property
    def passed(self) -> bool:
        return not self.issues


class QualityError(Exception):
    def __init__(self, report: QualityReport) -> None:
        self.report = report
        details = "; ".join(f"{item.check}: {item.message}" for item in report.issues)
        super().__init__(f"Silver quality checks failed ({details})")


def evaluate_silver(frame: pd.DataFrame) -> QualityReport:
    issues: list[QualityIssue] = []
    row_count = int(len(frame))

    def _add(check: str, mask, message: str) -> None:
        count = int(mask.sum()) if len(frame) else 0
        if count:
            issues.append(QualityIssue(check=check, message=message, count=count))

    if row_count == 0:
        issues.append(QualityIssue("not_empty", "Silver layer has no rows", 0))
        return QualityReport(row_count=0, issues=issues)

    _add("transaction_id_present", frame["transaction_id"].isna() | (frame["transaction_id"].astype(str).str.len() == 0), "transaction_id is required")
    _add("event_ts_present", frame["event_ts"].isna(), "event_ts is required")
    _add("transaction_id_unique", frame["transaction_id"].duplicated(), "transaction_id must be unique in Silver")
    _add("quantity_positive", frame["quantity"] <= 0, "quantity must be greater than 0")
    _add("unit_price_positive", frame["unit_price"] <= 0, "unit_price must be greater than 0")
    _add("status_allowed", ~frame["status"].isin(ALLOWED_STATUSES), "status must be completed, pending, or cancelled")
    _add("amount_consistent", (frame["amount"] - frame["quantity"] * frame["unit_price"]).abs() > 1e-6, "amount must equal quantity * unit_price")

    return QualityReport(row_count=row_count, issues=issues)


def assert_silver_quality(frame: pd.DataFrame) -> QualityReport:
    report = evaluate_silver(frame)
    if not report.passed:
        raise QualityError(report)
    return report
