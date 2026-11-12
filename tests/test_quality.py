import pandas as pd
import pytest

from lakehouse.quality import QualityError, assert_silver_quality, evaluate_silver


def _row(**overrides):
    base = {
        "transaction_id": "TX-1",
        "event_ts": pd.Timestamp("2026-01-03T09:15:00Z"),
        "region": "North",
        "product_sku": "SKU-A1",
        "product_category": "Electronics",
        "quantity": 2.0,
        "unit_price": 10.0,
        "currency": "INR",
        "status": "completed",
        "customer_id": "CUST-1",
        "amount": 20.0,
    }
    base.update(overrides)
    return base


def test_clean_silver_passes_quality():
    frame = pd.DataFrame([_row(), _row(transaction_id="TX-2", amount=20.0)])
    report = evaluate_silver(frame)
    assert report.passed
    assert report.row_count == 2


def test_duplicate_ids_fail_quality():
    frame = pd.DataFrame([_row(), _row()])
    report = evaluate_silver(frame)
    assert not report.passed
    assert any(issue.check == "transaction_id_unique" for issue in report.issues)
    with pytest.raises(QualityError):
        assert_silver_quality(frame)


def test_non_positive_amounts_and_unknown_status_fail():
    frame = pd.DataFrame(
        [
            _row(transaction_id="TX-1", quantity=0, amount=0),
            _row(transaction_id="TX-2", unit_price=-5, amount=-10),
            _row(transaction_id="TX-3", status="returned", amount=20),
        ]
    )
    checks = {issue.check for issue in evaluate_silver(frame).issues}
    assert "quantity_positive" in checks
    assert "unit_price_positive" in checks
    assert "status_allowed" in checks


def test_empty_silver_fails():
    frame = pd.DataFrame(columns=["transaction_id", "event_ts", "quantity", "unit_price", "status", "amount"])
    report = evaluate_silver(frame)
    assert not report.passed
    assert report.issues[0].check == "not_empty"
