from pathlib import Path

import pytest

SAMPLE_CSV = Path(__file__).resolve().parents[1] / "data" / "sample_transactions.csv"


@pytest.fixture
def sample_csv() -> Path:
    return SAMPLE_CSV


@pytest.fixture
def warehouse(tmp_path) -> Path:
    return tmp_path / "warehouse"
