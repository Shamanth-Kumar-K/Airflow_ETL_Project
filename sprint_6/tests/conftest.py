import pytest
import pandas as pd
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "sprint_6" / "data" / "curated"


@pytest.fixture(scope="session")
def customers_df():
    """
    Loads cleaned customers data once per test session.
    Fails fast if file is missing.
    """
    path = DATA_DIR / "customers_clean.csv"
    assert path.exists(), f"Missing file: {path}"
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def products_df():
    """
    Loads cleaned products data once per test session.
    """
    path = DATA_DIR / "products_clean.csv"
    assert path.exists(), f"Missing file: {path}"
    return pd.read_csv(path)


@pytest.fixture(scope="session")
def sales_df():
    """
    Loads enriched sales data once per test session.
    """
    path = DATA_DIR / "sales_enriched.csv.csv"
    assert path.exists(), f"Missing file: {path}"
    return pd.read_csv(path)
