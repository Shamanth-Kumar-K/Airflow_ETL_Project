import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import pandas as pd
from etl.transform import transform_data


def test_revenue_regression():
    products = pd.DataFrame({
        "ProductKey": [1],
        "UnitPrice": [50.0]
    })

    sales = pd.DataFrame({
        "SalesKey": [1],
        "ProductKey": [1],
        "Quantity": [4]
    })

    customers = pd.DataFrame({
        "CustomerKey": [1]
    })

    _, _, sales_enriched = transform_data(customers, products, sales)

    assert sales_enriched["Revenue"].iloc[0] == 200.0
