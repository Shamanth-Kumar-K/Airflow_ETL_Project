import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.transform import transform_data
import pandas as pd
def test_print_products_columns(products_df):
    print("PRODUCT COLUMNS:", products_df.columns.tolist())
    assert True

def test_sales_product_integration():
    products = pd.DataFrame({
        "ProductKey": [1],
        "UnitPrice": [100.0]
    })

    sales = pd.DataFrame({
        "Order Number": [1001],
        "Line Item": [1],
        "ProductKey": [1],
        "Quantity": [2]
    })

    customers = pd.DataFrame({
        "CustomerKey": [101]
    })

    _, _, sales_enriched = transform_data(customers, products, sales)

    assert "Revenue" in sales_enriched.columns
    assert sales_enriched["Revenue"].iloc[0] == 200.0
