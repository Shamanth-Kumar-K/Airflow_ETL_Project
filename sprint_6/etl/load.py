from pathlib import Path

def load_data(customers, products, sales, output_path: str):
    """
    Loads processed data to disk.
    NOTE:
    - Customer data is encrypted (Sprint 7 security requirement)
    - No PII is logged or exposed here
    """

    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 🔐 Secured customer data (encrypted fields)
    customers.to_csv(
        output_dir / "customers_secure.csv",
        index=False
    )

    # Non-sensitive datasets
    products.to_csv(
        output_dir / "products_cleaned.csv",
        index=False
    )

    sales.to_csv(
        output_dir / "sales_enriched.csv",
        index=False
    )
