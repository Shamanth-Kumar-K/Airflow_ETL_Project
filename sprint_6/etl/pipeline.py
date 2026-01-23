from sprint_6.etl.extract import extract_data
from sprint_6.etl.transform import transform_data
from sprint_6.etl.load import load_data


def run_pipeline(input_path: str, output_path: str):
    customers, products, sales = extract_data(input_path)
    customers, products, sales_enriched = transform_data(
        customers, products, sales
    )
    load_data(customers, products, sales_enriched, output_path)
