#this file will just read the CSV files
import pandas as pd
from pathlib import Path

def extract_data(input_path: str):
    input_dir = Path(input_path)

    customers = pd.read_csv(input_dir / "customers.csv",encoding="latin-1")
    products = pd.read_csv(input_dir / "Products.csv",encoding="latin-1")
    sales = pd.read_csv(input_dir / "Sales.csv",encoding="latin-1")

    return customers, products, sales

