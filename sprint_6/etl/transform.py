#this file contains some transformations like data correctness and business rules
import pandas as pd
import pandas as pd

def mask_name(name):
    if pd.isna(name):
        return name
    parts = str(name).split()
    return " ".join([p[0] + "*" * (len(p) - 1) for p in parts])

def mask_zip(zip_code):
    if pd.isna(zip_code):
        return zip_code
    zip_str = str(zip_code)
    return zip_str[:3] + "*" * (len(zip_str) - 3)

def mask_birthday(date):
    if pd.isna(date):
        return date
    try:
        parsed = pd.to_datetime(date, errors="coerce")
        if pd.isna(parsed):
            return None
        return f"{parsed.year}-01-01"
    except Exception:
        return None


def mask_customer_key(key):
    if pd.isna(key):
        return key
    key_str = str(key)
    return "*" * (len(key_str) - 3) + key_str[-3:]


def transform_data(customers, products, sales):
    # ---------------- PRODUCTS ----------------
    products = products.copy()

    # Rename FIRST — before any access
    products = products.rename(columns={
        "Unit Price USD": "UnitPrice",
        "Unit Cost USD": "UnitCost"
    })

    # Now it exists — safe to access
    products["UnitPrice"] = (
        products["UnitPrice"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
        .astype(float)
    )

    # ---------------- SALES ----------------
    sales = sales.copy()

    sales_enriched = sales.merge(
        products[["ProductKey", "UnitPrice"]],
        on="ProductKey",
        how="left"
    )

    sales_enriched["Revenue"] = (
        sales_enriched["Quantity"] * sales_enriched["UnitPrice"]
    )
    # ---- SECURITY MASKING (SPRINT 7) ----
    if "Name" in customers.columns:
        customers["Name"] = customers["Name"].apply(mask_name)

    if "Zip Code" in customers.columns:
        customers["Zip Code"] = customers["Zip Code"].apply(mask_zip)

    if "Birthday" in customers.columns:
        customers["Birthday"] = customers["Birthday"].apply(mask_birthday)

    if "CustomerKey" in customers.columns:
        customers["CustomerKey"] = customers["CustomerKey"].apply(mask_customer_key)

    return customers, products, sales_enriched
