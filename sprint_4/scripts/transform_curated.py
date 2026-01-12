import pandas as pd


def transform_curated(df: pd.DataFrame):
    """
    Pure curated-level validation.
    No file system access.
    """

    df = df.copy()

    # -----------------------------
    # VALIDATIONS
    # -----------------------------
    valid_age = (df["age_numeric"] >= 18) & (df["age_numeric"] <= 100)

    valid_amount = pd.to_numeric(
        df["TotalAmount"], errors="coerce"
    ).notna() & (df["TotalAmount"] > 0)

    valid_email = df["Email"].astype(str).str.contains("@", regex=False)

    valid_rows = valid_age & valid_amount & valid_email

    curated_valid = df[valid_rows].copy()
    curated_rejected = df[~valid_rows].copy()

    curated_rejected["reject_reason"] = "Failed curated validation"

    return curated_valid, curated_rejected
