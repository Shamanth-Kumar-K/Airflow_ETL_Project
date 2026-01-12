import pandas as pd


def transform_staging(df: pd.DataFrame):
    """
    Pure staging transformation.
    NO file system access.
    NO folder creation.
    NO side effects.
    """

    df = df.copy()

    # ------------------------------
    # AGE CLEANING + CONVERSION
    # ------------------------------
    df["age_cleaned"] = (
        df["Age"]
        .astype(str)
        .str.replace('"', '', regex=False)
        .str.strip()
    )

    df["age_numeric"] = pd.to_numeric(
        df["age_cleaned"],
        errors="coerce"
    )

    # ------------------------------
    # VALIDATION
    # ------------------------------
    valid_age = df["age_numeric"].notna() & (df["age_numeric"] > 0)

    staging_valid = df[valid_age].copy()
    staging_rejected = df[~valid_age].copy()

    staging_rejected["reject_reason"] = "Invalid age format"

    return staging_valid, staging_rejected
