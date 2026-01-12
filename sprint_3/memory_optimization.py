import pandas as pd

# ----------------------------------
# File path
# ----------------------------------
DATA_PATH = "/opt/airflow/sprint_3/Data/raw/ecommerce_amazon.csv"


def show_memory_usage(df, stage):
    """
    Prints memory usage of dataframe
    """
    print(f"\nMemory usage {stage}:")
    print(df.info(memory_usage="deep"))


def optimize_memory(df):
    """
    Optimizes memory usage and standardizes data by:
    - Cleaning Age column (string → integer)
    - Normalizing phone numbers (remove hyphens)
    - Down-casting numeric columns
    - Converting repeated object columns to category
    """

    # ---------------------------
    # AGE: string → integer
    # ---------------------------
    if "Age" in df.columns:
        df["Age"] = (
            pd.to_numeric(df["Age"], errors="coerce")
              .astype("Int8")
        )

    # ---------------------------
    # PHONE NUMBER: remove hyphens
    # ---------------------------
    if "Phone" in df.columns:
        df["Phone"] = (
            df["Phone"]
            .astype(str)
            .str.replace("-", "", regex=False)
        )

    # ---------------------------
    # Downcast numeric columns
    # ---------------------------
    for col in df.select_dtypes(include=["int64", "float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="unsigned")

    # ---------------------------
    # Convert object columns → category
    # ---------------------------
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("category")

    return df


def run_memory_optimization():
    print("Starting memory optimization task...")

    df = pd.read_csv(DATA_PATH)

    # Memory before optimization
    show_memory_usage(df, "BEFORE optimization")

    # Optimize memory + standardize data
    df = optimize_memory(df)

    # Memory after optimization
    show_memory_usage(df, "AFTER optimization")

    print("Memory optimization task completed.")

    # quick sanity check
    print(df[["Age", "Phone"]].head())
