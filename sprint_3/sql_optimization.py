import pandas as pd
import sqlite3
# ----------------------------------
# File paths
# ----------------------------------
DATA_PATH = "/opt/airflow/sprint_3/Data/raw/ecommerce_amazon.csv"
DB_PATH = "/opt/airflow/sprint_3/ecommerce.db"



def load_data():
    """
    Load ecommerce data into DataFrame
    """
    return pd.read_csv(DATA_PATH)


def load_to_sqlite(df):
    """
    Load DataFrame into SQLite database
    """
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("orders", conn, if_exists="replace", index=False)
    return conn


def create_indexes(conn):
    """
    Create indexes to optimize query performance
    """
    cursor = conn.cursor()

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_category
        ON orders(Category);
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_orderid
        ON orders(OrderID);
    """)

    conn.commit()


def run_optimized_query(conn):
    """
    Optimized SQL query for analytics
    """
    query = """
    SELECT
        Category,
        COUNT(OrderID) AS TotalOrders,
        SUM(TotalAmount) AS TotalSales
    FROM orders
    GROUP BY Category
    ORDER BY TotalSales DESC;
    """

    result = pd.read_sql_query(query, conn)
    print(result)


def run_sql_optimization():
    print("Starting SQL optimization task...")

    df = load_data()

    with sqlite3.connect(DB_PATH) as conn:
        load_to_sqlite(df)
        create_indexes(conn)
        run_optimized_query(conn)

    print("SQL optimization completed successfully!")
