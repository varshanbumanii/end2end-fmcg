import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --------------------------
# CONNECT
# --------------------------
conn = psycopg2.connect(
    host="localhost",
    database="fmcg_db",
    user="postgres",
    password="pgadmin"
)

cursor = conn.cursor()

# --------------------------
# BULK INSERT FUNCTION
# --------------------------
def insert_data(file, table, conflict_column=None):
    df = pd.read_csv(file)

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    cols = ",".join(columns)

    query = f"""
        INSERT INTO {table} ({cols})
        VALUES %s
    """

    # Handle duplicates
    if conflict_column:
        query += f" ON CONFLICT ({conflict_column}) DO NOTHING"

    execute_values(cursor, query, values)
    conn.commit()

    print(f"✅ Inserted {len(df)} rows into {table}")


# --------------------------
# LOAD MASTER DATA
# --------------------------
insert_data("products.csv", "products", "product_id")

# Load base customers only once
insert_data("customers.csv", "customers", "customer_id")

# Load new customers (day4)
insert_data("customers_day4.csv", "customers", "customer_id")

# --------------------------
# LOAD SALES DATA (APPEND)
# --------------------------
insert_data("sales_day1.csv", "sales_orders", "order_id")
insert_data("sales_day2.csv", "sales_orders", "order_id")
insert_data("sales_day3.csv", "sales_orders", "order_id")
insert_data("sales_day4.csv", "sales_orders", "order_id")

# --------------------------
cursor.close()
conn.close()