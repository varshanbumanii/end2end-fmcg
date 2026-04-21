import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# --------------------------
# CONNECT TO RDS
# --------------------------
conn = psycopg2.connect(
    host="fmcg-db.cf424qa4c1mr.ap-southeast-2.rds.amazonaws.com",
    database="postgres",
    user="postgres",
    password="Postgres123"
)

cursor = conn.cursor()

# --------------------------
# BULK INSERT FUNCTION
# --------------------------
def insert_data(file, table, conflict_column=None):
    df = pd.read_csv(file)

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ",".join(df.columns)

    query = f"INSERT INTO {table} ({cols}) VALUES %s"

    # 🔥 HANDLE DUPLICATES
    if conflict_column:
        query += f" ON CONFLICT ({conflict_column}) DO NOTHING"

    execute_values(cursor, query, tuples)
    conn.commit()

    print(f"✅ Inserted {len(df)} rows into {table}")

# --------------------------
# LOAD DATA (CORRECT ORDER)
# --------------------------

# PRODUCTS (load once / safe)
insert_data("products.csv", "products", "product_id")

# CUSTOMERS (base + new)
insert_data("customers.csv", "customers", "customer_id")
insert_data("customers_day4.csv", "customers", "customer_id")

# SALES (append batches)
insert_data("sales_day1.csv", "sales_orders", "order_id")
insert_data("sales_day2.csv", "sales_orders", "order_id")
insert_data("sales_day3.csv", "sales_orders", "order_id")
insert_data("sales_day4.csv", "sales_orders", "order_id")

# --------------------------
cursor.close()
conn.close()