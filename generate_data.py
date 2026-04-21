import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

NUM_RECORDS = 10000

regions = ['North', 'South', 'East', 'West']
channels = ['Primary', 'Secondary']
categories = ['Beverages', 'Snacks', 'Dairy', 'Personal Care']

# --------------------------
# Generate Customers
# --------------------------
def generate_customers(n):
    data = []
    for i in range(n):
        data.append({
            "customer_id": i + 1,
            "name": fake.name(),
            "region": random.choice(regions),
            "channel": random.choice(channels)
        })
    return pd.DataFrame(data)

# --------------------------
# Generate Products
# --------------------------
def generate_products(n):
    data = []
    for i in range(n):
        price = round(random.uniform(10, 500), 2)
        data.append({
            "product_id": i + 1,
            "product_name": fake.word().capitalize(),
            "category": random.choice(categories),
            "price": price
        })
    return pd.DataFrame(data)

# --------------------------
# Generate Sales Orders
# --------------------------
def generate_sales_orders(n, customers, products, day):
    data = []
    base_date = datetime(2025, 1, 1) + timedelta(days=day)

    for i in range(n):
        customer = customers.sample(1).iloc[0]
        product = products.sample(1).iloc[0]

        quantity = random.randint(1, 20)

        data.append({
            "order_id": f"{day}-{i}",
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "order_date": base_date,
            "quantity": quantity,
            "price": product["price"],
            "updated_at": datetime.now()
        })

    return pd.DataFrame(data)

# --------------------------
# Generate Data for 3 Days
# --------------------------
customers = generate_customers(NUM_RECORDS)
products = generate_products(NUM_RECORDS)

sales_day1 = generate_sales_orders(NUM_RECORDS, customers, products, 1)
sales_day2 = generate_sales_orders(NUM_RECORDS, customers, products, 2)
sales_day3 = generate_sales_orders(NUM_RECORDS, customers, products, 3)

# Save locally
customers.to_csv("customers.csv", index=False)
products.to_csv("products.csv", index=False)

sales_day1.to_csv("sales_day1.csv", index=False)
sales_day2.to_csv("sales_day2.csv", index=False)
sales_day3.to_csv("sales_day3.csv", index=False)

print("✅ Data Generated Successfully!")