import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# --------------------------
# CONFIG
# --------------------------
num_orders = 10000

regions = ["South", "North", "West", "East"]
region_weights = [0.8, 0.1, 0.05, 0.05]

channels = ["Online", "Retail"]
channel_weights = [0.7, 0.3]

products = [
    (1, "Coca Cola", "Beverages", 40),
    (2, "Pepsi", "Beverages", 38),
    (3, "Lays Chips", "Snacks", 20),
    (4, "Dairy Milk", "Snacks", 50),
    (5, "Head & Shoulders", "Personal Care", 180),
    (6, "Lux Soap", "Personal Care", 35),
    (7, "Amul Milk", "Dairy", 60),
    (8, "Paneer", "Dairy", 120)
]

product_weights = [0.2, 0.2, 0.15, 0.1, 0.1, 0.1, 0.075, 0.075]

# --------------------------
# GENERATE CUSTOMERS
# --------------------------
customers = []
for i in range(1, 51):
    customers.append({
        "customer_id": i,
        "name": f"Customer_{i}",
        "region": random.choices(regions, region_weights)[0],
        "channel": random.choices(channels, channel_weights)[0]
    })

customers_df = pd.DataFrame(customers)

# --------------------------
# GENERATE ORDERS
# --------------------------
orders = []

base_date = datetime(2026, 4, 20)

for i in range(num_orders):

    customer = random.choice(customers)

    product = random.choices(products, product_weights)[0]

    quantity = np.random.randint(1, 5)

    price_variation = product[3] * np.random.uniform(0.9, 1.2)

    order = {
        "order_id": f"ORD4_{i}",
        "customer_id": customer["customer_id"],
        "product_id": product[0],
        "order_date": (base_date + timedelta(days=np.random.randint(0, 3))).date(),
        "quantity": quantity,
        "price": round(price_variation, 2),
        "updated_at": datetime.now()
    }

    orders.append(order)

orders_df = pd.DataFrame(orders)

# --------------------------
# SAVE FILES
# --------------------------
customers_df.to_csv("customers_day4.csv", index=False)
orders_df.to_csv("sales_day4.csv", index=False)

print("🔥 Day 4 dataset generated!")