from flask import Flask, jsonify, render_template, request
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)

# --------------------------
# ATHENA CONNECTION
# --------------------------
engine = create_engine(
    "awsathena+rest://@athena.ap-southeast-2.amazonaws.com:443/fmcg_analytics?s3_staging_dir=s3://fmcg-data-lake-varsha/athena-results/"
)

def run_query(query):
    return pd.read_sql(query, engine)

# --------------------------
# ROUTES
# --------------------------
@app.route("/")
def home():
    return render_template("index.html")

# ---------------- KPI ----------------
@app.route("/kpis")
def kpis():
    df_sales = run_query("SELECT * FROM fmcg_analytics.sales_summary")
    df_customers = run_query("SELECT * FROM fmcg_analytics.customer_sales")

    total_revenue = df_sales["total_revenue"].sum()
    total_orders = df_sales["total_orders"].sum()
    total_customers = df_customers["customer_id"].nunique()
    aov = total_revenue / total_orders if total_orders else 0

    return jsonify({
        "total_revenue": float(total_revenue),
        "total_orders": int(total_orders),
        "total_customers": int(total_customers),
        "aov": float(aov)
    })

# ---------------- SALES ----------------
@app.route("/sales")
def sales():
    start = request.args.get("start")
    end = request.args.get("end")

    query = "SELECT * FROM fmcg_analytics.sales_summary WHERE 1=1"

    if start and end:
        query += f" AND order_date BETWEEN DATE '{start}' AND DATE '{end}'"

    df = run_query(query)
    return df.to_json(orient="records")

# ---------------- PRODUCTS ----------------
@app.route("/products")
def products():
    category = request.args.get("category")

    query = "SELECT * FROM fmcg_analytics.product_sales WHERE 1=1"

    if category and category != "":
        query += f" AND category = '{category}'"

    df = run_query(query)
    return df.to_json(orient="records")

# ---------------- CUSTOMERS ----------------
@app.route("/customers")
def customers():
    region = request.args.get("region")

    query = "SELECT * FROM fmcg_analytics.customer_sales WHERE 1=1"

    if region and region != "":
        query += f" AND region = '{region}'"

    df = run_query(query)
    return df.to_json(orient="records")

# --------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)