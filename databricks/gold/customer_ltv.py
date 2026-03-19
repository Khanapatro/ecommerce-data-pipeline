# Databricks notebook source
# 03_customer_ltv.py
# Purpose: Calculate customer lifetime value from silver orders and customers

from pyspark.sql.functions import (
    col, sum, countDistinct, avg, round, min, max,
    datediff, to_date, current_timestamp, current_date,
    coalesce, lit
)

print("📥 Reading silver tables...")
orders_df    = spark.table("silver.orders")
customers_df = spark.table("silver.customers")

# ─────────────────────────────────────────────
# Filter only valid orders
# ─────────────────────────────────────────────
orders_filtered = orders_df.filter(
    col("status").isin(["delivered", "shipped", "confirmed", "processing"])
)

# ─────────────────────────────────────────────
# Convert event_time to date
# ─────────────────────────────────────────────
orders_filtered = orders_filtered.withColumn(
    "order_date",
    to_date(col("event_time"))
)

# ─────────────────────────────────────────────
# Calculate customer order metrics
# IMPORTANT:
# - orders table is exploded by product
# - use line_total for correct revenue
# ─────────────────────────────────────────────
customer_orders = orders_filtered.groupBy("customer_id") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        round(sum("line_total"), 2).alias("total_spent"),
        round(avg("line_total"), 2).alias("avg_order_value"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("last_order_date")
    )

# ─────────────────────────────────────────────
# Calculate customer tenure
# ─────────────────────────────────────────────
customer_orders = customer_orders.withColumn(
    "days_as_customer",
    datediff(current_date(), col("first_order_date"))
)

# ─────────────────────────────────────────────
# Join with customer dimension
# ─────────────────────────────────────────────
customer_ltv = customer_orders.join(
    customers_df.select(
        "customer_id",
        "full_name",
        "email",
        "segment",
        "is_active",
        "signup_date"
    ),
    on="customer_id",
    how="left"
)

# ─────────────────────────────────────────────
# Handle missing customer data
# ─────────────────────────────────────────────
customer_ltv = customer_ltv \
.withColumn("full_name", coalesce(col("full_name"), lit("Unknown Customer"))) \
.withColumn("segment", coalesce(col("segment"), lit("unknown"))) \
.withColumn("email", coalesce(col("email"), lit("not_available"))) \
.withColumn("is_active", coalesce(col("is_active"), lit(False)))

# ─────────────────────────────────────────────
# Replace null numeric values
# ─────────────────────────────────────────────
customer_ltv = customer_ltv.fillna({
    "total_orders": 0,
    "total_spent": 0,
    "avg_order_value": 0
})

# ─────────────────────────────────────────────
# Calculate LTV score
# LTV = avg_order_value × total_orders
# ─────────────────────────────────────────────
customer_ltv = customer_ltv.withColumn(
    "ltv_score",
    round(col("avg_order_value") * col("total_orders"), 2)
)

# ─────────────────────────────────────────────
# Add gold metadata
# ─────────────────────────────────────────────
customer_ltv = customer_ltv.withColumn(
    "_gold_timestamp",
    current_timestamp()
)

print(f"   Customers with orders: {customer_ltv.count()}")

# ─────────────────────────────────────────────
# Write to Gold layer
# ─────────────────────────────────────────────
customer_ltv.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.customer_ltv")

print("✅ Saved to gold.customer_ltv")

display(
spark.sql("""
SELECT
    customer_id,
    full_name,
    segment,
    total_orders,
    total_spent,
    avg_order_value,
    ltv_score
FROM gold.customer_ltv
ORDER BY total_spent DESC
LIMIT 10
"""))