# Databricks notebook source
# 03_top_products.py
# Purpose: Calculate top products metrics from silver orders

from pyspark.sql.functions import (
    col, sum, countDistinct, avg, round,
    current_timestamp
)

print("📥 Reading silver.orders...")
df = spark.table("silver.orders")

print(f"   Records: {df.count()}")

# ─────────────────────────────────────────────
# Filter only completed orders
# ─────────────────────────────────────────────
df = df.filter(
    col("status").isin(["delivered", "shipped", "confirmed", "processing"])
)

# ─────────────────────────────────────────────
# Calculate product metrics
# ─────────────────────────────────────────────
top_products = df.groupBy(
    "product_id",
    "product_name",
    "category"
).agg(
    sum("quantity").alias("total_units_sold"),
    round(sum("line_total"), 2).alias("total_revenue"),
    countDistinct("order_id").alias("total_orders"),
    round(avg("unit_price"), 2).alias("avg_unit_price"),
    round(avg("discount") * 100, 2).alias("avg_discount_pct")
).orderBy(col("total_revenue").desc())

# ─────────────────────────────────────────────
# Add metadata
# ─────────────────────────────────────────────
top_products = top_products.withColumn(
    "_gold_timestamp",
    current_timestamp()
)

print(f"   Unique products: {top_products.count()}")

# ─────────────────────────────────────────────
# Write to Gold layer
# ─────────────────────────────────────────────
top_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.top_products")

print("✅ Saved to gold.top_products")

display(
spark.sql("""
SELECT
    product_name,
    category,
    total_units_sold,
    total_revenue,
    avg_discount_pct
FROM gold.top_products
ORDER BY total_revenue DESC
LIMIT 10
"""))