# Databricks notebook source
# 03_daily_revenue.py
# Purpose: Calculate daily revenue metrics from silver orders

from pyspark.sql.functions import (
    col, sum, countDistinct, avg, round,
    to_date, current_timestamp
)

print("📥 Reading silver.orders...")
df = spark.table("silver.orders")
print(f"   Records: {df.count()}")

# ─────────────────────────────────────────────
# Convert event_time to order_date
# ─────────────────────────────────────────────
df = df.withColumn(
    "order_date",
    to_date(col("event_time"))
)

# ─────────────────────────────────────────────
# First aggregate per order
# (important because orders table is exploded)
# ─────────────────────────────────────────────
order_level = df.groupBy(
    "order_id",
    "order_date"
).agg(
    round(sum("line_total"), 2).alias("order_revenue"),
    round(sum("tax"), 2).alias("order_tax"),
    round(sum("shipping_cost"), 2).alias("order_shipping"),
    round(sum("subtotal"), 2).alias("order_subtotal")
)

# ─────────────────────────────────────────────
# Calculate daily metrics
# ─────────────────────────────────────────────
daily_revenue = order_level.groupBy("order_date") \
    .agg(
        round(sum("order_revenue"), 2).alias("total_revenue"),
        countDistinct("order_id").alias("total_orders"),
        round(avg("order_revenue"), 2).alias("avg_order_value"),
        round(sum("order_tax"), 2).alias("total_tax"),
        round(sum("order_shipping"), 2).alias("total_shipping"),
        round(sum("order_subtotal"), 2).alias("total_subtotal")
    ) \
    .orderBy("order_date")

# ─────────────────────────────────────────────
# Add metadata
# ─────────────────────────────────────────────
daily_revenue = daily_revenue.withColumn(
    "_gold_timestamp",
    current_timestamp()
)

print(f"   Days of data: {daily_revenue.count()}")

# ─────────────────────────────────────────────
# Write to gold layer
# ─────────────────────────────────────────────
daily_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.daily_revenue")

print("✅ Saved to gold.daily_revenue")

display(
spark.sql("""
SELECT
    order_date,
    total_revenue,
    total_orders,
    avg_order_value
FROM gold.daily_revenue
ORDER BY order_date DESC
LIMIT 10
"""))