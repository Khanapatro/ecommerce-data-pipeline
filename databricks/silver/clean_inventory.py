# Databricks notebook source
# 02_clean_inventory.py
# Purpose: Clean and standardize inventory data from bronze to silver

from pyspark.sql.functions import (
    col, when, trim, to_timestamp,
    current_timestamp, lit, coalesce
)
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

print("📥 Reading bronze.inventory...")
df = spark.table("bronze.inventory")
print(f"   Raw records: {df.count()}")

# ─────────────────────────────────────────────
# Remove nulls on critical fields
# ─────────────────────────────────────────────
df = df.filter(
    col("product_id").isNotNull() &
    col("warehouse_id").isNotNull() &
    col("quantity_on_hand").isNotNull()
)

# ─────────────────────────────────────────────
# Fix data types
# ─────────────────────────────────────────────
df = df.withColumn("quantity_on_hand",  col("quantity_on_hand").cast(IntegerType())) \
       .withColumn("quantity_reserved", col("quantity_reserved").cast(IntegerType())) \
       .withColumn("reorder_point",     col("reorder_point").cast(IntegerType())) \
       .withColumn("reorder_quantity",  col("reorder_quantity").cast(IntegerType())) \
       .withColumn("unit_cost",         col("unit_cost").cast(DecimalType(10,2))) \
       .withColumn("event_time",        to_timestamp(col("event_time")))

# ─────────────────────────────────────────────
# Clean text fields
# ─────────────────────────────────────────────
df = df.withColumn("warehouse_location", trim(col("warehouse_location")))

# ─────────────────────────────────────────────
# Remove invalid negative quantities
# ─────────────────────────────────────────────
df = df.filter(col("quantity_on_hand") >= 0)

# ─────────────────────────────────────────────
# Calculate available quantity
# ─────────────────────────────────────────────
df = df.withColumn(
    "quantity_available",
    col("quantity_on_hand") - coalesce(col("quantity_reserved"), lit(0))
)

# ─────────────────────────────────────────────
# Flag low stock items
# ─────────────────────────────────────────────
df = df.withColumn(
    "is_low_stock",
    when(col("quantity_on_hand") <= col("reorder_point"), True)
    .otherwise(False)
)

# ─────────────────────────────────────────────
# Deduplicate records
# ─────────────────────────────────────────────
window = Window.partitionBy("product_id","warehouse_id") \
               .orderBy(desc("event_time"))

df = df.withColumn("row_num", row_number().over(window)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")

# ─────────────────────────────────────────────
# Add silver metadata
# ─────────────────────────────────────────────
df = df.withColumn("_silver_timestamp", current_timestamp())

print(f"   Clean records: {df.count()}")

# ─────────────────────────────────────────────
# Write to silver layer
# ─────────────────────────────────────────────
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("silver.inventory")

print("✅ Saved to silver.inventory")

display(
spark.sql("""
SELECT
    product_id,
    warehouse_id,
    warehouse_location,
    quantity_on_hand,
    quantity_reserved,
    quantity_available,
    is_low_stock
FROM silver.inventory
LIMIT 5
"""))