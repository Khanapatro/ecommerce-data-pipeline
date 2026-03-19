# Databricks notebook source
# 02_clean_orders.py
# Purpose: Clean, flatten and standardize orders data from bronze to silver

from pyspark.sql.functions import (
    col, explode, trim, lower,
    to_timestamp, current_timestamp
)
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

print(" Reading bronze.orders...")
df = spark.table("bronze.orders")
print(f"   Raw records: {df.count()}")

# ─────────────────────────────────────────────
# Remove nulls on critical fields
# ─────────────────────────────────────────────
df = df.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("order_total").isNotNull()
)

# ─────────────────────────────────────────────
# Fix data types
# ─────────────────────────────────────────────
df = df.withColumn("order_total",   col("order_total").cast(DecimalType(10,2))) \
       .withColumn("subtotal",      col("subtotal").cast(DecimalType(10,2))) \
       .withColumn("tax",           col("tax").cast(DecimalType(10,2))) \
       .withColumn("shipping_cost", col("shipping_cost").cast(DecimalType(10,2))) \
       .withColumn("item_count",    col("item_count").cast(IntegerType())) \
       .withColumn("event_time",    to_timestamp(col("event_time")))

# ─────────────────────────────────────────────
# Standardize text fields
# ─────────────────────────────────────────────
df = df.withColumn("status",  lower(trim(col("status")))) \
       .withColumn("channel", lower(trim(col("channel"))))

# ─────────────────────────────────────────────
# Flatten shipping_address struct
# ─────────────────────────────────────────────
df = df \
.withColumn("shipping_city",    col("shipping_address.city")) \
.withColumn("shipping_country", col("shipping_address.country")) \
.withColumn("shipping_state",   col("shipping_address.state")) \
.withColumn("shipping_street",  col("shipping_address.street")) \
.withColumn("shipping_zip_code",col("shipping_address.zip_code")) \
.drop("shipping_address")

# ─────────────────────────────────────────────
# Explode items array
# ─────────────────────────────────────────────
df = df.withColumn("item", explode(col("items")))

df = df \
.withColumn("product_id",   col("item.product_id")) \
.withColumn("product_name", col("item.product_name")) \
.withColumn("category",     col("item.category")) \
.withColumn("unit_price",   col("item.unit_price").cast(DecimalType(10,2))) \
.withColumn("quantity",     col("item.quantity").cast(IntegerType())) \
.withColumn("discount",     col("item.discount").cast(DecimalType(5,2))) \
.withColumn("line_total",   col("item.line_total").cast(DecimalType(10,2))) \
.drop("item", "items")

# ─────────────────────────────────────────────
# Remove negative values
# ─────────────────────────────────────────────
df = df.filter(
    (col("order_total") > 0) &
    (col("line_total") > 0)
)

# ─────────────────────────────────────────────
# Deduplicate orders
# ─────────────────────────────────────────────
window = Window.partitionBy("order_id","product_id").orderBy(desc("event_time"))

df = df.withColumn("row_num", row_number().over(window)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")

# ─────────────────────────────────────────────
# Add metadata column
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
  .saveAsTable("silver.orders")

print(" Saved to silver.orders")

display(
spark.sql("""
SELECT
    order_id,
    customer_id,
    status,
    order_total,
    product_id,
    product_name,
    category,
    quantity,
    line_total,
    shipping_city,
    shipping_country,
    channel,
    event_time
FROM silver.orders
LIMIT 10
"""))
