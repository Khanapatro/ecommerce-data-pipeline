# Databricks notebook source
# 02_clean_payments.py
# Purpose: Clean and standardize payments data from bronze to silver

from pyspark.sql.functions import (
    col, trim, lower, to_timestamp,
    current_timestamp
)
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

print(" Reading bronze.payments...")
df = spark.table("bronze.payments")
print(f"   Raw records: {df.count()}")

# ── Remove nulls on critical fields ────────────────────────────────
df = df.filter(
    col("payment_id").isNotNull() &
    col("order_id").isNotNull() &
    col("amount").isNotNull()
)

# ── Fix data types ──────────────────────────────────────────────────
df = df.withColumn("amount", col("amount").cast(DecimalType(10,2))) \
       .withColumn("processing_time_ms", col("processing_time_ms").cast(IntegerType())) \
       .withColumn("event_time", to_timestamp(col("event_time")))

# ── Standardize text fields ─────────────────────────────────────────
df = df.withColumn("payment_status", lower(trim(col("payment_status")))) \
       .withColumn("payment_method", lower(trim(col("payment_method")))) \
       .withColumn("gateway", lower(trim(col("gateway"))))

# ── Remove negative amounts ─────────────────────────────────────────
df = df.filter(col("amount") > 0)

# ── Flatten billing_address struct ─────────────────────────────────
df = df \
.withColumn("billing_city", col("billing_address.city")) \
.withColumn("billing_country", col("billing_address.country")) \
.withColumn("billing_state", col("billing_address.state")) \
.withColumn("billing_street", col("billing_address.street")) \
.withColumn("billing_zip_code", col("billing_address.zip_code")) \
.drop("billing_address")

# ── Deduplicate by payment_id ──────────────────────────────────────
window = Window.partitionBy("payment_id").orderBy(desc("event_time"))

df = df.withColumn("row_num", row_number().over(window)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")

# ── Add silver metadata ────────────────────────────────────────────
df = df.withColumn("_silver_timestamp", current_timestamp())

print(f"   Clean records: {df.count()}")

# ── Write to silver table ──────────────────────────────────────────
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("silver.payments")

print("Saved to silver.payments")

display(
    spark.sql("""
    SELECT
        payment_id,
        order_id,
        payment_method,
        payment_status,
        amount,
        billing_city,
        billing_state,
        billing_zip_code
    FROM silver.payments
    LIMIT 5
    """)
)
