# Databricks notebook source
# 02_clean_customers.py
# Purpose: Clean and standardize customers data from bronze to silver

from pyspark.sql.functions import (
    col, trim, lower, to_date,
    current_timestamp, concat, lit, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

print("📥 Reading bronze.customers...")
df = spark.table("bronze.customers")
print(f"   Raw records: {df.count()}")

# ── Remove nulls on critical fields ────────────────────────────────
df = df.filter(
    col("customer_id").isNotNull() &
    col("email").isNotNull()
)

# ── Fix data types ──────────────────────────────────────────────────
df = df.withColumn("event_time", to_date(col("event_time"))) \
       .withColumn("signup_date", to_date(col("signup_date")))

# ── Standardize text fields ─────────────────────────────────────────
df = df.withColumn("email", lower(trim(col("email")))) \
       .withColumn("segment", lower(trim(col("segment")))) \
       .withColumn("first_name", trim(col("first_name"))) \
       .withColumn("last_name", trim(col("last_name")))

# ── Fill missing segment with bronze ───────────────────────────────
df = df.withColumn(
    "segment",
    coalesce(col("segment"), lit("bronze"))
)

# ── Create full name ────────────────────────────────────────────────
df = df.withColumn(
    "full_name",
    concat(col("first_name"), lit(" "), col("last_name"))
)

# ── Flatten address struct ─────────────────────────────────────────
df = df \
.withColumn("address_city", col("address.city")) \
.withColumn("address_country", col("address.country")) \
.withColumn("address_state", col("address.state")) \
.withColumn("address_street", col("address.street")) \
.withColumn("address_zip_code", col("address.zip_code")) \
.drop("address")

# ── Deduplicate by customer_id (keep latest record) ─────────────────
window = Window.partitionBy("customer_id").orderBy(desc("event_time"))

df = df.withColumn("row_num", row_number().over(window)) \
       .filter(col("row_num") == 1) \
       .drop("row_num")

# ── Add silver metadata ─────────────────────────────────────────────
df = df.withColumn("_silver_timestamp", current_timestamp())

print(f"   Clean records: {df.count()}")

# ── Write to silver table ───────────────────────────────────────────
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("silver.customers")

print("Saved to silver.customers")

display(
    spark.sql("""
    SELECT
        customer_id,
        full_name,
        email,
        segment,
        address_city,
        address_state,
        address_zip_code
    FROM silver.customers
    LIMIT 5
    """)
)
