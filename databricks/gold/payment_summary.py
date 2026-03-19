# Databricks notebook source
# 03_payment_summary.py
# Purpose: Calculate payment analytics from silver payments

from pyspark.sql.functions import (
    col, sum, countDistinct, avg, round,
    when, current_timestamp, lit
)

print("📥 Reading silver.payments...")
df = spark.table("silver.payments")
print(f"   Records: {df.count()}")

# ─────────────────────────────────────────────
# Aggregate payment metrics
# ─────────────────────────────────────────────
payment_method_summary = df.groupBy("payment_method", "gateway") \
    .agg(
        countDistinct("payment_id").alias("total_transactions"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),
        round(avg("processing_time_ms"), 0).alias("avg_processing_time_ms"),

        sum(when(col("payment_status") == "captured", 1).otherwise(0)).alias("successful"),
        sum(when(col("payment_status") == "failed", 1).otherwise(0)).alias("failed"),
        sum(when(col("payment_status") == "refunded", 1).otherwise(0)).alias("refunded"),
        sum(when(col("payment_status") == "disputed", 1).otherwise(0)).alias("disputed")
    )

# ─────────────────────────────────────────────
# Calculate success / failure rates safely
# ─────────────────────────────────────────────
payment_method_summary = payment_method_summary \
    .withColumn(
        "success_rate",
        round(
            when(col("total_transactions") > 0,
                 (col("successful") / col("total_transactions")) * 100
            ).otherwise(lit(0)), 2
        )
    ) \
    .withColumn(
        "failure_rate",
        round(
            when(col("total_transactions") > 0,
                 (col("failed") / col("total_transactions")) * 100
            ).otherwise(lit(0)), 2
        )
    )

# ─────────────────────────────────────────────
# Add metadata
# ─────────────────────────────────────────────
payment_method_summary = payment_method_summary.withColumn(
    "_gold_timestamp",
    current_timestamp()
)

print(f"   Payment method combinations: {payment_method_summary.count()}")

# ─────────────────────────────────────────────
# Write to gold layer
# ─────────────────────────────────────────────
payment_method_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.payment_summary")

print("✅ Saved to gold.payment_summary")

display(
spark.sql("""
SELECT
    payment_method,
    gateway,
    total_transactions,
    total_amount,
    success_rate,
    failure_rate,
    avg_processing_time_ms
FROM gold.payment_summary
ORDER BY total_amount DESC
"""))