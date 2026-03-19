# Databricks notebook source
# 01_create_databases.py
# Purpose: Create bronze, silver, gold databases in Databricks

# ── Create databases ───────────────────────────────────────────────────────────
databases = ["bronze", "silver", "gold"]

for db in databases:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"Database created: {db}")

# ── Set database locations to mounted storage ──────────────────────────────────
spark.sql("""
    CREATE DATABASE IF NOT EXISTS bronze
    LOCATION '/mnt/ecommerce-bronze'
""")

spark.sql("""
    CREATE DATABASE IF NOT EXISTS silver
    LOCATION '/mnt/ecommerce-silver'
""")

spark.sql("""
    CREATE DATABASE IF NOT EXISTS gold
    LOCATION '/mnt/ecommerce-gold'
""")

# ── Verify databases ───────────────────────────────────────────────────────────
print("\n--- All Databases ---")
display(spark.sql("SHOW DATABASES"))
