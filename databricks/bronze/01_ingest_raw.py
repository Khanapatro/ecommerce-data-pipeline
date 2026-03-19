# Databricks notebook source
# 01_ingest_raw.py
# Purpose: Read raw JSON files from Azure Storage and save as Delta tables in bronze database

from pyspark.sql.functions import current_timestamp, lit, col

# ── Storage credentials ─────────────────────────────────────────────
storage_account_name = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="AZURE_STORAGE_ACCOUNT_NAME"
)
storage_account_key = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="AZURE_STORAGE_ACCOUNT_KEY"
)

# ── Configure Spark to access Azure Storage ─────────────────────────
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# ── Base path ───────────────────────────────────────────────────────
base_path = f"wasbs://ecommerce-bronze@{storage_account_name}.blob.core.windows.net/bronze"

# ── Topics to ingest ────────────────────────────────────────────────
topics = [
    "ecom.orders",
    "ecom.payments",
    "ecom.customers",
    "ecom.inventory",
    "ecom.clickstream"
]

# ── Ingest each topic ───────────────────────────────────────────────
for topic in topics:
    table_name = topic.replace("ecom.", "")
    source_path = f"{base_path}/{topic}"

    print(f"\n📥 Ingesting: {topic}")
    print(f"   Source: {source_path}")

    try:
        # Read raw JSON files
        df = spark.read \
                  .option("multiline", "false") \
                  .option("inferSchema", "true") \
                  .json(source_path)

        # Add metadata columns
        # Using _metadata.file_path instead of input_file_name() for Unity Catalog
        df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
               .withColumn("_source_topic", lit(topic)) \
               .withColumn("_source_file", col("_metadata.file_path"))

        # Show record count
        record_count = df.count()
        print(f"   Records found: {record_count}")
        print(f"   Columns: {len(df.columns)}")

        # Write to bronze Delta table
        df.write \
          .format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(f"bronze.{table_name}")

        print(f"   ✅ Saved to bronze.{table_name}")

    except Exception as e:
        print(f"   ❌ Error ingesting {topic}: {e}")

# ── Verify tables created ───────────────────────────────────────────
print("\n--- Bronze Tables ---")
display(spark.sql("SHOW TABLES IN bronze"))