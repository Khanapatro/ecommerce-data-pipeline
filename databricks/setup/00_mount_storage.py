# Databricks notebook source
# 00_mount_storage.py
# Purpose: Configure direct access to Azure Data Lake Gen2 containers
# Note: dbutils.fs.mount() is not supported on User Isolation clusters.
#       Using direct abfss:// access via Spark config instead.

# ── Credentials from Databricks secrets ────────────────────────────
storage_account_name = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="AZURE_STORAGE_ACCOUNT_NAME"
)
storage_account_key = dbutils.secrets.get(
    scope="ecommerce-scope",
    key="AZURE_STORAGE_ACCOUNT_KEY"
)

# ── Configure direct access via Spark conf ─────────────────────────
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)
print(f" Configured direct access for storage account: {storage_account_name}")

# ── Container paths (use these in downstream notebooks) ───────────
containers = ["ecommerce-bronze", "ecommerce-silver", "ecommerce-gold"]
for container in containers:
    path = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/"
    print(f"   {container}: {path}")

# ── List bronze files ──────────────────────────────────────────────
print("\n--- Bronze Files ---")
try:
    bronze_path = f"wasbs://ecommerce-bronze@{storage_account_name}.blob.core.windows.net/"
    display(dbutils.fs.ls(bronze_path))
except Exception as e:
    print(f"No files yet or error: {e}")
