# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Delta Sharing (Provider Metastore A → Recipient Metastore B)
# MAGIC
# MAGIC Bridges the two regions with **Databricks-to-Databricks Delta Sharing**. The provider
# MAGIC (Metastore A, Workspace A) shares the offline feature table *and* the registered model; the
# MAGIC recipient (Metastore B, Workspace B) mounts them as a read-only catalog. No data is copied:
# MAGIC recipients read from the provider's storage through scoped, time-limited credentials.
# MAGIC
# MAGIC > **This notebook spans two workspaces.** Run the **Provider** cells in Workspace A and the
# MAGIC > **Recipient** cells in Workspace B. It is intentionally *not* a "Run all".
# MAGIC >
# MAGIC > **Prerequisites:** `01_provider_single_region` has created the table + model in A, and
# MAGIC > `terraform/` has created Metastore B (its sharing identifier is in `_config`).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider side (Metastore A), Step 7: create the share and the recipient
# MAGIC
# MAGIC Run these cells in **Workspace A**. `RECIPIENT_SHARING_IDENTIFIER` is Metastore B's global
# MAGIC sharing identifier (e.g. `aws:us-west-2:<uuid>`), read from the metastore Terraform created.

# COMMAND ----------

spark.sql(f"CREATE SHARE IF NOT EXISTS {SHARE_NAME}")

# Databricks-to-Databricks recipient, identified by Metastore B's sharing identifier.
spark.sql(f"""
    CREATE RECIPIENT IF NOT EXISTS {RECIPIENT_NAME}
    USING ID '{RECIPIENT_SHARING_IDENTIFIER}'
""")
print("Share + recipient created:", SHARE_NAME, "->", RECIPIENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: add the feature table and model to the share, then grant to the recipient

# COMMAND ----------

# Share the offline feature table and the registered UC model.
spark.sql(f"ALTER SHARE {SHARE_NAME} ADD TABLE {FULL_TABLE_NAME}")
spark.sql(f"ALTER SHARE {SHARE_NAME} ADD MODEL {MODEL_NAME}")

# Grant the recipient read access to the share.
spark.sql(f"GRANT SELECT ON SHARE {SHARE_NAME} TO RECIPIENT {RECIPIENT_NAME}")

# Confirm what the share now contains
display(spark.sql(f"SHOW ALL IN SHARE {SHARE_NAME}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recipient side (Metastore B): mount the share as a catalog
# MAGIC
# MAGIC Run these cells in **Workspace B**. The provider name is how Metastore B refers back to
# MAGIC Metastore A; discover it with `SHOW PROVIDERS`, then create a catalog from the share.

# COMMAND ----------

# Discover the provider (Metastore A) as seen from this recipient metastore.
display(spark.sql("SHOW PROVIDERS"))

# COMMAND ----------

# Replace <provider_name> with the provider shown above (the row whose share is fraud_serving_share).
PROVIDER_NAME = "<provider_name>"   # e.g. the auto-generated D2D provider name for Metastore A

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {SHARED_CATALOG}
    USING SHARE {PROVIDER_NAME}.{SHARE_NAME}
""")

# Grant the serving principals access to the mounted, read-only catalog.
spark.sql(f"GRANT USE CATALOG ON CATALOG {SHARED_CATALOG} TO `account users`")
spark.sql(f"GRANT SELECT ON CATALOG {SHARED_CATALOG} TO `account users`")
print("Shared catalog mounted:", SHARED_CATALOG)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the shared assets are visible in Workspace B
# MAGIC
# MAGIC **Validation note:** the table should be queryable read-only and the model should list its
# MAGIC versions. These are exactly the objects `03_recipient_serving` consumes.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {SHARED_CATALOG}.{SCHEMA}"))
display(spark.sql(f"SHOW MODELS IN {SHARED_CATALOG}.{SCHEMA}"))
# Spot-check a read from the shared table
display(spark.table(FULL_SHARED_TABLE_NAME).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC
# MAGIC Reverse order: drop the recipient-side catalog first (Workspace B), then remove assets and
# MAGIC the share/recipient on the provider (Workspace A).

# COMMAND ----------

# --- Recipient side (Workspace B) ---
# spark.sql(f"DROP CATALOG IF EXISTS {SHARED_CATALOG} CASCADE")
#
# --- Provider side (Workspace A) ---
# spark.sql(f"ALTER SHARE {SHARE_NAME} REMOVE TABLE {FULL_TABLE_NAME}")
# spark.sql(f"ALTER SHARE {SHARE_NAME} REMOVE MODEL {MODEL_NAME}")
# spark.sql(f"DROP RECIPIENT IF EXISTS {RECIPIENT_NAME}")
# spark.sql(f"DROP SHARE IF EXISTS {SHARE_NAME}")
# print("Delta Sharing cleanup complete.")
