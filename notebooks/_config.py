# Databricks notebook source
# MAGIC %md
# MAGIC # Shared configuration
# MAGIC
# MAGIC Single source of truth for every name shared across the four artifacts. **Edit the
# MAGIC values below once**, then import this repo into *both* workspaces (provider and
# MAGIC recipient). Each notebook pulls these in with `%run ./_config`.
# MAGIC
# MAGIC A few values are only known after `terraform apply` creates Workspace B + Metastore B:
# MAGIC fill `RECIPIENT_SHARING_IDENTIFIER` from the new metastore's global sharing identifier
# MAGIC (Terraform output `metastore_id` / the metastore's `Delta Sharing organization name`).

# COMMAND ----------

# ============================== SHARED (both regions) ==============================
CATALOG         = "sample_catalog"      # UC catalog holding the offline table + model in Workspace A
SCHEMA          = "sample_schema"       # UC schema in Workspace A
TABLE_NAME      = "account_features"    # offline feature table (one row per account, PK = user_id)
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"
MODEL_NAME      = f"{CATALOG}.{SCHEMA}.fraud_detection_model"   # UC-registered model

PK_COLUMN = ["user_id"]                 # primary key column(s) of the feature table
FEATURES  = [                           # feature columns the model scores at transaction time
    "account_age_days", "num_txns_24h", "avg_amount_30d", "last_txn_amount",
    "num_countries_7d", "merchant_risk_score", "failed_auth_24h", "amount_to_avg_ratio",
]

# ============================== PROVIDER: Workspace A (us-east-1) ==============================
PROVIDER_PROJECT_ID      = "sample-feature-serving"    # Lakebase Autoscaling project (the instance)
CU_MIN, CU_MAX           = 1.0, 8.0                    # autoscaling compute range, in CU
HA_NODES                 = 2                           # node group size (2 = HA: primary + standby)
READABLE_SECONDARIES     = True                        # serve reads off the standby node(s)
SCALE_TO_ZERO_DISABLED   = True                        # keep the endpoint always-on for consistent latency
PROVIDER_ENDPOINT_NAME   = "sample-online-fs-endpoint"

SYNCED_TABLE = f"{FULL_TABLE_NAME}_online"             # online synced table in the provider region
PG_DATABASE  = CATALOG                                 # serving derives the Postgres db from the synced table's catalog
BRANCH       = f"projects/{PROVIDER_PROJECT_ID}/branches/production"  # default branch is always 'production'

# ============================== DELTA SHARING ==============================
SHARE_NAME     = "fraud_serving_share"                 # share created on Metastore A (provider)
RECIPIENT_NAME = "region_b_recipient"                  # recipient object on Metastore A
# Metastore B's global sharing identifier, e.g. "aws:us-west-2:<uuid>". Fill after terraform apply.
RECIPIENT_SHARING_IDENTIFIER = "<FILL_FROM_METASTORE_B>"
SHARED_CATALOG = "shared_from_provider"                # catalog mounted in Workspace B from the share

# ============================== RECIPIENT: Workspace B (us-west-2, serverless) ==============================
RECIPIENT_PROJECT_ID   = "sample-feature-serving-recipient"   # Lakebase Autoscaling project in region B
RECIPIENT_ENDPOINT_NAME = "sample-online-lookup-endpoint"
RECIPIENT_CATALOG      = "sample_recipient_catalog"    # local UC catalog in B (we have write perms here)
RECIPIENT_SCHEMA       = "sample_recipient_schema"

# The Delta-Shared offline table + model, as seen from Workspace B (read-only)
FULL_SHARED_TABLE_NAME = f"{SHARED_CATALOG}.{SCHEMA}.{TABLE_NAME}"
SHARED_MODEL_NAME      = f"{SHARED_CATALOG}.{SCHEMA}.fraud_detection_model"

print("Config loaded. Provider table:", FULL_TABLE_NAME, "| Model:", MODEL_NAME)
