# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Single-Region Feature Serving (Provider, Workspace A)
# MAGIC
# MAGIC Run this in the **provider** workspace (e.g. `us-east-1`). It builds the full single-region
# MAGIC fraud-serving stack that Part 2 later extends to a second region:
# MAGIC
# MAGIC 1. Offline fraud feature table in Delta (Step 1)
# MAGIC 2. Lakebase Autoscaling instance: CU range + HA + readable secondaries (Step 2)
# MAGIC 3. Continuous synced table with accelerated sync (Step 3)
# MAGIC 4. Train + register a GradientBoosting fraud model to Unity Catalog (Step 4)
# MAGIC 5. Real-time model serving endpoint (Step 5)
# MAGIC
# MAGIC Edit shared names in `_config`, then **Run all**.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC %restart_python

# COMMAND ----------

import databricks.sdk
from databricks.sdk import WorkspaceClient
print("sdk version:", databricks.sdk.version.__version__)   # need >= 0.116.0 for postgres
print("has postgres:", hasattr(WorkspaceClient(), "postgres"))

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


def wait_for(label, fetch, done, timeout=1800, interval=20):
    """Poll `fetch` until `done(result)` is truthy, or raise on timeout."""
    end = time.time() + timeout
    while time.time() < end:
        v = fetch()
        if done(v):
            return v
        time.sleep(interval)
    raise TimeoutError(f"timed out waiting for {label}")


def create_ignoring_exists(fn):
    """Run a create call, swallowing 'already exists' errors so re-runs are idempotent."""
    try:
        fn()
    except Exception as e:
        if "exist" not in str(e).lower():
            raise


def synced_table_ready(st):
    """True once the synced table is serving. Raises if the pipeline failed.

    A plain "ONLINE" substring check is unsafe: the failed state
    SYNCED_TABLE_ONLINE_PIPELINE_FAILED also contains "ONLINE".
    """
    state = str(st.status.detailed_state)
    if "FAILED" in state:
        raise RuntimeError(f"synced table pipeline failed: {state}")
    # Healthy serving states: ...ONLINE, ...ONLINE_NO_PENDING_UPDATE, ...ONLINE_CONTINUOUS_UPDATE
    return "ONLINE" in state

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Build the offline fraud feature table
# MAGIC
# MAGIC One row per account (keyed by `user_id`), holding recent-activity signals a fraud model
# MAGIC scores at transaction time. In production these come from streaming transaction data; here
# MAGIC we synthesize them and engineer real signal into an `is_fraud` label so Step 4 has something
# MAGIC meaningful to learn. Change Data Feed is enabled because continuous/triggered syncs require it.

# COMMAND ----------

from pyspark.sql.functions import col, rand, round, when, least, lit

# Synthesize account-level fraud-risk features (one row per account, keyed by user_id)
df = spark.range(5000).withColumnRenamed("id", "user_id")
df = (df
    .withColumn("account_age_days",    round(rand(seed=1) * 2000 + 1).cast("int"))   # newer accounts are riskier
    .withColumn("num_txns_24h",        round(rand(seed=2) * 20).cast("int"))         # transaction velocity
    .withColumn("avg_amount_30d",      round(rand(seed=3) * 200 + 20, 2))            # the account's normal spend
    .withColumn("last_txn_amount",     round(rand(seed=4) * 500 + 5, 2))            # amount of the txn being scored
    .withColumn("num_countries_7d",    round(rand(seed=5) * 4 + 1).cast("int"))      # distinct countries in last 7d
    .withColumn("merchant_risk_score", round(rand(seed=6), 3))                       # merchant risk, 0..1
    .withColumn("failed_auth_24h",     round(rand(seed=7) * 5).cast("int"))          # failed auth attempts
)

# Derived feature: how far the latest transaction deviates from the account's norm
df = df.withColumn("amount_to_avg_ratio", round(col("last_txn_amount") / col("avg_amount_30d"), 3))

# Build a fraud label WITH SIGNAL: risk rises with spend deviation, velocity,
# geographic spread, merchant risk, failed auths, and brand-new accounts.
fraud_score = (
      0.30 * least(col("amount_to_avg_ratio") / 5.0, lit(1.0))
    + 0.20 * least(col("num_txns_24h") / 20.0, lit(1.0))
    + 0.15 * least((col("num_countries_7d") - 1) / 3.0, lit(1.0))
    + 0.15 * col("merchant_risk_score")
    + 0.10 * least(col("failed_auth_24h") / 5.0, lit(1.0))
    + 0.10 * when(col("account_age_days") < 30, lit(1.0)).otherwise(lit(0.0))
)
# Draw a probabilistic label from the score so it is learnable but not perfectly separable
df = df.withColumn("is_fraud", (fraud_score > rand(seed=42)).cast("int"))
display(df)

# COMMAND ----------

# drop table if it already exists to maintain idempotency
spark.sql(f"DROP TABLE IF EXISTS {FULL_TABLE_NAME}")

(df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")  # required for continuous and triggered syncs
    .mode("overwrite")
    .saveAsTable(FULL_TABLE_NAME))

# A feature table needs a non-nullable primary key
spark.sql(f"ALTER TABLE {FULL_TABLE_NAME} ALTER COLUMN user_id SET NOT NULL")
spark.sql(f"ALTER TABLE {FULL_TABLE_NAME} ADD CONSTRAINT user_id_pk PRIMARY KEY (user_id)")
print("Offline feature table ready:", FULL_TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Lakebase Autoscaling instance
# MAGIC
# MAGIC We provision Lakebase with the **Databricks SDK** (`w.postgres`) rather than the Feature
# MAGIC Engineering client's `create_online_store`, because the SDK exposes the production config the
# MAGIC helper hides: an autoscaling CU range, an HA node group (primary + standby), and readable
# MAGIC secondaries. The instance is a Lakebase Autoscaling *project*; its default branch is `production`.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings,
    InitialEndpointSpec, EndpointGroupSpec,
)

create_ignoring_exists(lambda: w.postgres.create_project(
    project_id=PROVIDER_PROJECT_ID,
    project=Project(
        spec=ProjectSpec(
            display_name=PROVIDER_PROJECT_ID,
            pg_version=17,
            default_endpoint_settings=ProjectDefaultEndpointSettings(
                autoscaling_limit_min_cu=CU_MIN,
                autoscaling_limit_max_cu=CU_MAX,
                no_suspension=SCALE_TO_ZERO_DISABLED,
            ),
        ),
        initial_endpoint_spec=InitialEndpointSpec(
            group=EndpointGroupSpec(
                min=HA_NODES, max=HA_NODES,
                enable_readable_secondaries=READABLE_SECONDARIES,
            ),
        ),
    ),
))

wait_for(
    "Lakebase endpoint ACTIVE",
    lambda: list(w.postgres.list_endpoints(parent=BRANCH)),
    lambda eps: bool(eps) and all(str(e.status.current_state).endswith("ACTIVE") for e in eps),
)
print("Lakebase instance ready:", PROVIDER_PROJECT_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Continuous synced table (accelerated sync)
# MAGIC
# MAGIC The synced table propagates the offline Delta table into Lakebase without custom ETL. We use
# MAGIC the SDK's `create_synced_table` (not `publish_table`) for its production knobs, notably
# MAGIC `accelerated_sync`, which speeds the initial backfill and subsequent syncs, with a
# MAGIC `CONTINUOUS` scheduling policy.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    SyncedTable, SyncedTableSyncedTableSpec, NewPipelineSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedulingPolicy,
)

create_ignoring_exists(lambda: w.postgres.create_synced_table(
    synced_table_id=SYNCED_TABLE,
    synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
        branch=BRANCH,
        postgres_database=PG_DATABASE,
        source_table_full_name=FULL_TABLE_NAME,
        scheduling_policy=SchedulingPolicy.CONTINUOUS,   # SNAPSHOT | TRIGGERED | CONTINUOUS
        primary_key_columns=PK_COLUMN,
        create_database_objects_if_missing=True,
        accelerated_sync=True,                            # faster initial backfill + syncs
        new_pipeline_spec=NewPipelineSpec(
            storage_catalog=CATALOG,
            storage_schema=SCHEMA,
        ),
    )),
))

wait_for(
    "synced table ONLINE",
    lambda: w.postgres.get_synced_table(name=f"synced_tables/{SYNCED_TABLE}"),
    synced_table_ready,
)
print("Online store ready:", SYNCED_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Train and register the fraud model to Unity Catalog
# MAGIC
# MAGIC A gradient-boosted tree fits tabular fraud data well: it captures non-linear feature
# MAGIC interactions and trains in seconds on CPU. Registering to Unity Catalog (`databricks-uc`)
# MAGIC makes the model a governed, lineage-tracked asset we can serve directly and later share
# MAGIC across regions.

# COMMAND ----------

import mlflow
import pandas as pd
from mlflow.models.signature import infer_signature
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score

mlflow.set_registry_uri("databricks-uc")   # register to Unity Catalog, not the workspace registry

# Pull the offline feature table into pandas
pdf = spark.table(FULL_TABLE_NAME).select("user_id", *FEATURES, "is_fraud").toPandas()

X = pdf[FEATURES]
y = pdf["is_fraud"]
# Stratify: fraud is the rare class, so preserve its proportion across the split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y)


# Wrap the classifier so the endpoint returns the fraud probability, not a 0/1 label.
# A plain sklearn flavor serves model.predict(), which for GradientBoostingClassifier
# returns class labels; a fraud service wants the score.
class FraudProbabilityModel(mlflow.pyfunc.PythonModel):
    def __init__(self, clf):
        self.clf = clf

    def predict(self, context, model_input):
        return self.clf.predict_proba(model_input[FEATURES])[:, 1]


with mlflow.start_run(run_name="fraud_detection_gbt") as run:
    clf = GradientBoostingClassifier(n_estimators=200, max_depth=3, learning_rate=0.1, random_state=42)
    clf.fit(X_train, y_train)

    proba = clf.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)
    ap  = average_precision_score(y_test, proba)   # PR-AUC, more informative on imbalanced fraud data
    mlflow.log_metric("test_auc", auc)
    mlflow.log_metric("test_pr_auc", ap)
    print(f"Test ROC-AUC: {auc:.3f} | PR-AUC: {ap:.3f} | fraud rate: {y.mean():.1%}")

    # Signature reflects the actual served output: one fraud probability per row.
    fraud_model = FraudProbabilityModel(clf)
    example_out = fraud_model.predict(None, X_train.head(3))
    signature = infer_signature(X_train, example_out)
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=fraud_model,
        signature=signature,
        input_example=X_train.head(3),
        registered_model_name=MODEL_NAME,
    )

print("Model registered to Unity Catalog:", MODEL_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Real-time model serving endpoint
# MAGIC
# MAGIC Serve the registered model with single-digit-millisecond inference. The create-or-update
# MAGIC pattern makes re-runs safe: if the endpoint already exists we update its config instead of
# MAGIC failing. `scale_to_zero_enabled=False` keeps the provider endpoint always-on.

# COMMAND ----------

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.tracking import MlflowClient

# UC model serving requires a concrete model version. Resolve the latest one.
# (mlflow registry URI was set to "databricks-uc" in Step 4.)
model_version = str(max(int(v.version) for v in MlflowClient().search_model_versions(f"name='{MODEL_NAME}'")))

config = EndpointCoreConfigInput(
    name=PROVIDER_ENDPOINT_NAME,   # required by EndpointCoreConfigInput in current SDK
    served_entities=[
        ServedEntityInput(
            entity_name=MODEL_NAME,
            entity_version=model_version,   # required for UC-registered models
            scale_to_zero_enabled=False,
            workload_size="Small",
        )
    ]
)
try:
    w.serving_endpoints.create_and_wait(name=PROVIDER_ENDPOINT_NAME, config=config)
except Exception as e:
    if "exist" in str(e).lower():
        w.serving_endpoints.update_config_and_wait(
            name=PROVIDER_ENDPOINT_NAME, served_entities=config.served_entities)
    else:
        raise
print("Serving endpoint ready:", PROVIDER_ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC
# MAGIC Smoke-test the endpoint with a few sample rows. **Validation note:** in a live run this
# MAGIC returns fraud probabilities per record; you can also query from a terminal:
# MAGIC ```bash
# MAGIC curl -s -X POST "https://<HOST>/serving-endpoints/sample-online-fs-endpoint/invocations" \
# MAGIC   -H "Authorization: Bearer <TOKEN>" -H "Content-Type: application/json" \
# MAGIC   -d '{"dataframe_records": [{"account_age_days": 10, "num_txns_24h": 18, "avg_amount_30d": 40.0, "last_txn_amount": 350.0, "num_countries_7d": 4, "merchant_risk_score": 0.9, "failed_auth_24h": 3, "amount_to_avg_ratio": 8.75}]}'
# MAGIC ```

# COMMAND ----------

sample = spark.table(FULL_TABLE_NAME).select(*FEATURES).limit(3).toPandas().to_dict("records")
print(w.serving_endpoints.query(name=PROVIDER_ENDPOINT_NAME, dataframe_records=sample).as_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC
# MAGIC Uncomment and run to tear down everything this notebook created, in dependency order.
# MAGIC Safe to re-run (missing resources are skipped). Does **not** drop the offline feature table.

# COMMAND ----------

# def _try(label, fn):
#     try:
#         fn(); print("deleted:", label)
#     except Exception as e:
#         print("skipped:", label, "->", str(e).splitlines()[0][:120])
#
# _try(f"serving endpoint {PROVIDER_ENDPOINT_NAME}",
#      lambda: w.serving_endpoints.delete(name=PROVIDER_ENDPOINT_NAME))
# _try(f"synced table {SYNCED_TABLE}",
#      lambda: w.postgres.delete_synced_table(name=f"synced_tables/{SYNCED_TABLE}"))
# # purge=True hard-deletes; without it the instance is soft-deleted (recoverable for 7 days)
# _try(f"lakebase project {PROVIDER_PROJECT_ID}",
#      lambda: w.postgres.delete_project(name=f"projects/{PROVIDER_PROJECT_ID}", purge=True))
# print("Cleanup complete. (Offline table", FULL_TABLE_NAME, "left in place.)")
