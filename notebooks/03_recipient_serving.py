# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · In-Region Serving (Recipient, Workspace B, serverless)
# MAGIC
# MAGIC Run this in the **recipient** serverless workspace (e.g. `us-west-2`). It stands up a local
# MAGIC serving stack that reads the shared assets from Part 2:
# MAGIC
# MAGIC 1. Lakebase Autoscaling instance in this region (Step 9a, same as Step 2)
# MAGIC 2. Synced table sourced from the **Delta-Shared** offline table (Step 9b)
# MAGIC 3. Model serving endpoint serving the **Delta-Shared** model (Step 10)
# MAGIC
# MAGIC > **Prerequisites:** `02_delta_sharing` has mounted `SHARED_CATALOG` in this workspace and the
# MAGIC > shared table + model are visible. Feature lookups at inference time resolve against the
# MAGIC > *local* Lakebase instance created here, so serving stays in-region.

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
# MAGIC ## Step 9a · Lakebase Autoscaling instance in the recipient region
# MAGIC
# MAGIC Same call as Step 2 in the provider notebook: provision a serverless Postgres instance with
# MAGIC the same autoscaling, HA node, and readable-secondary options, here in the recipient region.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectDefaultEndpointSettings,
    InitialEndpointSpec, EndpointGroupSpec,
)

RECIPIENT_BRANCH = f"projects/{RECIPIENT_PROJECT_ID}/branches/production"

create_ignoring_exists(lambda: w.postgres.create_project(
    project_id=RECIPIENT_PROJECT_ID,
    project=Project(
        spec=ProjectSpec(
            display_name=RECIPIENT_PROJECT_ID,
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
    lambda: list(w.postgres.list_endpoints(parent=RECIPIENT_BRANCH)),
    lambda eps: bool(eps) and all(str(e.status.current_state).endswith("ACTIVE") for e in eps),
)
print("Recipient Lakebase instance ready:", RECIPIENT_PROJECT_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9b · Synced table sourced from the Delta Share
# MAGIC
# MAGIC The only change from Step 3: the sync source is the **Delta-Shared** offline table
# MAGIC (`FULL_SHARED_TABLE_NAME`), not a local table. No copy of the offline table is maintained in
# MAGIC this region. The online store syncs continuously from the shared table in the provider region.

# COMMAND ----------

from databricks.sdk.service.postgres import (
    SyncedTable, SyncedTableSyncedTableSpec, NewPipelineSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedulingPolicy,
)

RECIPIENT_SYNCED_TABLE = f"{RECIPIENT_CATALOG}.{RECIPIENT_SCHEMA}.{TABLE_NAME}_online"

create_ignoring_exists(lambda: w.postgres.create_synced_table(
    synced_table_id=RECIPIENT_SYNCED_TABLE,
    synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
        branch=RECIPIENT_BRANCH,
        postgres_database=RECIPIENT_CATALOG,
        source_table_full_name=FULL_SHARED_TABLE_NAME,   # <-- the Delta Share, not a local table
        scheduling_policy=SchedulingPolicy.CONTINUOUS,
        primary_key_columns=PK_COLUMN,
        create_database_objects_if_missing=True,
        accelerated_sync=True,
        new_pipeline_spec=NewPipelineSpec(
            storage_catalog=RECIPIENT_CATALOG,
            storage_schema=RECIPIENT_SCHEMA,
        ),
    )),
))

wait_for(
    "synced table ONLINE",
    lambda: w.postgres.get_synced_table(name=f"synced_tables/{RECIPIENT_SYNCED_TABLE}"),
    synced_table_ready,
)
print("Recipient online store syncing from Delta Share:", RECIPIENT_SYNCED_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 · Model serving endpoint serving the shared model
# MAGIC
# MAGIC Same create-or-update pattern as Step 5, but `entity_name` points at the **Delta-Shared**
# MAGIC model. `scale_to_zero_enabled=True` is reasonable for a secondary region with lighter traffic.

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# UC model serving requires a concrete model version. Resolve the latest version of the
# Delta-Shared model as seen from this recipient metastore.
mlflow.set_registry_uri("databricks-uc")
shared_model_version = str(max(int(v.version) for v in MlflowClient().search_model_versions(f"name='{SHARED_MODEL_NAME}'")))

config = EndpointCoreConfigInput(
    name=RECIPIENT_ENDPOINT_NAME,   # required by EndpointCoreConfigInput in current SDK
    served_entities=[
        ServedEntityInput(
            entity_name=SHARED_MODEL_NAME,   # <-- Delta Shared model
            entity_version=shared_model_version,   # required for UC-registered models
            scale_to_zero_enabled=True,
            workload_size="Small",
        )
    ]
)
try:
    w.serving_endpoints.create_and_wait(name=RECIPIENT_ENDPOINT_NAME, config=config)
except Exception as e:
    if "exist" in str(e).lower():
        w.serving_endpoints.update_config_and_wait(
            name=RECIPIENT_ENDPOINT_NAME, served_entities=config.served_entities)
    else:
        raise
print("Recipient serving endpoint ready:", RECIPIENT_ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify (in-region)
# MAGIC
# MAGIC **Validation note:** this query should return the same predictions as the provider endpoint,
# MAGIC but served from the recipient region with feature lookups against the local Lakebase instance.

# COMMAND ----------

sample = spark.table(FULL_SHARED_TABLE_NAME).select(*FEATURES).limit(3).toPandas().to_dict("records")
print(w.serving_endpoints.query(name=RECIPIENT_ENDPOINT_NAME, dataframe_records=sample).as_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 Cleanup (optional)
# MAGIC
# MAGIC Tears down what this notebook created. The shared catalog itself is torn down in
# MAGIC `02_delta_sharing`'s cleanup, not here.

# COMMAND ----------

# def _try(label, fn):
#     try:
#         fn(); print("deleted:", label)
#     except Exception as e:
#         print("skipped:", label, "->", str(e).splitlines()[0][:120])
#
# _try(f"serving endpoint {RECIPIENT_ENDPOINT_NAME}",
#      lambda: w.serving_endpoints.delete(name=RECIPIENT_ENDPOINT_NAME))
# _try(f"synced table {RECIPIENT_SYNCED_TABLE}",
#      lambda: w.postgres.delete_synced_table(name=f"synced_tables/{RECIPIENT_SYNCED_TABLE}"))
# _try(f"lakebase project {RECIPIENT_PROJECT_ID}",
#      lambda: w.postgres.delete_project(name=f"projects/{RECIPIENT_PROJECT_ID}", purge=True))
# print("Recipient cleanup complete.")
