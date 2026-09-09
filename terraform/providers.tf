# Account-level Databricks provider. Workspace + metastore creation are ACCOUNT APIs, so the
# provider points at accounts.cloud.databricks.com (not a workspace host).
#
# Authenticate with a service principal (recommended) or an account admin token. Simplest is env vars:
#   export DATABRICKS_ACCOUNT_ID=<your-account-id>
#   export DATABRICKS_CLIENT_ID=<service-principal-id>
#   export DATABRICKS_CLIENT_SECRET=<service-principal-secret>
# (or DATABRICKS_TOKEN for an account admin PAT)
provider "databricks" {
  alias      = "mws"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}
