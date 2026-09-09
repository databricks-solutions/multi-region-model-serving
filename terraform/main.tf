# Serverless recipient workspace (Workspace B).
#
# A serverless workspace needs NO customer-managed networking or storage: we set
# compute_mode = "SERVERLESS" and deliberately OMIT credentials_id, storage_configuration_id,
# and network_id. Databricks manages all compute and networking.
resource "databricks_mws_workspaces" "recipient" {
  provider       = databricks.mws
  account_id     = var.databricks_account_id
  workspace_name = var.workspace_name
  aws_region     = var.aws_region

  compute_mode = "SERVERLESS"
}

# Recipient Unity Catalog metastore (Metastore B), in the same region.
resource "databricks_metastore" "b" {
  provider      = databricks.mws
  name          = var.metastore_name
  region        = var.aws_region
  force_destroy = true
}

# Assign Metastore B to the serverless recipient workspace.
resource "databricks_metastore_assignment" "b" {
  provider     = databricks.mws
  workspace_id = databricks_mws_workspaces.recipient.workspace_id
  metastore_id = databricks_metastore.b.id
}
