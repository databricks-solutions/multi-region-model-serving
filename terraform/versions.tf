terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source = "databricks/databricks"
      # Serverless workspaces (compute_mode = "SERVERLESS") require a recent provider.
      version = ">= 1.80.0"
    }
  }
}
