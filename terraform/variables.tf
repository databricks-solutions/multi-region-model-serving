variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID (from the account console)."
}

variable "aws_region" {
  type        = string
  description = "AWS region for the recipient (Workspace B) and its metastore."
  default     = "us-west-2"
}

variable "workspace_name" {
  type        = string
  description = "Display name for the serverless recipient workspace (Workspace B)."
  default     = "multi-region-serving-recipient"
}

variable "metastore_name" {
  type        = string
  description = "Name for the recipient Unity Catalog metastore (Metastore B)."
  default     = "metastore-b-us-west-2"
}
