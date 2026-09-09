output "workspace_id" {
  description = "ID of the serverless recipient workspace (Workspace B)."
  value       = databricks_mws_workspaces.recipient.workspace_id
}

output "workspace_url" {
  description = "URL of Workspace B — authenticate the recipient notebooks against this host."
  value       = databricks_mws_workspaces.recipient.workspace_url
}

output "metastore_id" {
  description = "ID of Metastore B."
  value       = databricks_metastore.b.id
}

# The recipient's global sharing identifier is what the PROVIDER uses in
# `CREATE RECIPIENT ... USING ID '<identifier>'`. It has the form
# "<cloud>:<region>:<metastore-uuid>", e.g. "aws:us-west-2:<uuid>".
# Read it in Workspace B with:  SELECT current_metastore();
# or from the account console (Metastore B > Details). Copy it into
# _config.RECIPIENT_SHARING_IDENTIFIER before running 02_delta_sharing.
output "recipient_sharing_identifier_hint" {
  description = "How to obtain Metastore B's global sharing identifier for CREATE RECIPIENT."
  value       = "Run 'SELECT current_metastore();' in Workspace B, or see Metastore B details in the account console."
}
