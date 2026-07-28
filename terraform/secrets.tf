resource "aws_secretsmanager_secret" "runtime" {
  name                    = local.runtime_secret_name
  description             = "Supabase credential for ${local.name}"
  recovery_window_in_days = var.secret_recovery_window_days
}

# Secret values are intentionally not managed by Terraform. Populate a JSON
# object with key "supabase_service_role_key" out of band so the credential
# never enters Terraform configuration or state.
