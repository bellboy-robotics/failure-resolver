resource "aws_secretsmanager_secret" "runtime" {
  name                    = local.runtime_secret_name
  description             = "Supabase, OpenAI, GitHub, and optional Cloudflare Access credentials for ${local.name}"
  recovery_window_in_days = var.secret_recovery_window_days
}

# Secret values are intentionally not managed by Terraform. Populate a JSON
# object with keys "supabase_service_role_key", "openai_api_key", and
# "github_token" out of band so credentials never enter Terraform
# configuration or state. When resolver_auto_execute=true, also include
# "recovery_cf_access_client_id" and "recovery_cf_access_client_secret".
