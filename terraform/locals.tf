locals {
  name = "${var.project_name}-${var.environment}"

  common_tags = merge(
    {
      Application = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Purpose     = "proof-of-concept"
    },
    var.tags,
  )

  runtime_secret_name = coalesce(
    var.runtime_secret_name,
    "/${local.name}/runtime",
  )

  availability_zones = slice(
    data.aws_availability_zones.available.names,
    0,
    2,
  )

  image_reference = var.image_digest == null ? (
    "${aws_ecr_repository.service.repository_url}:bootstrap"
    ) : (
    "${aws_ecr_repository.service.repository_url}@${var.image_digest}"
  )

  runtime_environment = [
    {
      name  = "FAILURE_EVENTS_TABLE"
      value = var.failure_events_table
    },
    {
      name  = "LOG_LEVEL"
      value = "INFO"
    },
    {
      name  = "PORT"
      value = tostring(var.container_port)
    },
    {
      name  = "RESOLVER_MODE"
      value = "observe"
    },
    {
      name  = "SUPABASE_URL"
      value = var.supabase_url
    },
  ]

  valid_fargate_memory_by_cpu = {
    "256"  = [512, 1024, 2048]
    "512"  = [1024, 2048, 3072, 4096]
    "1024" = [2048, 3072, 4096, 5120, 6144, 7168, 8192]
  }
}
