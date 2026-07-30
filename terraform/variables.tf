variable "aws_region" {
  description = "AWS region in which the resolver observer runs."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = length(trimspace(var.aws_region)) > 0
    error_message = "aws_region must not be empty."
  }
}

variable "allowed_account_id" {
  description = "Exact AWS account allowed for this PoC; prevents accidental deployment into another account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.allowed_account_id))
    error_message = "allowed_account_id must be a 12-digit AWS account ID."
  }
}

variable "project_name" {
  description = "Lowercase name used for AWS resources."
  type        = string
  default     = "failure-resolver"

  validation {
    condition = (
      length(var.project_name) >= 3 &&
      length(var.project_name) <= 24 &&
      can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.project_name))
    )
    error_message = "project_name must be 3-24 lowercase alphanumeric or hyphen characters."
  }
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "poc"

  validation {
    condition = (
      length(var.environment) >= 2 &&
      length(var.environment) <= 12 &&
      can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.environment))
    )
    error_message = "environment must be 2-12 lowercase alphanumeric or hyphen characters."
  }
}

variable "vpc_cidr" {
  description = "Network-aligned /16 through /20 IPv4 CIDR for the isolated resolver PoC VPC."
  type        = string
  default     = "10.43.0.0/16"

  validation {
    condition = (
      can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/(1[6-9]|20)$", var.vpc_cidr)) &&
      try(cidrhost(var.vpc_cidr, 0) == split("/", var.vpc_cidr)[0], false)
    )
    error_message = "vpc_cidr must be a network-aligned IPv4 CIDR with a /16 through /20 prefix."
  }
}

variable "desired_count" {
  description = "Singleton observer count. Keep at zero until an immutable image digest and runtime secret value exist."
  type        = number
  default     = 0

  validation {
    condition     = contains([0, 1], var.desired_count)
    error_message = "desired_count must be 0 or 1."
  }
}

variable "supabase_url" {
  description = "HTTPS URL of the Supabase project containing public.failure_events."
  type        = string

  validation {
    condition     = can(regex("^https://[^[:space:]]+$", var.supabase_url))
    error_message = "supabase_url must be an HTTPS URL."
  }
}

variable "failure_events_table" {
  description = "Supabase table observed by the resolver."
  type        = string
  default     = "failure_events"

  validation {
    condition     = can(regex("^[a-z_][a-z0-9_]*$", var.failure_events_table))
    error_message = "failure_events_table must be a lowercase PostgreSQL identifier."
  }
}

variable "openai_model" {
  description = "OpenAI model used for memory generalization and failure matching."
  type        = string
  default     = "gpt-5.6-luna"

  validation {
    condition     = length(trimspace(var.openai_model)) > 0
    error_message = "openai_model must not be empty."
  }
}

variable "resolver_auto_execute" {
  description = "Opt in to bounded automatic recovery execution. Keep false until the Brain supports guarded $resume_flow and the robot allowlist is reviewed."
  type        = bool
  default     = false
}

variable "recovery_robot_allowlist" {
  description = "Exact robot sysids allowed to receive automatic recovery commands. Required when resolver_auto_execute is true."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for sysid in var.recovery_robot_allowlist :
      can(regex("^[A-Za-z0-9-]{1,64}$", trimspace(sysid)))
    ])
    error_message = "recovery_robot_allowlist entries must be 1-64 alphanumeric or hyphen characters."
  }
}

variable "recovery_max_attempts" {
  description = "Immutable attempt budget assigned when an automatic recovery session is prepared."
  type        = number
  default     = 3

  validation {
    condition = (
      floor(var.recovery_max_attempts) == var.recovery_max_attempts &&
      var.recovery_max_attempts >= 1 &&
      var.recovery_max_attempts <= 20
    )
    error_message = "recovery_max_attempts must be an integer between 1 and 20."
  }
}

variable "recovery_command_timeout_seconds" {
  description = "Maximum time to wait for one correlated robot command response."
  type        = number
  default     = 15

  validation {
    condition     = var.recovery_command_timeout_seconds > 0
    error_message = "recovery_command_timeout_seconds must be greater than zero."
  }
}

variable "recovery_outcome_timeout_seconds" {
  description = "Maximum time to observe fresh Flow state after the guarded continuation is accepted."
  type        = number
  default     = 60

  validation {
    condition     = var.recovery_outcome_timeout_seconds > 0
    error_message = "recovery_outcome_timeout_seconds must be greater than zero."
  }
}

variable "recovery_lease_seconds" {
  description = "Database lease for one claimed recovery attempt; an expired ambiguous attempt becomes terminal unknown."
  type        = number
  default     = 300

  validation {
    condition = (
      floor(var.recovery_lease_seconds) == var.recovery_lease_seconds &&
      var.recovery_lease_seconds >= 5 &&
      var.recovery_lease_seconds <= 900
    )
    error_message = "recovery_lease_seconds must be an integer between 5 and 900."
  }
}

variable "recovery_reconcile_interval_seconds" {
  description = "Seconds between bounded automatic recovery reconciliation passes."
  type        = number
  default     = 30

  validation {
    condition = (
      var.recovery_reconcile_interval_seconds > 0 &&
      var.recovery_reconcile_interval_seconds < var.recovery_lease_seconds
    )
    error_message = "recovery_reconcile_interval_seconds must be greater than zero and shorter than recovery_lease_seconds."
  }
}

variable "memory_repo_url" {
  description = "HTTPS Git repository that is the Markdown resolution-memory ground truth."
  type        = string
  default     = "https://github.com/bellboy-robotics/failure-resolver-memories.git"

  validation {
    condition     = can(regex("^https://github\\.com/[^/]+/[^/]+(?:\\.git)?$", trimspace(var.memory_repo_url)))
    error_message = "memory_repo_url must be an HTTPS GitHub repository URL."
  }
}

variable "memory_repo_branch" {
  description = "Branch for production-site memories (every site except 0)."
  type        = string
  default     = "main"

  validation {
    condition = (
      can(regex("^[A-Za-z0-9][A-Za-z0-9._/-]*$", trimspace(var.memory_repo_branch))) &&
      !strcontains(var.memory_repo_branch, "..") &&
      !strcontains(var.memory_repo_branch, "@{")
    )
    error_message = "memory_repo_branch must be a safe Git branch name."
  }
}

variable "memory_repo_dev_branch" {
  description = "Branch for dev-site memories (site_id 0 or unlocated)."
  type        = string
  default     = "failure-resolver-dev"

  validation {
    condition = (
      can(regex("^[A-Za-z0-9][A-Za-z0-9._/-]*$", trimspace(var.memory_repo_dev_branch))) &&
      !strcontains(var.memory_repo_dev_branch, "..") &&
      !strcontains(var.memory_repo_dev_branch, "@{")
    )
    error_message = "memory_repo_dev_branch must be a safe Git branch name."
  }
}

variable "container_port" {
  description = "Internal health server port; no public listener is created."
  type        = number
  default     = 8000

  validation {
    condition     = var.container_port >= 1 && var.container_port <= 65535
    error_message = "container_port must be a valid TCP port."
  }
}

variable "task_cpu" {
  description = "Fargate task CPU units."
  type        = number
  default     = 256
}

variable "task_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 512
}

variable "cpu_architecture" {
  description = "Container CPU architecture."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "image_digest" {
  description = "Immutable ECR digest in sha256:<64 hex> form. Required when desired_count is 1."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = (
      var.image_digest == null ||
      can(regex("^sha256:[0-9a-f]{64}$", var.image_digest))
    )
    error_message = "image_digest must be null or sha256 followed by 64 lowercase hex characters."
  }
}

variable "runtime_secret_name" {
  description = "Optional Secrets Manager name override."
  type        = string
  default     = null
  nullable    = true
}

variable "secret_recovery_window_days" {
  description = "Secrets Manager recovery window."
  type        = number
  default     = 7

  validation {
    condition = (
      var.secret_recovery_window_days >= 7 &&
      var.secret_recovery_window_days <= 30
    )
    error_message = "secret_recovery_window_days must be between 7 and 30."
  }
}

variable "log_retention_days" {
  description = "CloudWatch log retention."
  type        = number
  default     = 14

  validation {
    condition = contains(
      [1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653],
      var.log_retention_days,
    )
    error_message = "log_retention_days must be a value supported by CloudWatch Logs."
  }
}

variable "alarm_sns_topic_arns" {
  description = "Optional SNS topic ARNs notified when the singleton task is not running."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for arn in var.alarm_sns_topic_arns :
      can(regex("^arn:[^:]+:sns:[^:]+:[0-9]{12}:[^:]+$", arn))
    ])
    error_message = "alarm_sns_topic_arns must contain valid SNS topic ARNs."
  }
}

variable "tags" {
  description = "Additional tags applied to all supported resources."
  type        = map(string)
  default     = {}
}
