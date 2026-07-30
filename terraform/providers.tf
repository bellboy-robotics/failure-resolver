provider "aws" {
  region              = var.aws_region
  allowed_account_ids = [var.allowed_account_id]

  default_tags {
    tags = local.common_tags
  }
}

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}
