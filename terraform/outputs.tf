output "aws_account_id" {
  description = "AWS account receiving the stack."
  value       = data.aws_caller_identity.current.account_id
}

output "ecr_repository_url" {
  description = "Repository to which the observer image must be pushed."
  value       = aws_ecr_repository.service.repository_url
}

output "image_reference" {
  description = "Image reference registered in the current task definition."
  value       = local.image_reference
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.service.name
}

output "ecs_service_name" {
  value = aws_ecs_service.service.name
}

output "runtime_secret_arn" {
  description = "Populate this secret out of band; Terraform manages no secret value."
  value       = aws_secretsmanager_secret.runtime.arn
}

output "cloudwatch_log_group" {
  value = aws_cloudwatch_log_group.service.name
}

output "security_group_id" {
  value = aws_security_group.service.id
}

output "vpc_id" {
  description = "Dedicated PoC VPC with no production-network attachment."
  value       = aws_vpc.service.id
}

output "public_subnet_ids" {
  value = [for subnet in aws_subnet.public : subnet.id]
}
