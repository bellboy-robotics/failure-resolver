resource "aws_ecs_cluster" "service" {
  name = local.name

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_task_definition" "service" {
  family                   = local.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = tostring(var.task_cpu)
  memory                   = tostring(var.task_memory)
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  volume {
    name = "memory-checkout"
  }

  container_definitions = jsonencode([
    {
      name                   = "memory-volume-init"
      image                  = local.image_reference
      essential              = false
      readonlyRootFilesystem = true
      user                   = "0:0"
      command = [
        "sh",
        "-c",
        "chown 10001:10001 /var/lib/failure-resolver",
      ]
      mountPoints = [
        {
          sourceVolume  = "memory-checkout"
          containerPath = "/var/lib/failure-resolver"
          readOnly      = false
        },
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.service.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "memory-init"
        }
      }
    },
    {
      name                   = "failure-resolver"
      image                  = local.image_reference
      essential              = true
      readonlyRootFilesystem = true
      user                   = "10001:10001"
      workingDirectory       = "/app"
      stopTimeout            = 30

      dependsOn = [
        {
          containerName = "memory-volume-init"
          condition     = "SUCCESS"
        },
      ]

      linuxParameters = {
        initProcessEnabled = true
      }

      environment = local.runtime_environment

      mountPoints = [
        {
          sourceVolume  = "memory-checkout"
          containerPath = "/var/lib/failure-resolver"
          readOnly      = false
        },
      ]

      secrets = [
        {
          name      = "SUPABASE_SERVICE_ROLE_KEY"
          valueFrom = "${aws_secretsmanager_secret.runtime.arn}:supabase_service_role_key::"
        },
        {
          name      = "OPENAI_API_KEY"
          valueFrom = "${aws_secretsmanager_secret.runtime.arn}:openai_api_key::"
        },
        {
          name      = "GITHUB_TOKEN"
          valueFrom = "${aws_secretsmanager_secret.runtime.arn}:github_token::"
        },
      ]

      healthCheck = {
        command = [
          "CMD-SHELL",
          "python -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:${var.container_port}/health', timeout=3)\"",
        ]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 20
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.service.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "resolver"
        }
      }
    },
  ])

  lifecycle {
    precondition {
      condition = contains(
        lookup(local.valid_fargate_memory_by_cpu, tostring(var.task_cpu), []),
        var.task_memory,
      )
      error_message = "task_cpu and task_memory are not a supported Fargate combination."
    }
  }
}

resource "aws_ecs_service" "service" {
  name             = local.name
  cluster          = aws_ecs_cluster.service.id
  task_definition  = aws_ecs_task_definition.service.arn
  desired_count    = var.desired_count
  launch_type      = "FARGATE"
  platform_version = "1.4.0"

  enable_ecs_managed_tags = true
  enable_execute_command  = false
  propagate_tags          = "SERVICE"

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  network_configuration {
    assign_public_ip = true
    security_groups  = [aws_security_group.service.id]
    subnets          = [for subnet in aws_subnet.public : subnet.id]
  }

  lifecycle {
    precondition {
      condition     = var.desired_count == 0 || var.image_digest != null
      error_message = "image_digest is required before desired_count can be 1."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.execution_managed,
    aws_iam_role_policy.execution_secret,
    aws_route_table_association.public,
  ]
}
