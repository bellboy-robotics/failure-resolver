resource "aws_cloudwatch_log_group" "service" {
  name              = "/ecs/${local.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_metric_alarm" "service_not_running" {
  count = var.desired_count == 1 ? 1 : 0

  alarm_name          = "${local.name}-service-not-running"
  alarm_description   = "The failure resolver observer singleton has no running ECS task."
  namespace           = "ECS/ContainerInsights"
  metric_name         = "RunningTaskCount"
  statistic           = "Minimum"
  period              = 60
  evaluation_periods  = 2
  datapoints_to_alarm = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = {
    ClusterName = aws_ecs_cluster.service.name
    ServiceName = aws_ecs_service.service.name
  }

  alarm_actions             = var.alarm_sns_topic_arns
  ok_actions                = var.alarm_sns_topic_arns
  insufficient_data_actions = []
}
