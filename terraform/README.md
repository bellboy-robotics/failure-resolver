# Observe-only AWS deployment

This Terraform root runs the lightweight failure resolver observer as one
outbound-only ECS Fargate task. It subscribes directly to Supabase Realtime
changes on `public.failure_events`.

It intentionally creates no SQS queue, API Gateway, load balancer, public
listener, NAT gateway, Qdrant service, or robot permissions. The first
deployment only proves that the resolver sees persisted failures; it does not
run the matcher, call an LLM, update Supabase, or execute a recovery.

## Resources

- Dedicated PoC VPC, Internet Gateway, and two public subnets.
- Security group with no ingress and outbound TCP/443 only.
- ECR repository with immutable tags and scan-on-push.
- ECS Fargate cluster, task definition, and singleton service.
- CloudWatch log group and an optional singleton availability alarm.
- Empty task role and an execution role limited to image/log operations plus
  reading one Secrets Manager secret.
- Secrets Manager secret metadata only. Terraform never manages its value.

The task receives a public IP only for outbound Supabase HTTPS/WSS and AWS
control-plane access. The VPC has no peering or route to a production VPC.

## 1. Configure

```bash
cp backend.tf.example backend.tf
cp terraform.tfvars.example terraform.tfvars
# Set the exact AWS account ID and Supabase project URL.
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan
terraform apply
```

Leave `desired_count = 0` and `image_digest = null` on the first apply.

Populate the created secret out of band with this JSON shape:

```json
{"supabase_service_role_key":"replace-me"}
```

For example, pipe a locally constructed JSON document to
`aws secretsmanager put-secret-value --secret-id "$(terraform output -raw runtime_secret_arn)" --secret-string file:///path/to/secret.json`.
Do not put the service-role key in a `.tfvars` file, shell history, Terraform
state, source control, or the container image.

The service-role key is a PoC bootstrap credential because the existing RLS
policy only grants browser reads to authenticated Bellboy users. Replace it
with a narrower machine identity before the resolver gains write or recovery
capabilities.

## 2. Build and push

```bash
ECR_REPOSITORY="$(terraform output -raw ecr_repository_url)"
IMAGE_TAG="$(git -C .. rev-parse --short=12 HEAD)"

aws ecr get-login-password --region us-east-1 \
  | docker login --username AWS --password-stdin "${ECR_REPOSITORY%%/*}"

docker buildx build \
  --platform linux/amd64 \
  --tag "${ECR_REPOSITORY}:${IMAGE_TAG}" \
  --push \
  ..

IMAGE_DIGEST="$(aws ecr describe-images \
  --region us-east-1 \
  --repository-name "$(basename "${ECR_REPOSITORY}")" \
  --image-ids imageTag="${IMAGE_TAG}" \
  --query 'imageDetails[0].imageDigest' \
  --output text)"
```

Set that immutable digest and start the singleton:

```hcl
desired_count = 1
image_digest  = "sha256:..."
```

Then run `terraform plan && terraform apply`.

## 3. Verify

The local container health endpoint stays live even while Realtime reconnects.
Readiness reports whether the Supabase subscription is active:

```text
GET /health
GET /readyz
```

In AWS, inspect the ECS service and CloudWatch logs:

```bash
aws ecs describe-services \
  --cluster "$(terraform output -raw ecs_cluster_name)" \
  --services "$(terraform output -raw ecs_service_name)"

aws logs tail "$(terraform output -raw cloudwatch_log_group)" --follow
```

An insert or enrichment update to `public.failure_events` should produce a log
containing only safe routing fields such as `failure_id`, `sysid`, `flow_id`,
and `matcher_status`. Secret values, failure narratives, robot errors, images,
and action arguments must not be logged.

Supabase Realtime is a notification path, not a durable work queue. A later
matcher implementation must add database-backed claiming and startup catch-up
before this can process failures exactly once across disconnects.
