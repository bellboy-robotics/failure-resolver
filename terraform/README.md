# Failure resolver agent AWS deployment

This Terraform root runs `resolver.py` as one outbound-only ECS Fargate task.
It subscribes directly to Supabase Realtime changes on
`public.failure_events` and `public.flow_failure_resolutions`, reconciles
durable database state on startup, calls OpenAI for bounded reasoning, and
reads/writes Git-backed Markdown recovery memory.

It intentionally creates no SQS queue, API Gateway, load balancer, public
listener, NAT gateway, Qdrant service, or robot permissions. The agent updates
matcher state in Supabase and commits memory, but it does not execute a
physical recovery.

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
{
  "supabase_service_role_key": "replace-me",
  "openai_api_key": "replace-me",
  "github_token": "replace-me"
}
```

For example, pipe a locally constructed JSON document to
`aws secretsmanager put-secret-value --secret-id "$(terraform output -raw runtime_secret_arn)" --secret-string file:///path/to/secret.json`.
Do not put any of these values in a `.tfvars` file, shell history, Terraform
state, source control, or the container image. The GitHub token should be
fine-grained and limited to read/write access on the configured memory
repository.

The service-role key is a PoC bootstrap credential because the existing RLS
policy only grants browser reads to authenticated Bellboy users. Replace it
with a narrower machine identity after the PoC.

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

An insert into `public.failure_events` should advance its matcher status. A
successful, applied resolution in `public.flow_failure_resolutions` should
produce or update one Markdown memory commit. Supabase Realtime is only the
notification path; startup reconciliation covers events missed while the
service was disconnected.
