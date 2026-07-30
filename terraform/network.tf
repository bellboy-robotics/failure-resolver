resource "aws_vpc" "service" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = local.name
  }
}

resource "aws_internet_gateway" "service" {
  vpc_id = aws_vpc.service.id

  tags = {
    Name = local.name
  }
}

resource "aws_subnet" "public" {
  for_each = toset(local.availability_zones)

  vpc_id                  = aws_vpc.service.id
  availability_zone       = each.value
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, index(local.availability_zones, each.value))
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.name}-${each.value}"
    Tier = "public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.service.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.service.id
  }

  tags = {
    Name = "${local.name}-public"
  }
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

resource "aws_default_security_group" "default" {
  vpc_id = aws_vpc.service.id

  # Intentionally no ingress or egress. The ECS task has its own group.
}

resource "aws_security_group" "service" {
  name_prefix = "${local.name}-"
  description = "Outbound-only networking for ${local.name}"
  vpc_id      = aws_vpc.service.id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_egress_rule" "https_ipv4" {
  security_group_id = aws_security_group.service.id
  description       = "TLS and WSS to Supabase plus AWS control-plane endpoints"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}
