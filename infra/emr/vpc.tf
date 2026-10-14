data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "tag:Name"
    values = ["subnet-private-a"]
  }
}

data "aws_security_group" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "group-name"
    values = ["default"]
  }
}

resource "aws_security_group" "master" {
  name        = "emr-master-flight-radar-prod"
  description = "EMR Master security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "core" {
  name        = "emr-core-flight-radar-prod"
  description = "EMR Core/Task security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "service" {
  name        = "emr-service-flight-radar-prod"
  description = "EMR Service security group"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags

  lifecycle {
    create_before_destroy = true
  }
}

# Ingress rules (separate to avoid cycles)
resource "aws_security_group_rule" "master_ingress_core" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.master.id
  source_security_group_id = aws_security_group.core.id
}

resource "aws_security_group_rule" "master_ingress_service" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.master.id
  source_security_group_id = aws_security_group.service.id
}

resource "aws_security_group_rule" "core_ingress_master" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.core.id
  source_security_group_id = aws_security_group.master.id
}

resource "aws_security_group_rule" "core_ingress_self" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.core.id
  source_security_group_id = aws_security_group.core.id
}

resource "aws_security_group_rule" "service_ingress_master" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.service.id
  source_security_group_id = aws_security_group.master.id
}

# Use the specific private subnet (subnet-private-a in us-east-1a)
locals {
  private_subnet_id = data.aws_subnet.private.id
}