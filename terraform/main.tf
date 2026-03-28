terraform {
  required_version = ">= 1.3.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(var.tags, {
      Project     = var.project_name
      Environment = var.environment
    })
  }
}

locals {
  name_prefix    = "${var.project_name}-${var.environment}"
  s3_bucket_name = var.s3_bucket_name != "" ? var.s3_bucket_name : "${local.name_prefix}-docs-${data.aws_caller_identity.current.account_id}"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
  filter {
    name   = "state"
    values = ["available"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_security_group" "mysql" {
  name_prefix = "${local.name_prefix}-mysql-"
  description = "Security group for MySQL EC2 instance - Openflow CDC"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "MySQL from allowed CIDRs (Openflow SPCS egress)"
    from_port   = 3306
    to_port     = 3306
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_iam_role" "ec2_ssm" {
  name_prefix = "${local.name_prefix}-ssm-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ssm_managed" {
  role       = aws_iam_role.ec2_ssm.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2_ssm" {
  name_prefix = "${local.name_prefix}-ssm-"
  role        = aws_iam_role.ec2_ssm.name
}

resource "aws_instance" "mysql" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.mysql.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_ssm.name

  associate_public_ip_address = true

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/scripts/user_data.sh.tpl", {
    mysql_root_password = var.mysql_root_password
    mysql_cdc_password  = var.mysql_cdc_password
    mysql_database      = var.mysql_database
  })

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  tags = {
    Name = "${local.name_prefix}-mysql"
  }
}

resource "aws_s3_bucket" "insurance_docs" {
  bucket = local.s3_bucket_name

  force_destroy = true
}

resource "aws_s3_bucket_versioning" "insurance_docs" {
  bucket = aws_s3_bucket.insurance_docs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "insurance_docs" {
  bucket = aws_s3_bucket.insurance_docs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "insurance_docs" {
  bucket = aws_s3_bucket.insurance_docs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_object" "folder_claim_forms" {
  bucket  = aws_s3_bucket.insurance_docs.id
  key     = "claim_forms/"
  content = ""
}

resource "aws_s3_object" "folder_medical_reports" {
  bucket  = aws_s3_bucket.insurance_docs.id
  key     = "medical_reports/"
  content = ""
}

resource "aws_s3_object" "folder_policy_documents" {
  bucket  = aws_s3_bucket.insurance_docs.id
  key     = "policy_documents/"
  content = ""
}

resource "aws_s3_object" "folder_inspection_photos" {
  bucket  = aws_s3_bucket.insurance_docs.id
  key     = "inspection_photos/"
  content = ""
}

resource "aws_s3_object" "folder_correspondence" {
  bucket  = aws_s3_bucket.insurance_docs.id
  key     = "correspondence/"
  content = ""
}

resource "null_resource" "upload_schema" {
  depends_on = [aws_instance.mysql]

  provisioner "local-exec" {
    command = <<-EOT
      echo "Waiting 120s for EC2 user_data to complete MySQL setup..."
      sleep 120

      INSTANCE_ID="${aws_instance.mysql.id}"
      REGION="${var.aws_region}"

      echo "Uploading schema SQL via SSM..."
      SCHEMA_CONTENT=$(cat ${path.module}/scripts/mysql_schema.sql | base64)

      aws ssm send-command \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters "commands=[
          \"echo '$SCHEMA_CONTENT' | base64 -d > /tmp/mysql_schema.sql\",
          \"mysql -u root -p'${var.mysql_root_password}' < /tmp/mysql_schema.sql\",
          \"mysql -u root -p'${var.mysql_root_password}' -e 'SHOW TABLES IN insurance_db;'\",
          \"rm -f /tmp/mysql_schema.sql\"
        ]" \
        --region "$REGION" \
        --output text
    EOT
  }
}
