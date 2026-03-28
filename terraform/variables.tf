variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "finserv-insurance"
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "demo"
}

variable "instance_type" {
  description = "EC2 instance type for MySQL server"
  type        = string
  default     = "t3.micro"
}

variable "mysql_root_password" {
  description = "MySQL root password"
  type        = string
  sensitive   = true
}

variable "mysql_cdc_password" {
  description = "Password for the openflow_cdc replication user"
  type        = string
  sensitive   = true
}

variable "mysql_database" {
  description = "MySQL database name"
  type        = string
  default     = "insurance_db"
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to connect to MySQL (port 3306). Include Snowflake Openflow SPCS egress IPs."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "s3_bucket_name" {
  description = "S3 bucket name for insurance documents. Must be globally unique."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}
