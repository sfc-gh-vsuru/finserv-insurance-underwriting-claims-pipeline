output "ec2_instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.mysql.id
}

output "ec2_public_ip" {
  description = "Public IP of the MySQL EC2 instance"
  value       = aws_instance.mysql.public_ip
}

output "ec2_public_dns" {
  description = "Public DNS of the MySQL EC2 instance"
  value       = aws_instance.mysql.public_dns
}

output "mysql_connection_url" {
  description = "JDBC connection URL for Openflow CDC connector"
  value       = "jdbc:mysql://${aws_instance.mysql.public_ip}:3306/${var.mysql_database}"
}

output "mysql_cdc_username" {
  description = "MySQL CDC replication username"
  value       = "openflow_cdc"
}

output "s3_bucket_name" {
  description = "S3 bucket for insurance documents"
  value       = aws_s3_bucket.insurance_docs.id
}

output "s3_bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.insurance_docs.arn
}

output "security_group_id" {
  description = "Security group ID for MySQL instance"
  value       = aws_security_group.mysql.id
}

output "ssm_connect_command" {
  description = "AWS CLI command to connect via SSM"
  value       = "aws ssm start-session --target ${aws_instance.mysql.id} --region ${var.aws_region}"
}

output "openflow_eai_sql" {
  description = "SQL to create Snowflake External Access Integration for this MySQL instance"
  value       = <<-EOT

    -- Run in Snowflake as SECURITYADMIN
    USE ROLE SECURITYADMIN;

    CREATE OR REPLACE NETWORK RULE mysql_ec2_network_rule
      TYPE = HOST_PORT
      MODE = EGRESS
      VALUE_LIST = ('${aws_instance.mysql.public_ip}:3306');

    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION mysql_ec2_eai
      ALLOWED_NETWORK_RULES = (mysql_ec2_network_rule)
      ENABLED = TRUE
      COMMENT = 'EAI for Openflow MySQL CDC to EC2 instance ${aws_instance.mysql.id}';

    -- Grant to your Openflow runtime role:
    -- GRANT USAGE ON INTEGRATION mysql_ec2_eai TO ROLE <runtime_role>;

  EOT
}
