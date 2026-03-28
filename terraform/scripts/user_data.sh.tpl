#!/bin/bash
set -euo pipefail

exec > /var/log/user-data.log 2>&1
echo "=== User data script started at $(date) ==="

MYSQL_ROOT_PASSWORD="${mysql_root_password}"
MYSQL_CDC_PASSWORD="${mysql_cdc_password}"
MYSQL_DATABASE="${mysql_database}"

echo "=== Installing MySQL 8 ==="
dnf update -y
dnf install -y mysql-community-server mysql-community-client || {
    rpm -Uvh https://dev.mysql.com/get/mysql84-community-release-el9-1.noarch.rpm
    dnf install -y mysql-community-server mysql-community-client
}

echo "=== Configuring MySQL for CDC ==="
cat > /etc/my.cnf.d/cdc.cnf <<'MYCNF'
[mysqld]
server-id=1
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
gtid_mode=ON
enforce_gtid_consistency=ON
expire_logs_days=3

bind-address=0.0.0.0

max_connections=100
innodb_buffer_pool_size=256M
MYCNF

echo "=== Starting MySQL ==="
systemctl enable mysqld
systemctl start mysqld

TEMP_PASSWORD=$(grep 'temporary password' /var/log/mysqld.log | tail -1 | awk '{print $NF}')

echo "=== Setting root password ==="
mysql --connect-expired-password -u root -p"$TEMP_PASSWORD" <<EOSQL
ALTER USER 'root'@'localhost' IDENTIFIED BY '$MYSQL_ROOT_PASSWORD';
FLUSH PRIVILEGES;
EOSQL

echo "=== Creating CDC user ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<EOSQL
CREATE USER IF NOT EXISTS 'openflow_cdc'@'%' IDENTIFIED BY '$MYSQL_CDC_PASSWORD';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'openflow_cdc'@'%';
FLUSH PRIVILEGES;
EOSQL

echo "=== Creating database and schema ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /tmp/mysql_schema.sql

echo "=== Verifying CDC configuration ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SHOW VARIABLES LIKE 'gtid_mode';"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SHOW VARIABLES LIKE 'log_bin';"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SHOW VARIABLES LIKE 'binlog_format';"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SHOW DATABASES;"

echo "=== Installing SSM Agent ==="
dnf install -y amazon-ssm-agent || true
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

echo "=== User data script completed at $(date) ==="
