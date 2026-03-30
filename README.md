# FinServ Insurance Underwriting & Claims Pipeline

Real-time data pipeline for insurance underwriting and claims analytics, built for the Coalesce May 2026 Webinar.

**MySQL 8.4 (EC2)** --> **Openflow CDC (SPCS)** --> **Snowflake** --> **Coalesce (Transformations)** --> **Streamlit in Snowflake**

```
  AWS EC2 (us-west-2)       Openflow (SPCS)         Snowflake                 Coalesce
  ┌──────────────────┐     ┌───────────────┐       ┌────────────────────┐    ┌─────────────────────┐
  │ MySQL 8.4        │     │ NiFi Canvas   │       │ INSURANCE_RAW      │    │ Transformation      │
  │ insurance_db     │─bg─>│ MySQL CDC     │─CDC─> │  "insurance_db".* │───>│ Platform            │
  │ 8 tables         │     │ Connector     │       │  (8 tables, 53K)   │    │                     │
  │ ~53,705 rows     │     │               │       └────────────────────┘    │ RAW → CURATED (5)   │
  └──────────────────┘     │ MariaDB       │                                 │ CURATED → ANALYTICS │
                           │ Connector/J   │       ┌────────────────────┐    │ (7 views)           │
  AWS S3                   │ 3.5.3         │       │ INSURANCE_CURATED  │<───│                     │
  ┌──────────────────┐     └───────────────┘       │  UNDERWRITING.*    │    └─────────────────────┘
  │ finserv-insurance │                            │  CLAIMS.*          │
  │ -demo-docs-*     │                            │  COMMON.*          │    Streamlit in Snowflake
  │ (unstructured)   │                            ├────────────────────┤    ┌─────────────────────┐
  └──────────────────┘                            │ INSURANCE_ANALYTICS│───>│ 5-page Dashboard    │
                                                  │  DASHBOARDS.*      │    │ (SiS)               │
                                                  └────────────────────┘    └─────────────────────┘
```

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Repository Structure](#repository-structure)
- [Phase 1: AWS Infrastructure](#phase-1-aws-infrastructure)
- [Phase 2: Mock Data Generation](#phase-2-mock-data-generation)
- [Phase 3: Openflow CDC Setup](#phase-3-openflow-cdc-setup)
- [Phase 4: Coalesce Transformations](#phase-4-coalesce-transformations)
- [Phase 5: Streamlit Dashboard](#phase-5-streamlit-dashboard)
- [Live Demo Script](#live-demo-script)
- [Teardown](#teardown)
- [Key Gotchas & Lessons Learned](#key-gotchas--lessons-learned)
- [Reference](#reference)

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Terraform | >= 1.3.0 | `brew install terraform` |
| AWS CLI | v2 | `brew install awscli` |
| Python | >= 3.9 | `brew install python` |
| Faker | latest | `pip3 install faker pandas` |
| Snowflake CLI | latest | `pip install snowflake-cli` |

**AWS:** Credentials configured (`aws configure`) with permissions for EC2, S3, IAM, SSM.

**Snowflake:** Account with Openflow enabled, roles: `CORTEXCODECLIROLE`, `SECURITYADMIN`.

---

## Repository Structure

```
Finserv_Webinar_May2026/
├── README.md                         # This file
├── DESIGN.md                         # Detailed architecture and schema design
├── context.md                        # Full project context, troubleshooting & runbooks
├── AGENTS.md                         # AI agent instructions
├── deploy.sh                         # AWS deploy/destroy/status wrapper
├── .gitignore
│
├── terraform/                        # AWS Infrastructure as Code
│   ├── main.tf                       # EC2, S3, Security Group, IAM/SSM
│   ├── variables.tf                  # Configurable inputs
│   ├── outputs.tf                    # Connection details, EAI SQL
│   ├── terraform.tfvars.example      # Template (copy to terraform.tfvars)
│   ├── .gitignore                    # Excludes state/secrets
│   └── scripts/
│       ├── user_data.sh.tpl          # EC2 bootstrap: MySQL 8 + CDC config
│       └── mysql_schema.sql          # DDL for all 8 insurance tables
│
├── mock_data/                        # Data generation & loading
│   ├── generate_all.py               # Generates ~53,705 rows across 8 CSVs (Faker)
│   ├── load_to_mysql.py              # Uploads CSVs via S3 → EC2 → MySQL LOAD DATA
│   ├── generate_incremental.py       # Generates incremental INSERTs/UPDATEs/DELETEs
│   ├── load_incremental.py           # Executes incremental SQL on EC2 MySQL via SSM
│   └── output/                       # Generated CSV/SQL files (git-ignored)
│
├── Coalesce_transformation.md        # Transformation plan for Coalesce platform
│
├── snowflake/                        # Snowflake SQL objects
│   └── 04_streamlit_deploy.sql       # SiS deployment (TODO)
│
└── streamlit/                        # Streamlit app (TODO)
    ├── streamlit_app.py
    └── app_pages/
        ├── 01_executive_summary.py
        ├── 02_underwriting_analytics.py
        ├── 03_claims_analytics.py
        ├── 04_risk_insights.py
        └── 05_document_intelligence.py
```

---

## Phase 1: AWS Infrastructure

Terraform creates an EC2 instance running MySQL 8.4 with CDC enabled, plus an S3 bucket for unstructured documents.

### Quick Start

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit: mysql_root_password, mysql_cdc_password

./deploy.sh deploy
./deploy.sh status
```

### What Gets Created

| Resource | Details |
|----------|---------|
| EC2 Instance | t3.micro, Amazon Linux 2023, us-west-2 |
| MySQL 8.4 | CDC-enabled (binlog_format=ROW, binlog_row_image=FULL) |
| CDC User | `openflow_cdc` with SELECT + REPLICATION privileges |
| S3 Bucket | `finserv-insurance-demo-docs-<account_id>` |
| Security Group | Port 3306 open (configurable via `allowed_cidr_blocks`) |
| IAM Role | SSM Session Manager + S3 read access |

### EC2 Access

```bash
aws ssm start-session --target i-0875da06f1fcd78cd --region us-west-2
```

### MySQL CDC Configuration

Config file `/etc/my.cnf.d/cdc.cnf`:

```ini
[mysqld]
server-id=1
log_bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
binlog_row_metadata=FULL
gtid_mode=ON
enforce_gtid_consistency=ON
expire_logs_days=3
bind-address=0.0.0.0
max_connections=100
local_infile=ON
```

> **WARNING (MySQL 8.4):** `gtid_mode` and `binlog_row_metadata` do NOT persist from the config file after a MySQL restart. They must be set dynamically via `SET GLOBAL`. See [GTID Recovery](#if-mysql-was-restarted) below.

### GTID Online Migration

MySQL 8.4 requires stepped migration for GTID (cannot go straight to ON):

```sql
SET GLOBAL enforce_gtid_consistency = WARN;
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = OFF_PERMISSIVE;
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL gtid_mode = ON;
SET GLOBAL binlog_row_metadata = FULL;
```

**Do NOT restart MySQL after this.** Settings are in-memory only.

---

## Phase 2: Mock Data Generation

Generate ~53,705 rows of realistic insurance data using Python Faker.

### Data Volume

| Table | Rows | Description |
|-------|------|-------------|
| `customers` | 5,000 | Policyholders with demographics, credit scores |
| `policies` | 10,000 | 5 product types (AUTO, HOME, LIFE, HEALTH, COMMERCIAL) |
| `underwriting_decisions` | 10,000 | 1:1 with policies, approval/decline with risk scoring |
| `claims` | 4,000 | ~40% of policies have claims |
| `claim_payments` | 4,650 | ~1.16 payments per claim |
| `underwriters` | 25 | Underwriting staff with specializations |
| `adjusters` | 30 | Claims adjusters by region |
| `risk_factors` | 20,000 | ~2 factors per policy |
| **Total** | **~53,705** | |

### Generate and Load

```bash
cd mock_data/
python3 generate_all.py          # Creates 8 CSV files in output/
python3 load_to_mysql.py         # Uploads via S3 → EC2 → MySQL LOAD DATA LOCAL INFILE
```

The load script uses S3 as an intermediary (SSM inline base64 has a 97KB limit for large files). EC2 downloads from S3 and loads via `LOAD DATA LOCAL INFILE`.

### Data Realism

- Auto: higher claims in winter, younger drivers = higher risk
- Home: claims spike during storm seasons, property age drives premiums
- Health: premiums correlated to age, pre-existing conditions as risk factors
- Life: age + health conditions drive risk scores
- Commercial: larger coverages, industry-specific risk factors
- Temporal: policies span 2023-01 to 2026-03

---

## Phase 3: Openflow CDC Setup

Replicate MySQL tables to Snowflake in near-real-time using Openflow's MySQL CDC connector deployed on SPCS.

### Step 1: Create Openflow Admin Role & User

```sql
CREATE ROLE IF NOT EXISTS OPENFLOW_ADMIN;
GRANT ROLE OPENFLOW_ADMIN TO ROLE ACCOUNTADMIN;

GRANT CREATE OPENFLOW DATA PLANE INTEGRATION ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE OPENFLOW RUNTIME INTEGRATION ON ACCOUNT TO ROLE OPENFLOW_ADMIN;
GRANT CREATE ROLE ON ACCOUNT TO ROLE OPENFLOW_ADMIN;

GRANT ROLE OPENFLOW_ADMIN TO USER <your_user>;
ALTER USER <your_user> SET DEFAULT_ROLE = 'OPENFLOW_ADMIN';
```

### Step 2: Provision Openflow (Snowsight UI)

1. Navigate to **Snowsight -> Data -> Openflow**
2. Create a **Data Plane Integration**
3. Create a **Runtime** (SPCS, SMALL size)
4. Grant integration access to OPENFLOW_ADMIN:
   ```sql
   GRANT USAGE, MONITOR, OPERATE ON INTEGRATION <runtime_integration> TO ROLE OPENFLOW_ADMIN;
   GRANT USAGE ON INTEGRATION <dataplane_integration> TO ROLE OPENFLOW_ADMIN;
   ```

### Step 3: Create External Access Integration

```sql
CREATE OR REPLACE NETWORK RULE mysql_ec2_network_rule
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('<ec2_public_ip>:3306');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION mysql_ec2_eai
  ALLOWED_NETWORK_RULES = (mysql_ec2_network_rule)
  ENABLED = TRUE;

GRANT USAGE ON INTEGRATION mysql_ec2_eai TO ROLE <runtime_role>;
```

Attach EAI to Runtime: Runtime -> "..." menu -> "External access integrations" -> select `mysql_ec2_eai` -> Save.

### Step 4: Deploy MySQL CDC Connector

Deploy the `mysql` flow from the Snowflake Openflow Connector Registry via NiFi canvas or nipyapi CLI.

### Step 5: Configure Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| MySQL Connection URL | `jdbc:mariadb://<ec2_ip>:3306/insurance_db` | **Must use `jdbc:mariadb://` prefix, NOT `jdbc:mysql://`** |
| MySQL Username | `openflow_cdc` | |
| MySQL Password | (sensitive) | |
| MySQL JDBC Driver | MariaDB Connector/J 3.5.3 | Upload as asset parameter |
| Included Table Names | (empty) | |
| Included Table Regex | `.*` | Matches all tables in the database |
| Object Identifier Resolution | `CASE_INSENSITIVE` | |
| Snowflake Destination Database | `INSURANCE_RAW` | Must pre-create: `CREATE DATABASE IF NOT EXISTS INSURANCE_RAW` |
| Snowflake Warehouse | `COCOWH` | |
| Snowflake Role | `CORTEXCODECLIROLE` | |

> **Key Detail:** The MySQL connector bundles MariaDB Connector/J, which registers as `jdbc:mariadb://` not `jdbc:mysql://`. Using `jdbc:mysql://` will cause "No suitable driver found" errors.

> **Key Detail:** Use `Included Table Regex = .*` instead of listing table names. ListTableNames outputs double-quoted FQNs like `"insurance_db"."adjusters"` which are difficult to match with Included Table Names.

### Step 6: Upload JDBC Driver

```bash
curl -O https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.5.3/mariadb-java-client-3.5.3.jar
```

Upload via NiFi canvas: Parameter Context -> MySQL JDBC Driver -> Upload asset.

### Step 7: NiFi Canvas Login

> **Important:** ACCOUNTADMIN, ORGADMIN, SECURITYADMIN are **blocked** from NiFi canvas login by design. Use OPENFLOW_ADMIN or CORTEXCODECLIROLE.

> **Important:** If your account has a network policy, you may need to temporarily unset it for the OAuth callback:
> ```sql
> ALTER ACCOUNT UNSET NETWORK_POLICY;
> -- After login:
> ALTER ACCOUNT SET NETWORK_POLICY = <your_policy>;
> ```

Login to NiFi canvas with a **human user** (service users can't do OAuth browser login).

### Step 8: Verify, Enable, Start

1. Verify controller services configuration
2. Enable all controller services
3. Verify processor configuration
4. Start the flow
5. Validate data flow (see below)

### Actual Landing Tables

Despite `CASE_INSENSITIVE`, schema and table names are created **lowercase** in Snowflake. Must use quoted identifiers:

```sql
SELECT * FROM INSURANCE_RAW."insurance_db"."customers";
SELECT * FROM INSURANCE_RAW."insurance_db"."policies";
SELECT * FROM INSURANCE_RAW."insurance_db"."underwriting_decisions";
SELECT * FROM INSURANCE_RAW."insurance_db"."claims";
SELECT * FROM INSURANCE_RAW."insurance_db"."claim_payments";
SELECT * FROM INSURANCE_RAW."insurance_db"."underwriters";
SELECT * FROM INSURANCE_RAW."insurance_db"."adjusters";
SELECT * FROM INSURANCE_RAW."insurance_db"."risk_factors";
```

### Validate Data

```sql
SELECT 'adjusters' as tbl, COUNT(*) as cnt FROM INSURANCE_RAW."insurance_db"."adjusters"
UNION ALL SELECT 'claim_payments', COUNT(*) FROM INSURANCE_RAW."insurance_db"."claim_payments"
UNION ALL SELECT 'claims', COUNT(*) FROM INSURANCE_RAW."insurance_db"."claims"
UNION ALL SELECT 'customers', COUNT(*) FROM INSURANCE_RAW."insurance_db"."customers"
UNION ALL SELECT 'policies', COUNT(*) FROM INSURANCE_RAW."insurance_db"."policies"
UNION ALL SELECT 'risk_factors', COUNT(*) FROM INSURANCE_RAW."insurance_db"."risk_factors"
UNION ALL SELECT 'underwriters', COUNT(*) FROM INSURANCE_RAW."insurance_db"."underwriters"
UNION ALL SELECT 'underwriting_decisions', COUNT(*) FROM INSURANCE_RAW."insurance_db"."underwriting_decisions"
ORDER BY tbl;
```

### Snapshot Load Flow (NiFi Internal)

```
GenerateFlowFile
  └── ListTableNames
        (outputs FQNs: "insurance_db"."adjusters", etc.)
      └── PickTablesForReplication
            (filters by Included Table Regex=.*)
          ├── new → UpdateTableState → Perform Snapshot Load
          │           └── FetchSourceTableSchema
          │           └── CreateSnowflakeTable
          │           └── PutSnowpipeStreaming
          └── stale → UpdateTableState (remove from replication)

Incremental Load:
  CaptureChangeMySQL → ... → PutSnowpipeStreaming
```

### Incremental CDC Testing

Generate and apply incremental changes (INSERTs, UPDATEs, DELETEs) to test CDC replication:

```bash
cd mock_data/
python3 generate_incremental.py       # Generates ~450 SQL statements in output/
python3 load_incremental.py           # Uploads to S3 → executes on EC2 MySQL via SSM
```

Requires environment variables (or defaults to values in script):

```bash
export EC2_INSTANCE_ID=i-XXXXXXXXXXXX
export MYSQL_PASS='your_password'
export S3_BUCKET='your-bucket-name'
```

| Operation | Volume | Tables Affected |
|-----------|--------|----------------|
| INSERT | ~305 rows | customers (+50), policies (+80), decisions (+80), claims (+30), payments (+25), risk_factors (+40) |
| UPDATE | ~105 rows | claims (status transitions), policies (status changes), customers (address changes), decisions (overrides) |
| DELETE | ~40 rows | risk_factors (reassessment), cancelled policy cascades |

CDC latency is ~60 seconds. Verify in Snowflake:

```sql
-- Check new rows
SELECT "claim_id", "claim_number", "status", _SNOWFLAKE_INSERTED_AT
FROM INSURANCE_RAW."insurance_db"."claims"
WHERE "claim_id" > 4000 ORDER BY "claim_id" DESC LIMIT 5;

-- Check updates (recent)
SELECT COUNT(*) FROM INSURANCE_RAW."insurance_db"."claims"
WHERE _SNOWFLAKE_UPDATED_AT > DATEADD('minute', -5, CURRENT_TIMESTAMP());

-- Check soft deletes
SELECT COUNT(*) FROM INSURANCE_RAW."insurance_db"."risk_factors"
WHERE _SNOWFLAKE_DELETED = TRUE;
```

> **Note:** Openflow CDC creates lowercase column names. You must use quoted identifiers for columns too: `"claim_id"` not `CLAIM_ID`.

> **Note:** DELETEs appear as soft deletes (`_SNOWFLAKE_DELETED = TRUE`), not physical row removal.

---

## Phase 4: Coalesce Transformations

Three-layer architecture: RAW -> CURATED -> ANALYTICS. **Transformations are built and managed by the [Coalesce](https://coalesce.io/) platform** (not raw Snowflake SQL).

Full transformation specs (source tables, join logic, column mappings, expressions) are in **[`Coalesce_transformation.md`](Coalesce_transformation.md)**.

### Target Databases and Schemas

```sql
CREATE DATABASE IF NOT EXISTS INSURANCE_CURATED;
CREATE SCHEMA IF NOT EXISTS INSURANCE_CURATED.UNDERWRITING;
CREATE SCHEMA IF NOT EXISTS INSURANCE_CURATED.CLAIMS;
CREATE SCHEMA IF NOT EXISTS INSURANCE_CURATED.COMMON;

CREATE DATABASE IF NOT EXISTS INSURANCE_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS INSURANCE_ANALYTICS.DASHBOARDS;
```

### Curated Layer (5 views) — built by Coalesce

| View | Schema | Purpose |
|------|--------|---------|
| `V_UNDERWRITING_PIPELINE` | CURATED.UNDERWRITING | Joins policies + customers + decisions + underwriters |
| `V_RISK_FACTOR_SUMMARY` | CURATED.UNDERWRITING | Aggregated risk factors per policy |
| `V_CLAIMS_DETAIL` | CURATED.CLAIMS | Joins claims + policies + customers + adjusters + payments |
| `V_CLAIM_PAYMENTS_SUMMARY` | CURATED.CLAIMS | Payment aggregates per claim with type/method breakdowns |
| `V_CUSTOMER_360` | CURATED.COMMON | 360-degree customer view with policy, claims, payment rollups |

### Analytics Layer (7 views) — built by Coalesce

| View | Schema | Purpose |
|------|--------|---------|
| `V_KPI_UNDERWRITING` | ANALYTICS.DASHBOARDS | Monthly underwriting metrics by product type |
| `V_KPI_CLAIMS` | ANALYTICS.DASHBOARDS | Monthly claims metrics by type and product |
| `V_LOSS_RATIO` | ANALYTICS.DASHBOARDS | Loss ratio = incurred losses / earned premium |
| `V_FRAUD_SUMMARY` | ANALYTICS.DASHBOARDS | Fraud analysis by product type, claim type, month |
| `V_GEOGRAPHIC_SUMMARY` | ANALYTICS.DASHBOARDS | Policy and claims metrics by state |
| `V_UNDERWRITER_PERFORMANCE` | ANALYTICS.DASHBOARDS | Performance metrics per underwriter |
| `V_ADJUSTER_PERFORMANCE` | ANALYTICS.DASHBOARDS | Performance metrics per claims adjuster |

> **Note:** All source references must use quoted lowercase identifiers: `INSURANCE_RAW."insurance_db"."table_name"`
>
> **Note:** CDC soft-delete filter required on all source tables: `WHERE _SNOWFLAKE_DELETED IS NOT TRUE`

---

## Phase 5: Streamlit Dashboard

Multi-page Streamlit in Snowflake (SiS) application with 5 pages.

| Page | Content |
|------|---------|
| Executive Summary | KPI cards, loss ratio trend, premium vs losses, product mix |
| Underwriting Analytics | Pipeline funnel, approval rates, risk distribution, underwriter performance |
| Claims Analytics | Status breakdown, claim types, settlement metrics, fraud indicators |
| Risk Insights | Factor heatmap, credit vs claims correlation, high-risk alerts |
| Document Intelligence | Parsed S3 documents via Cortex AI, full-text search |

---

## Live Demo Script

### 1. Show Current State
Open Streamlit dashboard, show existing claims data.

### 2. Insert New Claim in MySQL
```sql
-- Run on EC2 via SSM
INSERT INTO insurance_db.claims (
  claim_number, policy_id, customer_id, incident_date, reported_date,
  claim_type, description, estimated_amount, status, priority
) VALUES (
  'CLM-LIVE-00001', 1, 1, CURDATE(), CURDATE(),
  'COLLISION', 'Live demo: rear-end collision on I-5', 15000.00, 'OPEN', 'HIGH'
);
```

### 3. Watch Snowflake Update
Query Snowflake to show the new row appearing (~60s CDC latency):
```sql
SELECT * FROM INSURANCE_RAW."insurance_db"."claims"
WHERE claim_number = 'CLM-LIVE-00001';
```

### 4. Refresh Dashboard
Refresh Streamlit to show updated metrics.

---

## Teardown

```bash
# Destroy all AWS resources (EC2, S3, IAM, SG)
./deploy.sh destroy

# Stop Openflow runtime in Snowsight UI, then:
DROP DATABASE IF EXISTS INSURANCE_RAW;
DROP DATABASE IF EXISTS INSURANCE_CURATED;
DROP DATABASE IF EXISTS INSURANCE_ANALYTICS;

DROP INTEGRATION IF EXISTS mysql_ec2_eai;
DROP NETWORK RULE IF EXISTS mysql_ec2_network_rule;
```

---

## Key Gotchas & Lessons Learned

### Openflow / NiFi

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `No suitable driver found for jdbc:mysql://` | MariaDB Connector/J registers as `jdbc:mariadb://` | Change URL prefix to `jdbc:mariadb://` |
| PickTablesForReplication drops all FlowFiles (In: X, Out: 0) | FQN format mismatch or stale Table State Store | Use `Included Table Regex = .*`; clear Table State Store state |
| Tables go to FAILED state on snapshot | Stale state or tables already exist in Snowflake | Drop target tables + clear Table State Store |
| `SNOWFLAKE_OBJECT_ALREADY_EXISTS` | Previous partial snapshot left empty tables | Drop all target tables before re-snapshot |
| NiFi login blocked ("role explicitly blocked") | ACCOUNTADMIN/ORGADMIN/SECURITYADMIN blocked by design | Use OPENFLOW_ADMIN or CORTEXCODECLIROLE |
| NiFi OAuth callback fails | Account network policy blocks the callback URL | Temporarily `ALTER ACCOUNT UNSET NETWORK_POLICY` |
| Service user can't login to NiFi | OAuth requires browser login (human user) | Use a human user (not service account) |
| CASE_INSENSITIVE still creates lowercase names | Known behavior — schema/tables are lowercase | Use quoted identifiers: `"insurance_db"."table"` |

### MySQL 8.4

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `binlog_row_metadata expected FULL got MINIMAL` | MySQL 8.4 defaults to MINIMAL, ignores config file | `SET GLOBAL binlog_row_metadata = FULL` |
| `gtid_mode is OFF` after restart | MySQL 8.4 ignores gtid_mode from config file | Online migration: OFF -> OFF_PERMISSIVE -> ON_PERMISSIVE -> ON |
| CDC settings lost after restart | gtid_mode and binlog_row_metadata are in-memory only | **Do NOT restart MySQL** |

### Data Loading

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| SSM inline transfer fails for large files | SSM has ~97KB parameter limit | Use S3 as intermediary |
| LOAD DATA LOCAL INFILE loads 0 rows | `local_infile` defaults to OFF | `SET GLOBAL local_infile = ON` |
| EC2 can't read from S3 | IAM role missing S3 policy | Add inline S3 read policy to EC2 IAM role |

### If MySQL Was Restarted

Run these commands to restore CDC settings:

```sql
SET GLOBAL enforce_gtid_consistency = WARN;
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = OFF_PERMISSIVE;
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL gtid_mode = ON;
SET GLOBAL binlog_row_metadata = FULL;
```

For detailed troubleshooting and operational runbooks, see **`context.md`**.

---

## Reference

### Current Deployment

| Resource | Value |
|----------|-------|
| EC2 Instance | `i-0875da06f1fcd78cd` |
| Region | us-west-2 |
| Public IP | 34.221.130.212 |
| JDBC URL | `jdbc:mariadb://34.221.130.212:3306/insurance_db` |
| CDC User | `openflow_cdc` |
| S3 Bucket | `finserv-insurance-demo-docs-943644343293` |
| Snowflake Account | sfsenorthamerica-demo_vsuru |
| Snowflake Role | CORTEXCODECLIROLE |
| Snowflake Warehouse | COCOWH |
| Openflow Runtime | `OPENFLOW_RUNTIME_E16109D5_CDB4_8813_B38C_669AC889C809` |
| Openflow Data Plane | `OPENFLOW_DATAPLANE_D7AB97D4_08F9_4651_8554_02466A57CBEB` |
| NiFi Canvas | `https://of--sfsenorthamerica-demo-vsuru.snowflakecomputing.app/finserv-runtime/nifi/` |
| Account Network Policy | `ACCOUNT_VPN_POLICY_SE` |

### Snowflake Databases

| Database | Purpose |
|----------|---------|
| `INSURANCE_RAW` | Openflow CDC landing zone (do not modify directly) |
| `INSURANCE_CURATED` | Transformation views built by Coalesce (underwriting, claims, common) |
| `INSURANCE_ANALYTICS` | Dashboard KPI views built by Coalesce, Streamlit app |

### Useful Commands

```bash
# SSM connect to EC2
aws ssm start-session --target i-0875da06f1fcd78cd --region us-west-2

# Check MySQL CDC status
sudo mysql -u root -p'<password>' -e "SHOW GLOBAL VARIABLES LIKE 'gtid_mode'; SHOW GLOBAL VARIABLES LIKE 'binlog_row_metadata';"

# Check Terraform state
./deploy.sh status

# Verify Snowflake data
# See "Validate Data" query above
```
