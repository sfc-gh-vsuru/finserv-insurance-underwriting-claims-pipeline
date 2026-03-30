# Coalesce Transformation Plan

## FinServ Insurance Underwriting & Claims Pipeline
### Transformations: INSURANCE_RAW -> INSURANCE_CURATED -> INSURANCE_ANALYTICS

**Owner:** Coalesce Platform
**Date:** 2026-03-28
**Status:** Ready for Implementation

---

## Overview

This document defines all transformations to be built in Coalesce, taking raw CDC-replicated tables from `INSURANCE_RAW` and producing curated and analytics-ready objects in `INSURANCE_CURATED` and `INSURANCE_ANALYTICS`.

### Data Flow

```
INSURANCE_RAW."insurance_db"          INSURANCE_CURATED                INSURANCE_ANALYTICS
(CDC landing - 8 tables)              (Enriched/Joined)                (Aggregated KPIs)
┌─────────────────────────┐          ┌────────────────────────┐       ┌─────────────────────────┐
│ "customers"        5000 │──┐       │ UNDERWRITING schema    │       │ DASHBOARDS schema       │
│ "policies"        10000 │──┤──────>│  V_UNDERWRITING_       │──────>│  V_KPI_UNDERWRITING     │
│ "underwriting_    10000 │──┤       │    PIPELINE             │       │                         │
│   decisions"            │──┤       │  V_RISK_FACTOR_SUMMARY │       │  V_KPI_CLAIMS           │
│ "underwriters"       25 │──┘       │                        │       │                         │
│ "claims"           4000 │──┐       │ CLAIMS schema          │       │  V_LOSS_RATIO           │
│ "claim_payments"   4650 │──┤──────>│  V_CLAIMS_DETAIL       │──────>│                         │
│ "adjusters"          30 │──┤       │  V_CLAIM_PAYMENTS_     │       │  V_FRAUD_SUMMARY        │
│ "risk_factors"    20000 │──┘       │    SUMMARY             │       │                         │
│                         │          │                        │       │  V_GEOGRAPHIC_SUMMARY   │
│ (CDC metadata columns:) │          │ COMMON schema          │       │                         │
│  _SNOWFLAKE_INSERTED_AT │          │  V_CUSTOMER_360        │       │  V_UNDERWRITER_         │
│  _SNOWFLAKE_UPDATED_AT  │          │                        │       │    PERFORMANCE          │
│  _SNOWFLAKE_DELETED     │          │                        │       │                         │
└─────────────────────────┘          └────────────────────────┘       │  V_ADJUSTER_PERFORMANCE │
                                                                      └─────────────────────────┘
```

---

## Source: INSURANCE_RAW."insurance_db"

All source tables are in database `INSURANCE_RAW`, schema `"insurance_db"` (lowercase, must be quoted).

> **IMPORTANT:** All source table/schema references require quoted lowercase identifiers:
> `INSURANCE_RAW."insurance_db"."table_name"`

> **IMPORTANT:** All source tables have 3 CDC metadata columns that should be excluded from curated/analytics views (unless needed for audit):
> - `_SNOWFLAKE_INSERTED_AT` (TIMESTAMP_NTZ) - when the row was first inserted by CDC
> - `_SNOWFLAKE_UPDATED_AT` (TIMESTAMP_NTZ) - when the row was last updated by CDC
> - `_SNOWFLAKE_DELETED` (BOOLEAN) - soft-delete flag from CDC

> **IMPORTANT:** Curated views should filter out soft-deleted rows: `WHERE _SNOWFLAKE_DELETED IS NULL OR _SNOWFLAKE_DELETED = FALSE`

### Source Table: "customers" (5,000 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| customer_id | NUMBER | NO | PK |
| first_name | TEXT | YES | |
| last_name | TEXT | YES | |
| date_of_birth | DATE | YES | For age calculation |
| gender | TEXT | YES | M, F, O |
| ssn_hash | TEXT | YES | SHA-256 hashed SSN |
| email | TEXT | YES | |
| phone | TEXT | YES | |
| address_line1 | TEXT | YES | |
| city | TEXT | YES | |
| state | TEXT | YES | 2-char US state code |
| zip_code | TEXT | YES | |
| credit_score | NUMBER | YES | 300-850 |
| occupation | TEXT | YES | |
| annual_income | NUMBER | YES | |
| created_at | TIMESTAMP_TZ | YES | |
| updated_at | TIMESTAMP_TZ | YES | |

### Source Table: "policies" (10,000 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| policy_id | NUMBER | NO | PK |
| policy_number | TEXT | YES | e.g., POL-2024-00001 |
| customer_id | NUMBER | YES | FK -> customers |
| product_type | TEXT | YES | AUTO, HOME, LIFE, HEALTH, COMMERCIAL |
| coverage_amount | NUMBER | YES | |
| premium_amount | NUMBER | YES | Annual premium |
| deductible | NUMBER | YES | |
| effective_date | DATE | YES | |
| expiration_date | DATE | YES | |
| status | TEXT | YES | ACTIVE, EXPIRED, CANCELLED, SUSPENDED |
| underwriting_status | TEXT | YES | PENDING, APPROVED, DECLINED, REFERRED |
| risk_score | NUMBER | YES | 1-100 |
| created_at | TIMESTAMP_TZ | YES | |
| updated_at | TIMESTAMP_TZ | YES | |

### Source Table: "underwriting_decisions" (10,000 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| decision_id | NUMBER | NO | PK |
| policy_id | NUMBER | YES | FK -> policies |
| underwriter_id | NUMBER | YES | FK -> underwriters |
| decision | TEXT | YES | APPROVED, DECLINED, REFERRED, COUNTER_OFFER |
| risk_category | TEXT | YES | LOW, MEDIUM, HIGH, VERY_HIGH |
| risk_score | NUMBER | YES | Risk score at decision time |
| premium_adjustment_pct | NUMBER | YES | +/- percentage |
| conditions | TEXT | YES | Special conditions/exclusions |
| notes | TEXT | YES | Underwriter notes |
| decision_date | TIMESTAMP_TZ | YES | |
| review_flag | NUMBER | YES | 0/1 boolean |
| created_at | TIMESTAMP_TZ | YES | |

### Source Table: "claims" (4,000 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| claim_id | NUMBER | NO | PK |
| claim_number | TEXT | YES | e.g., CLM-2024-00001 |
| policy_id | NUMBER | YES | FK -> policies |
| customer_id | NUMBER | YES | FK -> customers |
| incident_date | DATE | YES | |
| reported_date | DATE | YES | |
| claim_type | TEXT | YES | COLLISION, THEFT, FIRE, WATER, LIABILITY, MEDICAL, PROPERTY |
| description | TEXT | YES | |
| estimated_amount | NUMBER | YES | |
| approved_amount | NUMBER | YES | |
| status | TEXT | YES | OPEN, UNDER_REVIEW, APPROVED, DENIED, SETTLED, CLOSED |
| priority | TEXT | YES | LOW, MEDIUM, HIGH, URGENT |
| adjuster_id | NUMBER | YES | FK -> adjusters |
| fraud_flag | NUMBER | YES | 0/1 boolean |
| created_at | TIMESTAMP_TZ | YES | |
| updated_at | TIMESTAMP_TZ | YES | |

### Source Table: "claim_payments" (4,650 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| payment_id | NUMBER | NO | PK |
| claim_id | NUMBER | YES | FK -> claims |
| payment_date | DATE | YES | |
| amount | NUMBER | YES | |
| payment_type | TEXT | YES | PARTIAL, FINAL, SUPPLEMENT |
| payment_method | TEXT | YES | CHECK, ACH, WIRE |
| payee_name | TEXT | YES | |
| notes | TEXT | YES | |
| created_at | TIMESTAMP_TZ | YES | |

### Source Table: "underwriters" (25 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| underwriter_id | NUMBER | NO | PK |
| first_name | TEXT | YES | |
| last_name | TEXT | YES | |
| employee_id | TEXT | YES | |
| specialization | TEXT | YES | AUTO, HOME, LIFE, HEALTH, COMMERCIAL, GENERAL |
| experience_years | NUMBER | YES | |
| approval_limit | NUMBER | YES | Max coverage they can approve |
| active | NUMBER | YES | 0/1 boolean |
| created_at | TIMESTAMP_TZ | YES | |

### Source Table: "adjusters" (30 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| adjuster_id | NUMBER | NO | PK |
| first_name | TEXT | YES | |
| last_name | TEXT | YES | |
| employee_id | TEXT | YES | |
| region | TEXT | YES | |
| specialization | TEXT | YES | AUTO, PROPERTY, LIABILITY, MEDICAL |
| active | NUMBER | YES | 0/1 boolean |
| created_at | TIMESTAMP_TZ | YES | |

### Source Table: "risk_factors" (20,000 rows)

| Column | Data Type | Nullable | Description |
|--------|-----------|----------|-------------|
| factor_id | NUMBER | NO | PK |
| policy_id | NUMBER | YES | FK -> policies |
| factor_type | TEXT | YES | DRIVING_RECORD, PROPERTY_AGE, HEALTH_CONDITION, etc. |
| factor_value | TEXT | YES | |
| impact_score | NUMBER | YES | Positive = higher risk |
| source | TEXT | YES | DMV, INSPECTION, MEDICAL, etc. |
| assessed_date | DATE | YES | |
| created_at | TIMESTAMP_TZ | YES | |

---

## Target Databases & Schemas

### Create Before Transformations

```sql
CREATE DATABASE IF NOT EXISTS INSURANCE_CURATED;
CREATE SCHEMA IF NOT EXISTS INSURANCE_CURATED.UNDERWRITING;
CREATE SCHEMA IF NOT EXISTS INSURANCE_CURATED.CLAIMS;
CREATE SCHEMA IF NOT EXISTS INSURANCE_CURATED.COMMON;

CREATE DATABASE IF NOT EXISTS INSURANCE_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS INSURANCE_ANALYTICS.DASHBOARDS;
```

---

## INSURANCE_CURATED Transformations

### 1. INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE

**Type:** View
**Purpose:** Complete underwriting pipeline joining policies with customer demographics, underwriting decisions, and underwriter details. Primary view for underwriting analysis.

**Sources:**
- `INSURANCE_RAW."insurance_db"."policies"` (p)
- `INSURANCE_RAW."insurance_db"."customers"` (c)
- `INSURANCE_RAW."insurance_db"."underwriting_decisions"` (ud)
- `INSURANCE_RAW."insurance_db"."underwriters"` (uw)

**Join Logic:**
- `policies` INNER JOIN `customers` ON `p.customer_id = c.customer_id`
- `policies` LEFT JOIN `underwriting_decisions` ON `p.policy_id = ud.policy_id`
- `underwriting_decisions` LEFT JOIN `underwriters` ON `ud.underwriter_id = uw.underwriter_id`

**Filter:**
- Exclude soft-deleted rows from all source tables: `WHERE p._SNOWFLAKE_DELETED IS NOT TRUE AND c._SNOWFLAKE_DELETED IS NOT TRUE`

**Output Columns:**

| Column | Source | Expression |
|--------|--------|------------|
| POLICY_ID | policies | p.policy_id |
| POLICY_NUMBER | policies | p.policy_number |
| PRODUCT_TYPE | policies | p.product_type |
| COVERAGE_AMOUNT | policies | p.coverage_amount |
| PREMIUM_AMOUNT | policies | p.premium_amount |
| DEDUCTIBLE | policies | p.deductible |
| EFFECTIVE_DATE | policies | p.effective_date |
| EXPIRATION_DATE | policies | p.expiration_date |
| POLICY_STATUS | policies | p.status |
| UNDERWRITING_STATUS | policies | p.underwriting_status |
| POLICY_RISK_SCORE | policies | p.risk_score |
| CUSTOMER_ID | customers | c.customer_id |
| CUSTOMER_NAME | customers | `c.first_name \|\| ' ' \|\| c.last_name` |
| CUSTOMER_STATE | customers | c.state |
| CREDIT_SCORE | customers | c.credit_score |
| ANNUAL_INCOME | customers | c.annual_income |
| OCCUPATION | customers | c.occupation |
| CUSTOMER_AGE | customers | `DATEDIFF('year', c.date_of_birth, CURRENT_DATE())` |
| DATE_OF_BIRTH | customers | c.date_of_birth |
| DECISION | underwriting_decisions | ud.decision |
| RISK_CATEGORY | underwriting_decisions | ud.risk_category |
| DECISION_RISK_SCORE | underwriting_decisions | ud.risk_score |
| PREMIUM_ADJUSTMENT_PCT | underwriting_decisions | ud.premium_adjustment_pct |
| CONDITIONS | underwriting_decisions | ud.conditions |
| DECISION_DATE | underwriting_decisions | ud.decision_date |
| REVIEW_FLAG | underwriting_decisions | ud.review_flag |
| UNDERWRITER_NAME | underwriters | `uw.first_name \|\| ' ' \|\| uw.last_name` |
| UW_SPECIALIZATION | underwriters | uw.specialization |
| UW_EXPERIENCE | underwriters | uw.experience_years |

---

### 2. INSURANCE_CURATED.UNDERWRITING.V_RISK_FACTOR_SUMMARY

**Type:** View
**Purpose:** Aggregated risk factors per policy. Summarizes count, positive/negative split, average/max impact, and a detail string.

**Sources:**
- `INSURANCE_RAW."insurance_db"."risk_factors"` (rf)

**Filter:**
- `WHERE rf._SNOWFLAKE_DELETED IS NOT TRUE`

**Group By:** `policy_id`

**Output Columns:**

| Column | Expression |
|--------|------------|
| POLICY_ID | rf.policy_id |
| TOTAL_FACTORS | `COUNT(*)` |
| NEGATIVE_FACTORS | `SUM(CASE WHEN rf.impact_score > 0 THEN 1 ELSE 0 END)` |
| POSITIVE_FACTORS | `SUM(CASE WHEN rf.impact_score <= 0 THEN 1 ELSE 0 END)` |
| AVG_IMPACT | `AVG(rf.impact_score)` |
| MAX_IMPACT | `MAX(rf.impact_score)` |
| FACTOR_DETAILS | `LISTAGG(rf.factor_type \|\| ': ' \|\| rf.factor_value, '; ') WITHIN GROUP (ORDER BY ABS(rf.impact_score) DESC)` |

---

### 3. INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL

**Type:** View
**Purpose:** Complete claims view joining claims with policy, customer, adjuster, and payment aggregates. Primary view for claims analysis.

**Sources:**
- `INSURANCE_RAW."insurance_db"."claims"` (cl)
- `INSURANCE_RAW."insurance_db"."policies"` (p)
- `INSURANCE_RAW."insurance_db"."customers"` (c)
- `INSURANCE_RAW."insurance_db"."adjusters"` (adj)
- `INSURANCE_RAW."insurance_db"."claim_payments"` (pay) — pre-aggregated subquery

**Join Logic:**
- `claims` INNER JOIN `policies` ON `cl.policy_id = p.policy_id`
- `claims` INNER JOIN `customers` ON `cl.customer_id = c.customer_id`
- `claims` LEFT JOIN `adjusters` ON `cl.adjuster_id = adj.adjuster_id`
- `claims` LEFT JOIN (aggregated `claim_payments`) ON `cl.claim_id = pay.claim_id`

**Payment Subquery:**
```sql
SELECT
    claim_id,
    SUM(amount) AS TOTAL_PAID,
    COUNT(*) AS PAYMENT_COUNT
FROM INSURANCE_RAW."insurance_db"."claim_payments"
WHERE _SNOWFLAKE_DELETED IS NOT TRUE
GROUP BY claim_id
```

**Filter:**
- `WHERE cl._SNOWFLAKE_DELETED IS NOT TRUE AND p._SNOWFLAKE_DELETED IS NOT TRUE AND c._SNOWFLAKE_DELETED IS NOT TRUE`

**Output Columns:**

| Column | Source | Expression |
|--------|--------|------------|
| CLAIM_ID | claims | cl.claim_id |
| CLAIM_NUMBER | claims | cl.claim_number |
| INCIDENT_DATE | claims | cl.incident_date |
| REPORTED_DATE | claims | cl.reported_date |
| DAYS_TO_REPORT | claims | `DATEDIFF('day', cl.incident_date, cl.reported_date)` |
| CLAIM_TYPE | claims | cl.claim_type |
| DESCRIPTION | claims | cl.description |
| ESTIMATED_AMOUNT | claims | cl.estimated_amount |
| APPROVED_AMOUNT | claims | cl.approved_amount |
| CLAIM_STATUS | claims | cl.status |
| PRIORITY | claims | cl.priority |
| FRAUD_FLAG | claims | cl.fraud_flag |
| POLICY_NUMBER | policies | p.policy_number |
| POLICY_ID | policies | p.policy_id |
| PRODUCT_TYPE | policies | p.product_type |
| COVERAGE_AMOUNT | policies | p.coverage_amount |
| DEDUCTIBLE | policies | p.deductible |
| PREMIUM_AMOUNT | policies | p.premium_amount |
| CUSTOMER_ID | customers | c.customer_id |
| CUSTOMER_NAME | customers | `c.first_name \|\| ' ' \|\| c.last_name` |
| CUSTOMER_STATE | customers | c.state |
| CUSTOMER_CITY | customers | c.city |
| ADJUSTER_NAME | adjusters | `adj.first_name \|\| ' ' \|\| adj.last_name` |
| ADJUSTER_REGION | adjusters | adj.region |
| TOTAL_PAID | claim_payments | `COALESCE(pay.TOTAL_PAID, 0)` |
| PAYMENT_COUNT | claim_payments | `COALESCE(pay.PAYMENT_COUNT, 0)` |
| PAYOUT_PCT | derived | `CASE WHEN cl.approved_amount > 0 THEN ROUND(COALESCE(pay.TOTAL_PAID, 0) / cl.approved_amount * 100, 2) ELSE 0 END` |

---

### 4. INSURANCE_CURATED.CLAIMS.V_CLAIM_PAYMENTS_SUMMARY

**Type:** View
**Purpose:** Payment details aggregated per claim with breakdowns by payment type and method.

**Sources:**
- `INSURANCE_RAW."insurance_db"."claim_payments"` (pay)
- `INSURANCE_RAW."insurance_db"."claims"` (cl)

**Join Logic:**
- `claim_payments` INNER JOIN `claims` ON `pay.claim_id = cl.claim_id`

**Filter:**
- `WHERE pay._SNOWFLAKE_DELETED IS NOT TRUE AND cl._SNOWFLAKE_DELETED IS NOT TRUE`

**Group By:** `pay.claim_id, cl.claim_number, cl.status`

**Output Columns:**

| Column | Expression |
|--------|------------|
| CLAIM_ID | pay.claim_id |
| CLAIM_NUMBER | cl.claim_number |
| CLAIM_STATUS | cl.status |
| TOTAL_PAID | `SUM(pay.amount)` |
| PAYMENT_COUNT | `COUNT(*)` |
| FIRST_PAYMENT_DATE | `MIN(pay.payment_date)` |
| LAST_PAYMENT_DATE | `MAX(pay.payment_date)` |
| DAYS_BETWEEN_FIRST_LAST | `DATEDIFF('day', MIN(pay.payment_date), MAX(pay.payment_date))` |
| PARTIAL_PAYMENTS | `SUM(CASE WHEN pay.payment_type = 'PARTIAL' THEN 1 ELSE 0 END)` |
| FINAL_PAYMENTS | `SUM(CASE WHEN pay.payment_type = 'FINAL' THEN 1 ELSE 0 END)` |
| SUPPLEMENT_PAYMENTS | `SUM(CASE WHEN pay.payment_type = 'SUPPLEMENT' THEN 1 ELSE 0 END)` |
| CHECK_AMOUNT | `SUM(CASE WHEN pay.payment_method = 'CHECK' THEN pay.amount ELSE 0 END)` |
| ACH_AMOUNT | `SUM(CASE WHEN pay.payment_method = 'ACH' THEN pay.amount ELSE 0 END)` |
| WIRE_AMOUNT | `SUM(CASE WHEN pay.payment_method = 'WIRE' THEN pay.amount ELSE 0 END)` |

---

### 5. INSURANCE_CURATED.COMMON.V_CUSTOMER_360

**Type:** View
**Purpose:** 360-degree customer view aggregating all policy, claims, and payment activity per customer. Used for customer-level analytics and risk profiling.

**Sources:**
- `INSURANCE_RAW."insurance_db"."customers"` (c)
- `INSURANCE_RAW."insurance_db"."policies"` (p)
- `INSURANCE_RAW."insurance_db"."claims"` (cl)
- `INSURANCE_RAW."insurance_db"."claim_payments"` (pay)

**Strategy:** Left join customer to pre-aggregated policy, claims, and payments subqueries.

**Policy Subquery (per customer):**
```sql
SELECT customer_id,
    COUNT(*) AS TOTAL_POLICIES,
    SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS ACTIVE_POLICIES,
    SUM(premium_amount) AS TOTAL_PREMIUM,
    SUM(coverage_amount) AS TOTAL_COVERAGE,
    AVG(risk_score) AS AVG_RISK_SCORE,
    MIN(effective_date) AS FIRST_POLICY_DATE,
    MAX(effective_date) AS LATEST_POLICY_DATE
FROM INSURANCE_RAW."insurance_db"."policies"
WHERE _SNOWFLAKE_DELETED IS NOT TRUE
GROUP BY customer_id
```

**Claims Subquery (per customer):**
```sql
SELECT customer_id,
    COUNT(*) AS TOTAL_CLAIMS,
    SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,
    SUM(CASE WHEN fraud_flag = 1 THEN 1 ELSE 0 END) AS FRAUD_FLAGS,
    SUM(estimated_amount) AS TOTAL_ESTIMATED,
    SUM(approved_amount) AS TOTAL_APPROVED
FROM INSURANCE_RAW."insurance_db"."claims"
WHERE _SNOWFLAKE_DELETED IS NOT TRUE
GROUP BY customer_id
```

**Filter:**
- `WHERE c._SNOWFLAKE_DELETED IS NOT TRUE`

**Output Columns:**

| Column | Source | Expression |
|--------|--------|------------|
| CUSTOMER_ID | customers | c.customer_id |
| CUSTOMER_NAME | customers | `c.first_name \|\| ' ' \|\| c.last_name` |
| DATE_OF_BIRTH | customers | c.date_of_birth |
| CUSTOMER_AGE | customers | `DATEDIFF('year', c.date_of_birth, CURRENT_DATE())` |
| GENDER | customers | c.gender |
| EMAIL | customers | c.email |
| PHONE | customers | c.phone |
| CITY | customers | c.city |
| STATE | customers | c.state |
| ZIP_CODE | customers | c.zip_code |
| CREDIT_SCORE | customers | c.credit_score |
| OCCUPATION | customers | c.occupation |
| ANNUAL_INCOME | customers | c.annual_income |
| TOTAL_POLICIES | policies agg | `COALESCE(pol.TOTAL_POLICIES, 0)` |
| ACTIVE_POLICIES | policies agg | `COALESCE(pol.ACTIVE_POLICIES, 0)` |
| TOTAL_PREMIUM | policies agg | `COALESCE(pol.TOTAL_PREMIUM, 0)` |
| TOTAL_COVERAGE | policies agg | `COALESCE(pol.TOTAL_COVERAGE, 0)` |
| AVG_RISK_SCORE | policies agg | pol.AVG_RISK_SCORE |
| FIRST_POLICY_DATE | policies agg | pol.FIRST_POLICY_DATE |
| LATEST_POLICY_DATE | policies agg | pol.LATEST_POLICY_DATE |
| TOTAL_CLAIMS | claims agg | `COALESCE(clm.TOTAL_CLAIMS, 0)` |
| OPEN_CLAIMS | claims agg | `COALESCE(clm.OPEN_CLAIMS, 0)` |
| FRAUD_FLAGS | claims agg | `COALESCE(clm.FRAUD_FLAGS, 0)` |
| TOTAL_ESTIMATED | claims agg | `COALESCE(clm.TOTAL_ESTIMATED, 0)` |
| TOTAL_APPROVED | claims agg | `COALESCE(clm.TOTAL_APPROVED, 0)` |
| LOSS_RATIO | derived | `CASE WHEN COALESCE(pol.TOTAL_PREMIUM, 0) > 0 THEN ROUND(COALESCE(clm.TOTAL_APPROVED, 0) / pol.TOTAL_PREMIUM * 100, 2) ELSE 0 END` |
| CUSTOMER_TENURE_DAYS | derived | `DATEDIFF('day', pol.FIRST_POLICY_DATE, CURRENT_DATE())` |

---

## INSURANCE_ANALYTICS Transformations

### 6. INSURANCE_ANALYTICS.DASHBOARDS.V_KPI_UNDERWRITING

**Type:** View
**Purpose:** Monthly underwriting KPIs by product type. Powers the Underwriting Analytics Streamlit page.

**Sources:**
- `INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE` (upstream curated view)

**Filter:**
- `WHERE DECISION_DATE IS NOT NULL`

**Group By:** `DATE_TRUNC('month', DECISION_DATE), PRODUCT_TYPE`

**Output Columns:**

| Column | Expression |
|--------|------------|
| MONTH | `DATE_TRUNC('month', DECISION_DATE)` |
| PRODUCT_TYPE | PRODUCT_TYPE |
| TOTAL_DECISIONS | `COUNT(*)` |
| APPROVED | `SUM(CASE WHEN DECISION = 'APPROVED' THEN 1 ELSE 0 END)` |
| DECLINED | `SUM(CASE WHEN DECISION = 'DECLINED' THEN 1 ELSE 0 END)` |
| REFERRED | `SUM(CASE WHEN DECISION = 'REFERRED' THEN 1 ELSE 0 END)` |
| COUNTER_OFFERS | `SUM(CASE WHEN DECISION = 'COUNTER_OFFER' THEN 1 ELSE 0 END)` |
| APPROVAL_RATE | `ROUND(AVG(CASE WHEN DECISION = 'APPROVED' THEN 1.0 ELSE 0.0 END) * 100, 2)` |
| AVG_RISK_SCORE | `AVG(DECISION_RISK_SCORE)` |
| AVG_COVERAGE | `AVG(COVERAGE_AMOUNT)` |
| AVG_PREMIUM | `AVG(PREMIUM_AMOUNT)` |
| TOTAL_PREMIUM | `SUM(PREMIUM_AMOUNT)` |
| TOTAL_COVERAGE | `SUM(COVERAGE_AMOUNT)` |
| AVG_CREDIT_SCORE | `AVG(CREDIT_SCORE)` |
| REVIEW_FLAG_COUNT | `SUM(CASE WHEN REVIEW_FLAG = 1 THEN 1 ELSE 0 END)` |

---

### 7. INSURANCE_ANALYTICS.DASHBOARDS.V_KPI_CLAIMS

**Type:** View
**Purpose:** Monthly claims KPIs by claim type and product type. Powers the Claims Analytics Streamlit page.

**Sources:**
- `INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL` (upstream curated view)

**Group By:** `DATE_TRUNC('month', REPORTED_DATE), CLAIM_TYPE, PRODUCT_TYPE`

**Output Columns:**

| Column | Expression |
|--------|------------|
| MONTH | `DATE_TRUNC('month', REPORTED_DATE)` |
| CLAIM_TYPE | CLAIM_TYPE |
| PRODUCT_TYPE | PRODUCT_TYPE |
| TOTAL_CLAIMS | `COUNT(*)` |
| TOTAL_ESTIMATED | `SUM(ESTIMATED_AMOUNT)` |
| TOTAL_APPROVED | `SUM(APPROVED_AMOUNT)` |
| TOTAL_PAID | `SUM(TOTAL_PAID)` |
| AVG_DAYS_TO_REPORT | `AVG(DAYS_TO_REPORT)` |
| OPEN_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'OPEN' THEN 1 ELSE 0 END)` |
| UNDER_REVIEW_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'UNDER_REVIEW' THEN 1 ELSE 0 END)` |
| APPROVED_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'APPROVED' THEN 1 ELSE 0 END)` |
| SETTLED_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'SETTLED' THEN 1 ELSE 0 END)` |
| DENIED_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'DENIED' THEN 1 ELSE 0 END)` |
| CLOSED_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'CLOSED' THEN 1 ELSE 0 END)` |
| FRAUD_FLAGS | `SUM(CASE WHEN FRAUD_FLAG = 1 THEN 1 ELSE 0 END)` |
| HIGH_PRIORITY_CLAIMS | `SUM(CASE WHEN PRIORITY IN ('HIGH', 'URGENT') THEN 1 ELSE 0 END)` |
| AVG_PAYOUT_PCT | `ROUND(AVG(CASE WHEN APPROVED_AMOUNT > 0 THEN TOTAL_PAID / APPROVED_AMOUNT ELSE NULL END) * 100, 2)` |

---

### 8. INSURANCE_ANALYTICS.DASHBOARDS.V_LOSS_RATIO

**Type:** View
**Purpose:** Monthly loss ratio (incurred losses / earned premium) by product type. Key insurance industry metric.

**Sources:**
- `INSURANCE_RAW."insurance_db"."claims"` (cl)
- `INSURANCE_RAW."insurance_db"."policies"` (p)

**Join Logic:**
- `claims` INNER JOIN `policies` ON `cl.policy_id = p.policy_id`

**Filter:**
- `WHERE cl._SNOWFLAKE_DELETED IS NOT TRUE AND p._SNOWFLAKE_DELETED IS NOT TRUE`

**Group By:** `DATE_TRUNC('month', cl.reported_date), p.product_type`

**Output Columns:**

| Column | Expression |
|--------|------------|
| MONTH | `DATE_TRUNC('month', cl.reported_date)` |
| PRODUCT_TYPE | p.product_type |
| TOTAL_CLAIMS | `COUNT(*)` |
| INCURRED_LOSSES | `SUM(cl.approved_amount)` |
| EARNED_PREMIUM | `SUM(p.premium_amount)` |
| LOSS_RATIO_PCT | `ROUND(CASE WHEN SUM(p.premium_amount) > 0 THEN SUM(cl.approved_amount) / SUM(p.premium_amount) * 100 ELSE 0 END, 2)` |
| AVG_CLAIM_AMOUNT | `AVG(cl.approved_amount)` |
| ESTIMATED_VS_APPROVED | `ROUND(CASE WHEN SUM(cl.estimated_amount) > 0 THEN SUM(cl.approved_amount) / SUM(cl.estimated_amount) * 100 ELSE 0 END, 2)` |

---

### 9. INSURANCE_ANALYTICS.DASHBOARDS.V_FRAUD_SUMMARY

**Type:** View
**Purpose:** Fraud analysis summary by product type, claim type, and time period. Powers fraud indicators on the Claims Analytics page.

**Sources:**
- `INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL` (upstream curated view)

**Filter:**
- `WHERE FRAUD_FLAG = 1`

**Group By:** `DATE_TRUNC('month', REPORTED_DATE), PRODUCT_TYPE, CLAIM_TYPE`

**Output Columns:**

| Column | Expression |
|--------|------------|
| MONTH | `DATE_TRUNC('month', REPORTED_DATE)` |
| PRODUCT_TYPE | PRODUCT_TYPE |
| CLAIM_TYPE | CLAIM_TYPE |
| FRAUD_CLAIM_COUNT | `COUNT(*)` |
| FRAUD_ESTIMATED_AMOUNT | `SUM(ESTIMATED_AMOUNT)` |
| FRAUD_APPROVED_AMOUNT | `SUM(APPROVED_AMOUNT)` |
| FRAUD_PAID_AMOUNT | `SUM(TOTAL_PAID)` |
| AVG_FRAUD_CLAIM_AMOUNT | `AVG(ESTIMATED_AMOUNT)` |
| FRAUD_OPEN_COUNT | `SUM(CASE WHEN CLAIM_STATUS = 'OPEN' THEN 1 ELSE 0 END)` |
| FRAUD_DENIED_COUNT | `SUM(CASE WHEN CLAIM_STATUS = 'DENIED' THEN 1 ELSE 0 END)` |

---

### 10. INSURANCE_ANALYTICS.DASHBOARDS.V_GEOGRAPHIC_SUMMARY

**Type:** View
**Purpose:** Claims and policy metrics aggregated by state. Powers the geographic heatmap on the Executive Summary page.

**Sources:**
- `INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL` (for claims metrics)
- `INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE` (for policy metrics)

**Strategy:** Two CTEs (one for claims by state, one for policies by state) joined on state.

**Policy CTE:**
```sql
SELECT
    CUSTOMER_STATE AS STATE,
    COUNT(*) AS TOTAL_POLICIES,
    SUM(CASE WHEN POLICY_STATUS = 'ACTIVE' THEN 1 ELSE 0 END) AS ACTIVE_POLICIES,
    SUM(PREMIUM_AMOUNT) AS TOTAL_PREMIUM,
    SUM(COVERAGE_AMOUNT) AS TOTAL_COVERAGE,
    AVG(POLICY_RISK_SCORE) AS AVG_RISK_SCORE
FROM INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE
GROUP BY CUSTOMER_STATE
```

**Claims CTE:**
```sql
SELECT
    CUSTOMER_STATE AS STATE,
    COUNT(*) AS TOTAL_CLAIMS,
    SUM(ESTIMATED_AMOUNT) AS TOTAL_ESTIMATED,
    SUM(APPROVED_AMOUNT) AS TOTAL_APPROVED,
    SUM(TOTAL_PAID) AS TOTAL_PAID,
    SUM(CASE WHEN FRAUD_FLAG = 1 THEN 1 ELSE 0 END) AS FRAUD_COUNT
FROM INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL
GROUP BY CUSTOMER_STATE
```

**Output Columns:**

| Column | Expression |
|--------|------------|
| STATE | COALESCE(pol.STATE, clm.STATE) |
| TOTAL_POLICIES | COALESCE(pol.TOTAL_POLICIES, 0) |
| ACTIVE_POLICIES | COALESCE(pol.ACTIVE_POLICIES, 0) |
| TOTAL_PREMIUM | COALESCE(pol.TOTAL_PREMIUM, 0) |
| TOTAL_COVERAGE | COALESCE(pol.TOTAL_COVERAGE, 0) |
| AVG_RISK_SCORE | pol.AVG_RISK_SCORE |
| TOTAL_CLAIMS | COALESCE(clm.TOTAL_CLAIMS, 0) |
| TOTAL_ESTIMATED | COALESCE(clm.TOTAL_ESTIMATED, 0) |
| TOTAL_APPROVED | COALESCE(clm.TOTAL_APPROVED, 0) |
| TOTAL_PAID | COALESCE(clm.TOTAL_PAID, 0) |
| FRAUD_COUNT | COALESCE(clm.FRAUD_COUNT, 0) |
| LOSS_RATIO_PCT | `ROUND(CASE WHEN COALESCE(pol.TOTAL_PREMIUM, 0) > 0 THEN COALESCE(clm.TOTAL_APPROVED, 0) / pol.TOTAL_PREMIUM * 100 ELSE 0 END, 2)` |

---

### 11. INSURANCE_ANALYTICS.DASHBOARDS.V_UNDERWRITER_PERFORMANCE

**Type:** View
**Purpose:** Performance metrics per underwriter. Powers underwriter leaderboard on the Underwriting Analytics page.

**Sources:**
- `INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE` (upstream curated view)

**Filter:**
- `WHERE UNDERWRITER_NAME IS NOT NULL`

**Group By:** `UNDERWRITER_NAME, UW_SPECIALIZATION, UW_EXPERIENCE`

**Output Columns:**

| Column | Expression |
|--------|------------|
| UNDERWRITER_NAME | UNDERWRITER_NAME |
| UW_SPECIALIZATION | UW_SPECIALIZATION |
| UW_EXPERIENCE | UW_EXPERIENCE |
| TOTAL_DECISIONS | `COUNT(*)` |
| APPROVED_COUNT | `SUM(CASE WHEN DECISION = 'APPROVED' THEN 1 ELSE 0 END)` |
| DECLINED_COUNT | `SUM(CASE WHEN DECISION = 'DECLINED' THEN 1 ELSE 0 END)` |
| REFERRED_COUNT | `SUM(CASE WHEN DECISION = 'REFERRED' THEN 1 ELSE 0 END)` |
| APPROVAL_RATE | `ROUND(AVG(CASE WHEN DECISION = 'APPROVED' THEN 1.0 ELSE 0.0 END) * 100, 2)` |
| AVG_RISK_SCORE | `AVG(DECISION_RISK_SCORE)` |
| AVG_COVERAGE_AMOUNT | `AVG(COVERAGE_AMOUNT)` |
| TOTAL_PREMIUM_WRITTEN | `SUM(CASE WHEN DECISION = 'APPROVED' THEN PREMIUM_AMOUNT ELSE 0 END)` |
| TOTAL_COVERAGE_WRITTEN | `SUM(CASE WHEN DECISION = 'APPROVED' THEN COVERAGE_AMOUNT ELSE 0 END)` |
| AVG_PREMIUM_ADJUSTMENT | `AVG(PREMIUM_ADJUSTMENT_PCT)` |
| REVIEW_FLAG_RATE | `ROUND(AVG(CASE WHEN REVIEW_FLAG = 1 THEN 1.0 ELSE 0.0 END) * 100, 2)` |
| HIGH_RISK_DECISIONS | `SUM(CASE WHEN RISK_CATEGORY IN ('HIGH', 'VERY_HIGH') THEN 1 ELSE 0 END)` |

---

### 12. INSURANCE_ANALYTICS.DASHBOARDS.V_ADJUSTER_PERFORMANCE

**Type:** View
**Purpose:** Performance metrics per claims adjuster. Powers adjuster workload analysis on the Claims Analytics page.

**Sources:**
- `INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL` (upstream curated view)

**Filter:**
- `WHERE ADJUSTER_NAME IS NOT NULL`

**Group By:** `ADJUSTER_NAME, ADJUSTER_REGION`

**Output Columns:**

| Column | Expression |
|--------|------------|
| ADJUSTER_NAME | ADJUSTER_NAME |
| ADJUSTER_REGION | ADJUSTER_REGION |
| TOTAL_CLAIMS | `COUNT(*)` |
| OPEN_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'OPEN' THEN 1 ELSE 0 END)` |
| SETTLED_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'SETTLED' THEN 1 ELSE 0 END)` |
| DENIED_CLAIMS | `SUM(CASE WHEN CLAIM_STATUS = 'DENIED' THEN 1 ELSE 0 END)` |
| SETTLEMENT_RATE | `ROUND(AVG(CASE WHEN CLAIM_STATUS IN ('SETTLED', 'CLOSED') THEN 1.0 ELSE 0.0 END) * 100, 2)` |
| TOTAL_ESTIMATED | `SUM(ESTIMATED_AMOUNT)` |
| TOTAL_APPROVED | `SUM(APPROVED_AMOUNT)` |
| TOTAL_PAID | `SUM(TOTAL_PAID)` |
| AVG_CLAIM_AMOUNT | `AVG(ESTIMATED_AMOUNT)` |
| AVG_DAYS_TO_REPORT | `AVG(DAYS_TO_REPORT)` |
| FRAUD_FLAGS | `SUM(CASE WHEN FRAUD_FLAG = 1 THEN 1 ELSE 0 END)` |
| HIGH_PRIORITY_CLAIMS | `SUM(CASE WHEN PRIORITY IN ('HIGH', 'URGENT') THEN 1 ELSE 0 END)` |
| AVG_PAYOUT_PCT | `ROUND(AVG(PAYOUT_PCT), 2)` |

---

## Transformation Dependency Graph

```
Layer 1: Source (INSURANCE_RAW)
  "customers", "policies", "underwriting_decisions", "underwriters",
  "claims", "claim_payments", "adjusters", "risk_factors"
    │
    ▼
Layer 2: Curated (INSURANCE_CURATED)
  ┌─ UNDERWRITING.V_UNDERWRITING_PIPELINE  (policies + customers + decisions + underwriters)
  ├─ UNDERWRITING.V_RISK_FACTOR_SUMMARY    (risk_factors aggregated)
  ├─ CLAIMS.V_CLAIMS_DETAIL               (claims + policies + customers + adjusters + payments)
  ├─ CLAIMS.V_CLAIM_PAYMENTS_SUMMARY      (claim_payments + claims aggregated)
  └─ COMMON.V_CUSTOMER_360                (customers + policies + claims aggregated)
    │
    ▼
Layer 3: Analytics (INSURANCE_ANALYTICS)
  ┌─ DASHBOARDS.V_KPI_UNDERWRITING         (from V_UNDERWRITING_PIPELINE)
  ├─ DASHBOARDS.V_KPI_CLAIMS              (from V_CLAIMS_DETAIL)
  ├─ DASHBOARDS.V_LOSS_RATIO              (from RAW claims + policies directly)
  ├─ DASHBOARDS.V_FRAUD_SUMMARY           (from V_CLAIMS_DETAIL, filtered fraud_flag=1)
  ├─ DASHBOARDS.V_GEOGRAPHIC_SUMMARY      (from V_CLAIMS_DETAIL + V_UNDERWRITING_PIPELINE)
  ├─ DASHBOARDS.V_UNDERWRITER_PERFORMANCE (from V_UNDERWRITING_PIPELINE)
  └─ DASHBOARDS.V_ADJUSTER_PERFORMANCE    (from V_CLAIMS_DETAIL)
```

---

## Summary

| Layer | Schema | Object | Type | Source Tables / Views |
|-------|--------|--------|------|----------------------|
| CURATED | UNDERWRITING | V_UNDERWRITING_PIPELINE | View | policies, customers, underwriting_decisions, underwriters |
| CURATED | UNDERWRITING | V_RISK_FACTOR_SUMMARY | View | risk_factors |
| CURATED | CLAIMS | V_CLAIMS_DETAIL | View | claims, policies, customers, adjusters, claim_payments |
| CURATED | CLAIMS | V_CLAIM_PAYMENTS_SUMMARY | View | claim_payments, claims |
| CURATED | COMMON | V_CUSTOMER_360 | View | customers, policies, claims |
| ANALYTICS | DASHBOARDS | V_KPI_UNDERWRITING | View | V_UNDERWRITING_PIPELINE |
| ANALYTICS | DASHBOARDS | V_KPI_CLAIMS | View | V_CLAIMS_DETAIL |
| ANALYTICS | DASHBOARDS | V_LOSS_RATIO | View | claims (RAW), policies (RAW) |
| ANALYTICS | DASHBOARDS | V_FRAUD_SUMMARY | View | V_CLAIMS_DETAIL |
| ANALYTICS | DASHBOARDS | V_GEOGRAPHIC_SUMMARY | View | V_CLAIMS_DETAIL, V_UNDERWRITING_PIPELINE |
| ANALYTICS | DASHBOARDS | V_UNDERWRITER_PERFORMANCE | View | V_UNDERWRITING_PIPELINE |
| ANALYTICS | DASHBOARDS | V_ADJUSTER_PERFORMANCE | View | V_CLAIMS_DETAIL |

**Total: 12 views** (5 curated + 7 analytics)

---

## Notes for Coalesce Implementation

1. **Quoted Identifiers Required:** All source references must use lowercase quoted identifiers: `INSURANCE_RAW."insurance_db"."table_name"`
2. **CDC Soft-Delete Filter:** All curated views must filter `WHERE _SNOWFLAKE_DELETED IS NOT TRUE` on source tables
3. **CDC Metadata Columns:** `_SNOWFLAKE_INSERTED_AT`, `_SNOWFLAKE_UPDATED_AT`, `_SNOWFLAKE_DELETED` should be excluded from output columns unless needed for audit
4. **View Dependencies:** Analytics views depend on curated views. Build curated layer first.
5. **Data Types:** `fraud_flag` and `review_flag` and `active` are NUMBER (0/1), not BOOLEAN. Treat as integer in CASE expressions.
6. **Warehouse:** Use the designated project warehouse for all operations
7. **Role:** Use the role that owns the source database (INSURANCE_RAW)
