#!/usr/bin/env python3
"""
Build all 12 transformation View nodes in Coalesce workspace via REST API.
Uses overrideSQL=true with full CREATE OR REPLACE VIEW SQL for each node.

Storage Location Mappings:
  TARGET1 = INSURANCE_RAW."insurance_db"       (source)
  TARGET2 = INSURANCE_CURATED.COMMON
  TARGET3 = INSURANCE_CURATED.UNDERWRITING
  TARGET4 = INSURANCE_ANALYTICS.DASHBOARDS
  TARGET  = INSURANCE_CURATED.CLAIMS
"""

import os
import sys
import json
import time
import requests
from pathlib import Path

ENV_FILE = Path(__file__).parent.parent / ".env"

def load_env():
    env = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    return env

env = load_env()
BASE_URL = env.get("COALESCE_BASE_URL", "https://app.coalescesoftware.io")
TOKEN = env.get("COALESCE_TOKEN", "")
WORKSPACE_ID = env.get("COALESCE_WORKSPACE_ID", "")
ENVIRONMENT_ID = env.get("COALESCE_ENVIRONMENT_ID", "")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

API = f"{BASE_URL}/api/v1/workspaces/{WORKSPACE_ID}"

RAW = 'INSURANCE_RAW."insurance_db"'

VIEWS = [
    {
        "name": "V_UNDERWRITING_PIPELINE",
        "location": "TARGET3",
        "description": "Complete underwriting pipeline joining policies with customer demographics, underwriting decisions, and underwriter details.",
        "sql": f"""CREATE OR REPLACE VIEW INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE AS
SELECT
    p."policy_id" AS POLICY_ID,
    p."policy_number" AS POLICY_NUMBER,
    p."product_type" AS PRODUCT_TYPE,
    p."coverage_amount" AS COVERAGE_AMOUNT,
    p."premium_amount" AS PREMIUM_AMOUNT,
    p."deductible" AS DEDUCTIBLE,
    p."effective_date" AS EFFECTIVE_DATE,
    p."expiration_date" AS EXPIRATION_DATE,
    p."status" AS POLICY_STATUS,
    p."underwriting_status" AS UNDERWRITING_STATUS,
    p."risk_score" AS POLICY_RISK_SCORE,
    c."customer_id" AS CUSTOMER_ID,
    c."first_name" || ' ' || c."last_name" AS CUSTOMER_NAME,
    c."state" AS CUSTOMER_STATE,
    c."credit_score" AS CREDIT_SCORE,
    c."annual_income" AS ANNUAL_INCOME,
    c."occupation" AS OCCUPATION,
    DATEDIFF('year', c."date_of_birth", CURRENT_DATE()) AS CUSTOMER_AGE,
    c."date_of_birth" AS DATE_OF_BIRTH,
    ud."decision" AS DECISION,
    ud."risk_category" AS RISK_CATEGORY,
    ud."risk_score" AS DECISION_RISK_SCORE,
    ud."premium_adjustment_pct" AS PREMIUM_ADJUSTMENT_PCT,
    ud."conditions" AS CONDITIONS,
    ud."decision_date" AS DECISION_DATE,
    ud."review_flag" AS REVIEW_FLAG,
    uw."first_name" || ' ' || uw."last_name" AS UNDERWRITER_NAME,
    uw."specialization" AS UW_SPECIALIZATION,
    uw."experience_years" AS UW_EXPERIENCE
FROM {RAW}."policies" p
INNER JOIN {RAW}."customers" c ON p."customer_id" = c."customer_id"
LEFT JOIN {RAW}."underwriting_decisions" ud ON p."policy_id" = ud."policy_id"
LEFT JOIN {RAW}."underwriters" uw ON ud."underwriter_id" = uw."underwriter_id"
WHERE (p._SNOWFLAKE_DELETED IS NULL OR p._SNOWFLAKE_DELETED = FALSE)
  AND (c._SNOWFLAKE_DELETED IS NULL OR c._SNOWFLAKE_DELETED = FALSE)"""
    },
    {
        "name": "V_RISK_FACTOR_SUMMARY",
        "location": "TARGET3",
        "description": "Aggregated risk factors per policy with count, positive/negative split, average/max impact, and detail string.",
        "sql": f"""CREATE OR REPLACE VIEW INSURANCE_CURATED.UNDERWRITING.V_RISK_FACTOR_SUMMARY AS
SELECT
    rf."policy_id" AS POLICY_ID,
    COUNT(*) AS TOTAL_FACTORS,
    SUM(CASE WHEN rf."impact_score" > 0 THEN 1 ELSE 0 END) AS NEGATIVE_FACTORS,
    SUM(CASE WHEN rf."impact_score" <= 0 THEN 1 ELSE 0 END) AS POSITIVE_FACTORS,
    AVG(rf."impact_score") AS AVG_IMPACT,
    MAX(rf."impact_score") AS MAX_IMPACT,
    LISTAGG(rf."factor_type" || ': ' || rf."factor_value", '; ') WITHIN GROUP (ORDER BY ABS(rf."impact_score") DESC) AS FACTOR_DETAILS
FROM {RAW}."risk_factors" rf
WHERE (rf._SNOWFLAKE_DELETED IS NULL OR rf._SNOWFLAKE_DELETED = FALSE)
GROUP BY rf."policy_id\""""
    },
    {
        "name": "V_CLAIMS_DETAIL",
        "location": "TARGET",
        "description": "Complete claims view joining claims with policy, customer, adjuster, and payment aggregates.",
        "sql": f"""CREATE OR REPLACE VIEW INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL AS
SELECT
    cl."claim_id" AS CLAIM_ID,
    cl."claim_number" AS CLAIM_NUMBER,
    cl."incident_date" AS INCIDENT_DATE,
    cl."reported_date" AS REPORTED_DATE,
    DATEDIFF('day', cl."incident_date", cl."reported_date") AS DAYS_TO_REPORT,
    cl."claim_type" AS CLAIM_TYPE,
    cl."description" AS DESCRIPTION,
    cl."estimated_amount" AS ESTIMATED_AMOUNT,
    cl."approved_amount" AS APPROVED_AMOUNT,
    cl."status" AS CLAIM_STATUS,
    cl."priority" AS PRIORITY,
    cl."fraud_flag" AS FRAUD_FLAG,
    p."policy_number" AS POLICY_NUMBER,
    p."policy_id" AS POLICY_ID,
    p."product_type" AS PRODUCT_TYPE,
    p."coverage_amount" AS COVERAGE_AMOUNT,
    p."deductible" AS DEDUCTIBLE,
    p."premium_amount" AS PREMIUM_AMOUNT,
    c."customer_id" AS CUSTOMER_ID,
    c."first_name" || ' ' || c."last_name" AS CUSTOMER_NAME,
    c."state" AS CUSTOMER_STATE,
    c."city" AS CUSTOMER_CITY,
    adj."first_name" || ' ' || adj."last_name" AS ADJUSTER_NAME,
    adj."region" AS ADJUSTER_REGION,
    COALESCE(pay.TOTAL_PAID, 0) AS TOTAL_PAID,
    COALESCE(pay.PAYMENT_COUNT, 0) AS PAYMENT_COUNT,
    CASE WHEN cl."approved_amount" > 0
         THEN ROUND(COALESCE(pay.TOTAL_PAID, 0) / cl."approved_amount" * 100, 2)
         ELSE 0 END AS PAYOUT_PCT
FROM {RAW}."claims" cl
INNER JOIN {RAW}."policies" p ON cl."policy_id" = p."policy_id"
INNER JOIN {RAW}."customers" c ON cl."customer_id" = c."customer_id"
LEFT JOIN {RAW}."adjusters" adj ON cl."adjuster_id" = adj."adjuster_id"
LEFT JOIN (
    SELECT
        "claim_id" AS claim_id,
        SUM("amount") AS TOTAL_PAID,
        COUNT(*) AS PAYMENT_COUNT
    FROM {RAW}."claim_payments"
    WHERE (_SNOWFLAKE_DELETED IS NULL OR _SNOWFLAKE_DELETED = FALSE)
    GROUP BY "claim_id"
) pay ON cl."claim_id" = pay.claim_id
WHERE (cl._SNOWFLAKE_DELETED IS NULL OR cl._SNOWFLAKE_DELETED = FALSE)
  AND (p._SNOWFLAKE_DELETED IS NULL OR p._SNOWFLAKE_DELETED = FALSE)
  AND (c._SNOWFLAKE_DELETED IS NULL OR c._SNOWFLAKE_DELETED = FALSE)"""
    },
    {
        "name": "V_CLAIM_PAYMENTS_SUMMARY",
        "location": "TARGET",
        "description": "Payment details aggregated per claim with breakdowns by payment type and method.",
        "sql": f"""CREATE OR REPLACE VIEW INSURANCE_CURATED.CLAIMS.V_CLAIM_PAYMENTS_SUMMARY AS
SELECT
    pay."claim_id" AS CLAIM_ID,
    cl."claim_number" AS CLAIM_NUMBER,
    cl."status" AS CLAIM_STATUS,
    SUM(pay."amount") AS TOTAL_PAID,
    COUNT(*) AS PAYMENT_COUNT,
    MIN(pay."payment_date") AS FIRST_PAYMENT_DATE,
    MAX(pay."payment_date") AS LAST_PAYMENT_DATE,
    DATEDIFF('day', MIN(pay."payment_date"), MAX(pay."payment_date")) AS DAYS_BETWEEN_FIRST_LAST,
    SUM(CASE WHEN pay."payment_type" = 'PARTIAL' THEN 1 ELSE 0 END) AS PARTIAL_PAYMENTS,
    SUM(CASE WHEN pay."payment_type" = 'FINAL' THEN 1 ELSE 0 END) AS FINAL_PAYMENTS,
    SUM(CASE WHEN pay."payment_type" = 'SUPPLEMENT' THEN 1 ELSE 0 END) AS SUPPLEMENT_PAYMENTS,
    SUM(CASE WHEN pay."payment_method" = 'CHECK' THEN pay."amount" ELSE 0 END) AS CHECK_AMOUNT,
    SUM(CASE WHEN pay."payment_method" = 'ACH' THEN pay."amount" ELSE 0 END) AS ACH_AMOUNT,
    SUM(CASE WHEN pay."payment_method" = 'WIRE' THEN pay."amount" ELSE 0 END) AS WIRE_AMOUNT
FROM {RAW}."claim_payments" pay
INNER JOIN {RAW}."claims" cl ON pay."claim_id" = cl."claim_id"
WHERE (pay._SNOWFLAKE_DELETED IS NULL OR pay._SNOWFLAKE_DELETED = FALSE)
  AND (cl._SNOWFLAKE_DELETED IS NULL OR cl._SNOWFLAKE_DELETED = FALSE)
GROUP BY pay."claim_id", cl."claim_number", cl."status\""""
    },
    {
        "name": "V_CUSTOMER_360",
        "location": "TARGET2",
        "description": "360-degree customer view aggregating all policy, claims, and payment activity per customer.",
        "sql": f"""CREATE OR REPLACE VIEW INSURANCE_CURATED.COMMON.V_CUSTOMER_360 AS
SELECT
    c."customer_id" AS CUSTOMER_ID,
    c."first_name" || ' ' || c."last_name" AS CUSTOMER_NAME,
    c."date_of_birth" AS DATE_OF_BIRTH,
    DATEDIFF('year', c."date_of_birth", CURRENT_DATE()) AS CUSTOMER_AGE,
    c."gender" AS GENDER,
    c."email" AS EMAIL,
    c."phone" AS PHONE,
    c."city" AS CITY,
    c."state" AS STATE,
    c."zip_code" AS ZIP_CODE,
    c."credit_score" AS CREDIT_SCORE,
    c."occupation" AS OCCUPATION,
    c."annual_income" AS ANNUAL_INCOME,
    COALESCE(pol.TOTAL_POLICIES, 0) AS TOTAL_POLICIES,
    COALESCE(pol.ACTIVE_POLICIES, 0) AS ACTIVE_POLICIES,
    COALESCE(pol.TOTAL_PREMIUM, 0) AS TOTAL_PREMIUM,
    COALESCE(pol.TOTAL_COVERAGE, 0) AS TOTAL_COVERAGE,
    pol.AVG_RISK_SCORE,
    pol.FIRST_POLICY_DATE,
    pol.LATEST_POLICY_DATE,
    COALESCE(clm.TOTAL_CLAIMS, 0) AS TOTAL_CLAIMS,
    COALESCE(clm.OPEN_CLAIMS, 0) AS OPEN_CLAIMS,
    COALESCE(clm.FRAUD_FLAGS, 0) AS FRAUD_FLAGS,
    COALESCE(clm.TOTAL_ESTIMATED, 0) AS TOTAL_ESTIMATED,
    COALESCE(clm.TOTAL_APPROVED, 0) AS TOTAL_APPROVED,
    CASE WHEN COALESCE(pol.TOTAL_PREMIUM, 0) > 0
         THEN ROUND(COALESCE(clm.TOTAL_APPROVED, 0) / pol.TOTAL_PREMIUM * 100, 2)
         ELSE 0 END AS LOSS_RATIO,
    DATEDIFF('day', pol.FIRST_POLICY_DATE, CURRENT_DATE()) AS CUSTOMER_TENURE_DAYS
FROM {RAW}."customers" c
LEFT JOIN (
    SELECT "customer_id" AS customer_id,
        COUNT(*) AS TOTAL_POLICIES,
        SUM(CASE WHEN "status" = 'ACTIVE' THEN 1 ELSE 0 END) AS ACTIVE_POLICIES,
        SUM("premium_amount") AS TOTAL_PREMIUM,
        SUM("coverage_amount") AS TOTAL_COVERAGE,
        AVG("risk_score") AS AVG_RISK_SCORE,
        MIN("effective_date") AS FIRST_POLICY_DATE,
        MAX("effective_date") AS LATEST_POLICY_DATE
    FROM {RAW}."policies"
    WHERE (_SNOWFLAKE_DELETED IS NULL OR _SNOWFLAKE_DELETED = FALSE)
    GROUP BY "customer_id"
) pol ON c."customer_id" = pol.customer_id
LEFT JOIN (
    SELECT "customer_id" AS customer_id,
        COUNT(*) AS TOTAL_CLAIMS,
        SUM(CASE WHEN "status" = 'OPEN' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,
        SUM(CASE WHEN "fraud_flag" = 1 THEN 1 ELSE 0 END) AS FRAUD_FLAGS,
        SUM("estimated_amount") AS TOTAL_ESTIMATED,
        SUM("approved_amount") AS TOTAL_APPROVED
    FROM {RAW}."claims"
    WHERE (_SNOWFLAKE_DELETED IS NULL OR _SNOWFLAKE_DELETED = FALSE)
    GROUP BY "customer_id"
) clm ON c."customer_id" = clm.customer_id
WHERE (c._SNOWFLAKE_DELETED IS NULL OR c._SNOWFLAKE_DELETED = FALSE)"""
    },
    {
        "name": "V_KPI_UNDERWRITING",
        "location": "TARGET4",
        "description": "Monthly underwriting KPIs by product type.",
        "sql": """CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_KPI_UNDERWRITING AS
SELECT
    DATE_TRUNC('month', DECISION_DATE) AS MONTH,
    PRODUCT_TYPE,
    COUNT(*) AS TOTAL_DECISIONS,
    SUM(CASE WHEN DECISION = 'APPROVED' THEN 1 ELSE 0 END) AS APPROVED,
    SUM(CASE WHEN DECISION = 'DECLINED' THEN 1 ELSE 0 END) AS DECLINED,
    SUM(CASE WHEN DECISION = 'REFERRED' THEN 1 ELSE 0 END) AS REFERRED,
    SUM(CASE WHEN DECISION = 'COUNTER_OFFER' THEN 1 ELSE 0 END) AS COUNTER_OFFERS,
    ROUND(AVG(CASE WHEN DECISION = 'APPROVED' THEN 1.0 ELSE 0.0 END) * 100, 2) AS APPROVAL_RATE,
    AVG(DECISION_RISK_SCORE) AS AVG_RISK_SCORE,
    AVG(COVERAGE_AMOUNT) AS AVG_COVERAGE,
    AVG(PREMIUM_AMOUNT) AS AVG_PREMIUM,
    SUM(PREMIUM_AMOUNT) AS TOTAL_PREMIUM,
    SUM(COVERAGE_AMOUNT) AS TOTAL_COVERAGE,
    AVG(CREDIT_SCORE) AS AVG_CREDIT_SCORE,
    SUM(CASE WHEN REVIEW_FLAG = 1 THEN 1 ELSE 0 END) AS REVIEW_FLAG_COUNT
FROM INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE
WHERE DECISION_DATE IS NOT NULL
GROUP BY DATE_TRUNC('month', DECISION_DATE), PRODUCT_TYPE"""
    },
    {
        "name": "V_KPI_CLAIMS",
        "location": "TARGET4",
        "description": "Monthly claims KPIs by claim type and product type.",
        "sql": """CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_KPI_CLAIMS AS
SELECT
    DATE_TRUNC('month', REPORTED_DATE) AS MONTH,
    CLAIM_TYPE,
    PRODUCT_TYPE,
    COUNT(*) AS TOTAL_CLAIMS,
    SUM(ESTIMATED_AMOUNT) AS TOTAL_ESTIMATED,
    SUM(APPROVED_AMOUNT) AS TOTAL_APPROVED,
    SUM(TOTAL_PAID) AS TOTAL_PAID,
    AVG(DAYS_TO_REPORT) AS AVG_DAYS_TO_REPORT,
    SUM(CASE WHEN CLAIM_STATUS = 'OPEN' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'UNDER_REVIEW' THEN 1 ELSE 0 END) AS UNDER_REVIEW_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'APPROVED' THEN 1 ELSE 0 END) AS APPROVED_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'SETTLED' THEN 1 ELSE 0 END) AS SETTLED_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'DENIED' THEN 1 ELSE 0 END) AS DENIED_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'CLOSED' THEN 1 ELSE 0 END) AS CLOSED_CLAIMS,
    SUM(CASE WHEN FRAUD_FLAG = 1 THEN 1 ELSE 0 END) AS FRAUD_FLAGS,
    SUM(CASE WHEN PRIORITY IN ('HIGH', 'URGENT') THEN 1 ELSE 0 END) AS HIGH_PRIORITY_CLAIMS,
    ROUND(AVG(CASE WHEN APPROVED_AMOUNT > 0 THEN TOTAL_PAID / APPROVED_AMOUNT ELSE NULL END) * 100, 2) AS AVG_PAYOUT_PCT
FROM INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL
GROUP BY DATE_TRUNC('month', REPORTED_DATE), CLAIM_TYPE, PRODUCT_TYPE"""
    },
    {
        "name": "V_LOSS_RATIO",
        "location": "TARGET4",
        "description": "Monthly loss ratio (incurred losses / earned premium) by product type.",
        "sql": f"""CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_LOSS_RATIO AS
SELECT
    DATE_TRUNC('month', cl."reported_date") AS MONTH,
    p."product_type" AS PRODUCT_TYPE,
    COUNT(*) AS TOTAL_CLAIMS,
    SUM(cl."approved_amount") AS INCURRED_LOSSES,
    SUM(p."premium_amount") AS EARNED_PREMIUM,
    ROUND(CASE WHEN SUM(p."premium_amount") > 0
               THEN SUM(cl."approved_amount") / SUM(p."premium_amount") * 100
               ELSE 0 END, 2) AS LOSS_RATIO_PCT,
    AVG(cl."approved_amount") AS AVG_CLAIM_AMOUNT,
    ROUND(CASE WHEN SUM(cl."estimated_amount") > 0
               THEN SUM(cl."approved_amount") / SUM(cl."estimated_amount") * 100
               ELSE 0 END, 2) AS ESTIMATED_VS_APPROVED
FROM {RAW}."claims" cl
INNER JOIN {RAW}."policies" p ON cl."policy_id" = p."policy_id"
WHERE (cl._SNOWFLAKE_DELETED IS NULL OR cl._SNOWFLAKE_DELETED = FALSE)
  AND (p._SNOWFLAKE_DELETED IS NULL OR p._SNOWFLAKE_DELETED = FALSE)
GROUP BY DATE_TRUNC('month', cl."reported_date"), p."product_type\""""
    },
    {
        "name": "V_FRAUD_SUMMARY",
        "location": "TARGET4",
        "description": "Fraud analysis summary by product type, claim type, and time period.",
        "sql": """CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_FRAUD_SUMMARY AS
SELECT
    DATE_TRUNC('month', REPORTED_DATE) AS MONTH,
    PRODUCT_TYPE,
    CLAIM_TYPE,
    COUNT(*) AS FRAUD_CLAIM_COUNT,
    SUM(ESTIMATED_AMOUNT) AS FRAUD_ESTIMATED_AMOUNT,
    SUM(APPROVED_AMOUNT) AS FRAUD_APPROVED_AMOUNT,
    SUM(TOTAL_PAID) AS FRAUD_PAID_AMOUNT,
    AVG(ESTIMATED_AMOUNT) AS AVG_FRAUD_CLAIM_AMOUNT,
    SUM(CASE WHEN CLAIM_STATUS = 'OPEN' THEN 1 ELSE 0 END) AS FRAUD_OPEN_COUNT,
    SUM(CASE WHEN CLAIM_STATUS = 'DENIED' THEN 1 ELSE 0 END) AS FRAUD_DENIED_COUNT
FROM INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL
WHERE FRAUD_FLAG = 1
GROUP BY DATE_TRUNC('month', REPORTED_DATE), PRODUCT_TYPE, CLAIM_TYPE"""
    },
    {
        "name": "V_GEOGRAPHIC_SUMMARY",
        "location": "TARGET4",
        "description": "Claims and policy metrics aggregated by state.",
        "sql": """CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_GEOGRAPHIC_SUMMARY AS
WITH pol AS (
    SELECT
        CUSTOMER_STATE AS STATE,
        COUNT(*) AS TOTAL_POLICIES,
        SUM(CASE WHEN POLICY_STATUS = 'ACTIVE' THEN 1 ELSE 0 END) AS ACTIVE_POLICIES,
        SUM(PREMIUM_AMOUNT) AS TOTAL_PREMIUM,
        SUM(COVERAGE_AMOUNT) AS TOTAL_COVERAGE,
        AVG(POLICY_RISK_SCORE) AS AVG_RISK_SCORE
    FROM INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE
    GROUP BY CUSTOMER_STATE
),
clm AS (
    SELECT
        CUSTOMER_STATE AS STATE,
        COUNT(*) AS TOTAL_CLAIMS,
        SUM(ESTIMATED_AMOUNT) AS TOTAL_ESTIMATED,
        SUM(APPROVED_AMOUNT) AS TOTAL_APPROVED,
        SUM(TOTAL_PAID) AS TOTAL_PAID,
        SUM(CASE WHEN FRAUD_FLAG = 1 THEN 1 ELSE 0 END) AS FRAUD_COUNT
    FROM INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL
    GROUP BY CUSTOMER_STATE
)
SELECT
    COALESCE(pol.STATE, clm.STATE) AS STATE,
    COALESCE(pol.TOTAL_POLICIES, 0) AS TOTAL_POLICIES,
    COALESCE(pol.ACTIVE_POLICIES, 0) AS ACTIVE_POLICIES,
    COALESCE(pol.TOTAL_PREMIUM, 0) AS TOTAL_PREMIUM,
    COALESCE(pol.TOTAL_COVERAGE, 0) AS TOTAL_COVERAGE,
    pol.AVG_RISK_SCORE,
    COALESCE(clm.TOTAL_CLAIMS, 0) AS TOTAL_CLAIMS,
    COALESCE(clm.TOTAL_ESTIMATED, 0) AS TOTAL_ESTIMATED,
    COALESCE(clm.TOTAL_APPROVED, 0) AS TOTAL_APPROVED,
    COALESCE(clm.TOTAL_PAID, 0) AS TOTAL_PAID,
    COALESCE(clm.FRAUD_COUNT, 0) AS FRAUD_COUNT,
    ROUND(CASE WHEN COALESCE(pol.TOTAL_PREMIUM, 0) > 0
               THEN COALESCE(clm.TOTAL_APPROVED, 0) / pol.TOTAL_PREMIUM * 100
               ELSE 0 END, 2) AS LOSS_RATIO_PCT
FROM pol
FULL OUTER JOIN clm ON pol.STATE = clm.STATE"""
    },
    {
        "name": "V_UNDERWRITER_PERFORMANCE",
        "location": "TARGET4",
        "description": "Performance metrics per underwriter.",
        "sql": """CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_UNDERWRITER_PERFORMANCE AS
SELECT
    UNDERWRITER_NAME,
    UW_SPECIALIZATION,
    UW_EXPERIENCE,
    COUNT(*) AS TOTAL_DECISIONS,
    SUM(CASE WHEN DECISION = 'APPROVED' THEN 1 ELSE 0 END) AS APPROVED_COUNT,
    SUM(CASE WHEN DECISION = 'DECLINED' THEN 1 ELSE 0 END) AS DECLINED_COUNT,
    SUM(CASE WHEN DECISION = 'REFERRED' THEN 1 ELSE 0 END) AS REFERRED_COUNT,
    ROUND(AVG(CASE WHEN DECISION = 'APPROVED' THEN 1.0 ELSE 0.0 END) * 100, 2) AS APPROVAL_RATE,
    AVG(DECISION_RISK_SCORE) AS AVG_RISK_SCORE,
    AVG(COVERAGE_AMOUNT) AS AVG_COVERAGE_AMOUNT,
    SUM(CASE WHEN DECISION = 'APPROVED' THEN PREMIUM_AMOUNT ELSE 0 END) AS TOTAL_PREMIUM_WRITTEN,
    SUM(CASE WHEN DECISION = 'APPROVED' THEN COVERAGE_AMOUNT ELSE 0 END) AS TOTAL_COVERAGE_WRITTEN,
    AVG(PREMIUM_ADJUSTMENT_PCT) AS AVG_PREMIUM_ADJUSTMENT,
    ROUND(AVG(CASE WHEN REVIEW_FLAG = 1 THEN 1.0 ELSE 0.0 END) * 100, 2) AS REVIEW_FLAG_RATE,
    SUM(CASE WHEN RISK_CATEGORY IN ('HIGH', 'VERY_HIGH') THEN 1 ELSE 0 END) AS HIGH_RISK_DECISIONS
FROM INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE
WHERE UNDERWRITER_NAME IS NOT NULL
GROUP BY UNDERWRITER_NAME, UW_SPECIALIZATION, UW_EXPERIENCE"""
    },
    {
        "name": "V_ADJUSTER_PERFORMANCE",
        "location": "TARGET4",
        "description": "Performance metrics per claims adjuster.",
        "sql": """CREATE OR REPLACE VIEW INSURANCE_ANALYTICS.DASHBOARDS.V_ADJUSTER_PERFORMANCE AS
SELECT
    ADJUSTER_NAME,
    ADJUSTER_REGION,
    COUNT(*) AS TOTAL_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'OPEN' THEN 1 ELSE 0 END) AS OPEN_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'SETTLED' THEN 1 ELSE 0 END) AS SETTLED_CLAIMS,
    SUM(CASE WHEN CLAIM_STATUS = 'DENIED' THEN 1 ELSE 0 END) AS DENIED_CLAIMS,
    ROUND(AVG(CASE WHEN CLAIM_STATUS IN ('SETTLED', 'CLOSED') THEN 1.0 ELSE 0.0 END) * 100, 2) AS SETTLEMENT_RATE,
    SUM(ESTIMATED_AMOUNT) AS TOTAL_ESTIMATED,
    SUM(APPROVED_AMOUNT) AS TOTAL_APPROVED,
    SUM(TOTAL_PAID) AS TOTAL_PAID,
    AVG(ESTIMATED_AMOUNT) AS AVG_CLAIM_AMOUNT,
    AVG(DAYS_TO_REPORT) AS AVG_DAYS_TO_REPORT,
    SUM(CASE WHEN FRAUD_FLAG = 1 THEN 1 ELSE 0 END) AS FRAUD_FLAGS,
    SUM(CASE WHEN PRIORITY IN ('HIGH', 'URGENT') THEN 1 ELSE 0 END) AS HIGH_PRIORITY_CLAIMS,
    ROUND(AVG(PAYOUT_PCT), 2) AS AVG_PAYOUT_PCT
FROM INSURANCE_CURATED.CLAIMS.V_CLAIMS_DETAIL
WHERE ADJUSTER_NAME IS NOT NULL
GROUP BY ADJUSTER_NAME, ADJUSTER_REGION"""
    },
]


def create_node(view_def):
    name = view_def["name"]
    print(f"  Creating {name}...", end=" ", flush=True)

    resp = requests.post(
        f"{API}/nodes",
        headers=HEADERS,
        json={"nodeType": "View", "predecessorNodeIDs": []},
    )
    if resp.status_code not in (200, 201):
        print(f"FAILED (create): {resp.status_code} {resp.text[:200]}")
        return None
    node = resp.json()
    node_id = node["id"]

    source_mapping = node.get("metadata", {}).get("sourceMapping", [])
    if source_mapping:
        source_mapping[0]["name"] = name
    else:
        source_mapping = [{
            "aliases": {},
            "customSQL": {"customSQL": ""},
            "dependencies": [],
            "join": {"joinCondition": ""},
            "name": name,
            "noLinkRefs": [],
        }]

    put_body = {
        "id": node_id,
        "database": "",
        "schema": "",
        "name": name,
        "description": view_def["description"],
        "nodeType": "View",
        "locationName": view_def["location"],
        "config": {
            "selectDistinct": False,
            "insertStrategy": "UNION",
            "override": True,
            "overrideCreateSQL": view_def["sql"],
        },
        "isMultisource": False,
        "materializationType": "view",
        "overrideSQL": True,
        "metadata": {
            "columns": [],
            "sourceMapping": source_mapping,
            "cteString": "",
            "appliedNodeTests": [],
            "enabledColumnTestIDs": [],
        },
    }

    resp2 = requests.put(
        f"{API}/nodes/{node_id}",
        headers=HEADERS,
        json=put_body,
    )
    if resp2.status_code not in (200, 204):
        print(f"FAILED (configure): {resp2.status_code} {resp2.text[:300]}")
        return None

    if resp2.status_code == 200 and resp2.text:
        result = resp2.json()
        print(f"OK -> {result.get('database','?')}.{result.get('schema','?')}.{name}")
    else:
        get_resp = requests.get(f"{API}/nodes/{node_id}", headers=HEADERS)
        if get_resp.status_code == 200:
            result = get_resp.json()
            print(f"OK -> {result.get('database','?')}.{result.get('schema','?')}.{name}")
        else:
            print(f"OK (configured, but can't verify location)")
    return node_id


def main():
    print(f"Coalesce Workspace: {WORKSPACE_ID}")
    print(f"Environment: {ENVIRONMENT_ID}")
    print(f"Base URL: {BASE_URL}")
    print()

    existing = requests.get(f"{API}/nodes", headers=HEADERS).json()
    if existing.get("total", 0) > 0:
        print(f"WARNING: Workspace already has {existing['total']} nodes.")
        if "--force" not in sys.argv:
            print("Use --force to proceed anyway, or clean up first.")
            return

    print(f"Creating {len(VIEWS)} View nodes...\n")
    results = {}
    for v in VIEWS:
        node_id = create_node(v)
        results[v["name"]] = node_id
        time.sleep(0.3)

    print(f"\n{'='*60}")
    success = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    print(f"Created: {success}/{len(VIEWS)}  |  Failed: {failed}")

    if failed:
        print("\nFailed nodes:")
        for name, nid in results.items():
            if not nid:
                print(f"  - {name}")

    results_file = Path(__file__).parent / "node_ids.json"
    results_file.write_text(json.dumps(results, indent=2))
    print(f"\nNode IDs saved to {results_file}")


if __name__ == "__main__":
    main()
