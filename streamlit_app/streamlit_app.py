import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Insurance Analytics",
    page_icon="🛡️",
    layout="wide",
)

session = get_active_session()

st.title("🛡️ Insurance Analytics Dashboard")
st.caption("Coalesce + Snowflake Pipeline Demo | Underwriting & Claims Transformation")

@st.cache_data(ttl=600)
def load_top_kpis():
    return session.sql("""
        SELECT
            (SELECT COUNT(*) FROM INSURANCE_RAW."insurance_db"."policies"
             WHERE _SNOWFLAKE_DELETED = FALSE AND "status" = 'ACTIVE') AS active_policies,
            (SELECT ROUND(SUM("premium_amount"), 0) FROM INSURANCE_RAW."insurance_db"."policies"
             WHERE _SNOWFLAKE_DELETED = FALSE) AS total_premium,
            (SELECT COUNT(*) FROM INSURANCE_RAW."insurance_db"."claims"
             WHERE _SNOWFLAKE_DELETED = FALSE AND "status" IN ('OPEN', 'UNDER_REVIEW')) AS open_claims,
            (SELECT ROUND(
                (SELECT SUM("approved_amount") FROM INSURANCE_RAW."insurance_db"."claims" WHERE _SNOWFLAKE_DELETED = FALSE)
                / NULLIF((SELECT SUM("premium_amount") FROM INSURANCE_RAW."insurance_db"."policies" WHERE _SNOWFLAKE_DELETED = FALSE), 0)
                * 100, 1)) AS loss_ratio,
            (SELECT COUNT(DISTINCT "customer_id") FROM INSURANCE_RAW."insurance_db"."policies"
             WHERE _SNOWFLAKE_DELETED = FALSE) AS total_customers
    """).to_pandas()

row = load_top_kpis().iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Active Policies", f"{int(row['ACTIVE_POLICIES']):,}")
c2.metric("Total Premium", f"${int(row['TOTAL_PREMIUM']):,.0f}")
c3.metric("Open Claims", f"{int(row['OPEN_CLAIMS']):,}")
c4.metric("Overall Loss Ratio", f"{row['LOSS_RATIO']:.1f}%")
c5.metric("Customers", f"{int(row['TOTAL_CUSTOMERS']):,}")

st.divider()
st.info("👈 Use the sidebar to navigate between pages: Executive Summary, Underwriting, Claims, and Risk Insights.")
