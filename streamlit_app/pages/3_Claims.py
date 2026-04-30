import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("⚖️ Claims Analytics")

@st.cache_data(ttl=600)
def load_kpi_claims():
    return session.sql("""
        SELECT MONTH, CLAIM_TYPE, PRODUCT_TYPE, TOTAL_CLAIMS,
               TOTAL_ESTIMATED, TOTAL_APPROVED, TOTAL_PAID,
               AVG_DAYS_TO_REPORT, OPEN_CLAIMS, UNDER_REVIEW_CLAIMS,
               APPROVED_CLAIMS, SETTLED_CLAIMS, DENIED_CLAIMS, CLOSED_CLAIMS,
               FRAUD_FLAGS, HIGH_PRIORITY_CLAIMS, AVG_PAYOUT_PCT
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_KPI_CLAIMS
        ORDER BY MONTH
    """).to_pandas()

@st.cache_data(ttl=600)
def load_fraud_summary():
    return session.sql("""
        SELECT MONTH, PRODUCT_TYPE, CLAIM_TYPE, FRAUD_CLAIM_COUNT,
               FRAUD_ESTIMATED_AMOUNT, FRAUD_APPROVED_AMOUNT,
               AVG_FRAUD_CLAIM_AMOUNT, FRAUD_OPEN_COUNT, FRAUD_DENIED_COUNT
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_FRAUD_SUMMARY
        ORDER BY MONTH
    """).to_pandas()

@st.cache_data(ttl=600)
def load_adjuster_performance():
    return session.sql("""
        SELECT ADJUSTER_NAME, ADJUSTER_REGION, TOTAL_CLAIMS, OPEN_CLAIMS,
               SETTLED_CLAIMS, DENIED_CLAIMS, SETTLEMENT_RATE,
               ROUND(TOTAL_APPROVED, 0) AS TOTAL_APPROVED,
               ROUND(TOTAL_PAID, 0) AS TOTAL_PAID,
               ROUND(AVG_CLAIM_AMOUNT, 0) AS AVG_CLAIM_AMOUNT,
               ROUND(AVG_DAYS_TO_REPORT, 1) AS AVG_DAYS_TO_REPORT,
               FRAUD_FLAGS, HIGH_PRIORITY_CLAIMS
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_ADJUSTER_PERFORMANCE
        ORDER BY TOTAL_CLAIMS DESC
    """).to_pandas()

claims_df = load_kpi_claims()
claims_df["MONTH"] = pd.to_datetime(claims_df["MONTH"])

with st.sidebar:
    claim_types = sorted(claims_df["CLAIM_TYPE"].unique())
    sel_types = st.multiselect("Claim Type", claim_types, default=claim_types, key="cl_types")
    products = sorted(claims_df["PRODUCT_TYPE"].unique())
    sel_products = st.multiselect("Product Type", products, default=products, key="cl_products")

filtered = claims_df[
    claims_df["CLAIM_TYPE"].isin(sel_types) &
    claims_df["PRODUCT_TYPE"].isin(sel_products)
]

totals = filtered.agg({
    "TOTAL_CLAIMS": "sum", "TOTAL_APPROVED": "sum", "TOTAL_PAID": "sum",
    "OPEN_CLAIMS": "sum", "UNDER_REVIEW_CLAIMS": "sum", "SETTLED_CLAIMS": "sum",
    "DENIED_CLAIMS": "sum", "CLOSED_CLAIMS": "sum", "FRAUD_FLAGS": "sum",
    "HIGH_PRIORITY_CLAIMS": "sum", "APPROVED_CLAIMS": "sum"
})
avg_claim = totals["TOTAL_APPROVED"] / totals["TOTAL_CLAIMS"] if totals["TOTAL_CLAIMS"] > 0 else 0
fraud_rate = totals["FRAUD_FLAGS"] / totals["TOTAL_CLAIMS"] * 100 if totals["TOTAL_CLAIMS"] > 0 else 0
settlement_rate = totals["SETTLED_CLAIMS"] / totals["TOTAL_CLAIMS"] * 100 if totals["TOTAL_CLAIMS"] > 0 else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Claims", f"{int(totals['TOTAL_CLAIMS']):,}")
c2.metric("Avg Claim", f"${avg_claim:,.0f}")
c3.metric("Fraud Rate", f"{fraud_rate:.1f}%")
c4.metric("Settlement Rate", f"{settlement_rate:.1f}%")
c5.metric("High Priority", f"{int(totals['HIGH_PRIORITY_CLAIMS']):,}")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Claims by Status")
    status_data = pd.DataFrame({
        "Status": ["Open", "Under Review", "Approved", "Settled", "Denied", "Closed"],
        "Count": [int(totals["OPEN_CLAIMS"]), int(totals["UNDER_REVIEW_CLAIMS"]),
                  int(totals["APPROVED_CLAIMS"]), int(totals["SETTLED_CLAIMS"]),
                  int(totals["DENIED_CLAIMS"]), int(totals["CLOSED_CLAIMS"])]
    })
    bar = alt.Chart(status_data).mark_bar().encode(
        x=alt.X("Status:N", sort=["Open", "Under Review", "Approved", "Settled", "Denied", "Closed"]),
        y=alt.Y("Count:Q"),
        color=alt.Color("Status:N", scale=alt.Scale(
            domain=["Open", "Under Review", "Approved", "Settled", "Denied", "Closed"],
            range=["#FFD700", "#29B5E8", "#21C354", "#0068C9", "#FF4B4B", "#83C9FF"]
        )),
        tooltip=["Status:N", alt.Tooltip("Count:Q", format=",")]
    ).properties(height=300)
    st.altair_chart(bar, use_container_width=True)

with col2:
    st.subheader("Claims Volume by Type & Product")
    type_prod = filtered.groupby(["CLAIM_TYPE", "PRODUCT_TYPE"]).agg(
        claims=("TOTAL_CLAIMS", "sum")
    ).reset_index()
    grouped = alt.Chart(type_prod).mark_bar().encode(
        x=alt.X("CLAIM_TYPE:N", title="Claim Type"),
        y=alt.Y("claims:Q", title="Claims"),
        color="PRODUCT_TYPE:N",
        tooltip=["CLAIM_TYPE:N", "PRODUCT_TYPE:N", alt.Tooltip("claims:Q", format=",")]
    ).properties(height=300)
    st.altair_chart(grouped, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Monthly Claims Trend")
    monthly = filtered.groupby("MONTH").agg(claims=("TOTAL_CLAIMS", "sum")).reset_index()
    line = alt.Chart(monthly).mark_area(opacity=0.5, color="#29B5E8").encode(
        x=alt.X("MONTH:T", title="Month"),
        y=alt.Y("claims:Q", title="Claims"),
        tooltip=["MONTH:T", alt.Tooltip("claims:Q", format=",")]
    ).properties(height=300)
    st.altair_chart(line, use_container_width=True)

with col4:
    st.subheader("Fraud by Product & Claim Type")
    fraud_df = load_fraud_summary()
    fraud_agg = fraud_df.groupby(["PRODUCT_TYPE", "CLAIM_TYPE"]).agg(
        fraud_count=("FRAUD_CLAIM_COUNT", "sum")
    ).reset_index()
    heatmap = alt.Chart(fraud_agg).mark_rect().encode(
        x=alt.X("CLAIM_TYPE:N", title="Claim Type"),
        y=alt.Y("PRODUCT_TYPE:N", title="Product"),
        color=alt.Color("fraud_count:Q", title="Fraud Claims", scale=alt.Scale(scheme="reds")),
        tooltip=["PRODUCT_TYPE:N", "CLAIM_TYPE:N", alt.Tooltip("fraud_count:Q", format=",")]
    ).properties(height=300)
    st.altair_chart(heatmap, use_container_width=True)

st.subheader("Adjuster Performance")
adj_df = load_adjuster_performance()
display_df = adj_df.rename(columns={
    "ADJUSTER_NAME": "Adjuster", "ADJUSTER_REGION": "Region",
    "TOTAL_CLAIMS": "Claims", "OPEN_CLAIMS": "Open",
    "SETTLED_CLAIMS": "Settled", "DENIED_CLAIMS": "Denied",
    "SETTLEMENT_RATE": "Settlement %", "TOTAL_APPROVED": "Approved $",
    "TOTAL_PAID": "Paid $", "AVG_CLAIM_AMOUNT": "Avg Claim",
    "AVG_DAYS_TO_REPORT": "Avg Days", "FRAUD_FLAGS": "Fraud",
    "HIGH_PRIORITY_CLAIMS": "High Priority",
})
st.dataframe(display_df, use_container_width=True, height=450)
