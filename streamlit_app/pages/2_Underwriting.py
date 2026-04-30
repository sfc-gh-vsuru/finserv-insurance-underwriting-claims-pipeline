import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("✅ Underwriting Analytics")

@st.cache_data(ttl=600)
def load_kpi_underwriting():
    return session.sql("""
        SELECT MONTH, PRODUCT_TYPE, TOTAL_DECISIONS, APPROVED, DECLINED, REFERRED,
               COUNTER_OFFERS, APPROVAL_RATE, AVG_RISK_SCORE, AVG_PREMIUM,
               TOTAL_PREMIUM, TOTAL_COVERAGE, AVG_CREDIT_SCORE, REVIEW_FLAG_COUNT
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_KPI_UNDERWRITING
        ORDER BY MONTH
    """).to_pandas()

@st.cache_data(ttl=600)
def load_underwriter_performance():
    return session.sql("""
        SELECT UNDERWRITER_NAME, UW_SPECIALIZATION, UW_EXPERIENCE, TOTAL_DECISIONS,
               APPROVED_COUNT, DECLINED_COUNT, REFERRED_COUNT, APPROVAL_RATE,
               ROUND(AVG_RISK_SCORE, 1) AS AVG_RISK_SCORE,
               ROUND(TOTAL_PREMIUM_WRITTEN, 0) AS TOTAL_PREMIUM_WRITTEN,
               ROUND(REVIEW_FLAG_RATE, 1) AS REVIEW_FLAG_RATE,
               HIGH_RISK_DECISIONS
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_UNDERWRITER_PERFORMANCE
        ORDER BY TOTAL_DECISIONS DESC
    """).to_pandas()

@st.cache_data(ttl=600)
def load_risk_distribution():
    return session.sql("""
        SELECT DECISION, RISK_CATEGORY,
               COUNT(*) AS CNT
        FROM INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE
        WHERE DECISION IS NOT NULL
        GROUP BY DECISION, RISK_CATEGORY
        ORDER BY RISK_CATEGORY, DECISION
    """).to_pandas()

uw_df = load_kpi_underwriting()
uw_df["MONTH"] = pd.to_datetime(uw_df["MONTH"])

with st.sidebar:
    products = sorted(uw_df["PRODUCT_TYPE"].unique())
    sel_products = st.multiselect("Product Type", products, default=products, key="uw_products")
    min_date = uw_df["MONTH"].min().date()
    max_date = uw_df["MONTH"].max().date()
    date_range = st.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date, key="uw_dates")

filtered = uw_df[uw_df["PRODUCT_TYPE"].isin(sel_products)]
if len(date_range) == 2:
    filtered = filtered[(filtered["MONTH"].dt.date >= date_range[0]) & (filtered["MONTH"].dt.date <= date_range[1])]

totals = filtered.agg({
    "TOTAL_DECISIONS": "sum", "APPROVED": "sum", "DECLINED": "sum",
    "REFERRED": "sum", "REVIEW_FLAG_COUNT": "sum"
})
overall_approval = (totals["APPROVED"] / totals["TOTAL_DECISIONS"] * 100) if totals["TOTAL_DECISIONS"] > 0 else 0
avg_risk = filtered["AVG_RISK_SCORE"].mean()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Decisions", f"{int(totals['TOTAL_DECISIONS']):,}")
c2.metric("Approval Rate", f"{overall_approval:.1f}%")
c3.metric("Avg Risk Score", f"{avg_risk:.1f}")
c4.metric("Reviews Flagged", f"{int(totals['REVIEW_FLAG_COUNT']):,}")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Decision Breakdown")
    funnel_data = pd.DataFrame({
        "Decision": ["Approved", "Declined", "Referred", "Counter-Offer"],
        "Count": [int(totals["APPROVED"]), int(totals["DECLINED"]),
                  int(totals["REFERRED"]), int(filtered["COUNTER_OFFERS"].sum())]
    })
    bar = alt.Chart(funnel_data).mark_bar().encode(
        x=alt.X("Decision:N", sort=["Approved", "Declined", "Referred", "Counter-Offer"]),
        y=alt.Y("Count:Q"),
        color=alt.Color("Decision:N", scale=alt.Scale(
            domain=["Approved", "Declined", "Referred", "Counter-Offer"],
            range=["#21C354", "#FF4B4B", "#FFD700", "#29B5E8"]
        )),
        tooltip=["Decision:N", alt.Tooltip("Count:Q", format=",")]
    ).properties(height=300)
    st.altair_chart(bar, use_container_width=True)

with col2:
    st.subheader("Monthly Approval Rate by Product")
    monthly = filtered.groupby(["MONTH", "PRODUCT_TYPE"]).agg(
        approved=("APPROVED", "sum"), total=("TOTAL_DECISIONS", "sum")
    ).reset_index()
    monthly["APPROVAL_RATE"] = (monthly["approved"] / monthly["total"] * 100).round(1)

    line = alt.Chart(monthly).mark_line(strokeWidth=2).encode(
        x=alt.X("MONTH:T", title="Month"),
        y=alt.Y("APPROVAL_RATE:Q", title="Approval Rate (%)", scale=alt.Scale(domain=[40, 80])),
        color="PRODUCT_TYPE:N",
        tooltip=["MONTH:T", "PRODUCT_TYPE:N", alt.Tooltip("APPROVAL_RATE:Q", format=".1f")]
    ).properties(height=300)
    st.altair_chart(line, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Risk Score by Decision & Category")
    risk_df = load_risk_distribution()
    heatmap = alt.Chart(risk_df).mark_rect().encode(
        x=alt.X("DECISION:N", title="Decision"),
        y=alt.Y("RISK_CATEGORY:N", title="Risk Category",
                 sort=["LOW", "MEDIUM", "HIGH", "VERY_HIGH"]),
        color=alt.Color("CNT:Q", title="Count", scale=alt.Scale(scheme="blues")),
        tooltip=["DECISION:N", "RISK_CATEGORY:N", alt.Tooltip("CNT:Q", format=",")]
    ).properties(height=300)
    st.altair_chart(heatmap, use_container_width=True)

with col4:
    st.subheader("Underwriter Performance")
    perf_df = load_underwriter_performance()
    display_df = perf_df.drop(columns=["REFERRED_COUNT"], errors="ignore").rename(columns={
        "UNDERWRITER_NAME": "Name", "UW_SPECIALIZATION": "Specialty",
        "UW_EXPERIENCE": "Exp (yrs)", "TOTAL_DECISIONS": "Decisions",
        "APPROVED_COUNT": "Approved", "DECLINED_COUNT": "Declined",
        "APPROVAL_RATE": "Approval %", "AVG_RISK_SCORE": "Avg Risk",
        "TOTAL_PREMIUM_WRITTEN": "Premium Written",
        "REVIEW_FLAG_RATE": "Review %", "HIGH_RISK_DECISIONS": "High Risk",
    })
    st.dataframe(display_df, use_container_width=True, height=400)
