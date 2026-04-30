import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("⚠️ Risk Insights")

@st.cache_data(ttl=600)
def load_risk_factors():
    return session.sql("""
        SELECT rf."factor_type" AS FACTOR_TYPE,
               rf."factor_value" AS FACTOR_VALUE,
               p."product_type" AS PRODUCT_TYPE,
               COUNT(*) AS FACTOR_COUNT,
               ROUND(AVG(rf."impact_score"), 2) AS AVG_IMPACT,
               ROUND(MAX(rf."impact_score"), 2) AS MAX_IMPACT
        FROM INSURANCE_RAW."insurance_db"."risk_factors" rf
        JOIN INSURANCE_RAW."insurance_db"."policies" p ON rf."policy_id" = p."policy_id"
        WHERE rf._SNOWFLAKE_DELETED = FALSE AND p._SNOWFLAKE_DELETED = FALSE
        GROUP BY rf."factor_type", rf."factor_value", p."product_type"
        ORDER BY FACTOR_COUNT DESC
    """).to_pandas()

@st.cache_data(ttl=600)
def load_customer_risk():
    return session.sql("""
        SELECT CUSTOMER_ID, CUSTOMER_NAME, CREDIT_SCORE, TOTAL_POLICIES,
               TOTAL_PREMIUM, TOTAL_CLAIMS, TOTAL_APPROVED, LOSS_RATIO,
               AVG_RISK_SCORE, OCCUPATION, ANNUAL_INCOME, STATE,
               CUSTOMER_AGE
        FROM INSURANCE_CURATED.COMMON.V_CUSTOMER_360
        WHERE TOTAL_POLICIES > 0
    """).to_pandas()

@st.cache_data(ttl=600)
def load_high_risk_policies():
    return session.sql("""
        SELECT POLICY_ID, POLICY_NUMBER, PRODUCT_TYPE, CUSTOMER_NAME,
               CUSTOMER_STATE, COVERAGE_AMOUNT, PREMIUM_AMOUNT,
               POLICY_RISK_SCORE, RISK_CATEGORY, DECISION,
               CREDIT_SCORE, ANNUAL_INCOME
        FROM INSURANCE_CURATED.UNDERWRITING.V_UNDERWRITING_PIPELINE
        WHERE POLICY_RISK_SCORE > 80
        ORDER BY POLICY_RISK_SCORE DESC
        LIMIT 100
    """).to_pandas()

risk_df = load_risk_factors()
cust_df = load_customer_risk()

with st.sidebar:
    products = sorted(risk_df["PRODUCT_TYPE"].unique())
    sel_product = st.selectbox("Product Focus", ["All"] + products, key="risk_product")

if sel_product != "All":
    risk_filtered = risk_df[risk_df["PRODUCT_TYPE"] == sel_product]
else:
    risk_filtered = risk_df

high_risk_count = len(cust_df[cust_df["AVG_RISK_SCORE"].notna() & (cust_df["AVG_RISK_SCORE"] > 70)])
avg_credit = cust_df["CREDIT_SCORE"].mean()
with_claims = len(cust_df[cust_df["TOTAL_CLAIMS"] > 0])
claim_rate = with_claims / len(cust_df) * 100 if len(cust_df) > 0 else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Customers with Policies", f"{len(cust_df):,}")
c2.metric("Avg Credit Score", f"{avg_credit:.0f}")
c3.metric("Claim Frequency", f"{claim_rate:.1f}%")
c4.metric("High Risk (>70)", f"{high_risk_count:,}")

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Risk Factor Heatmap")
    factor_heat = risk_filtered.groupby(["PRODUCT_TYPE", "FACTOR_TYPE"]).agg(
        count=("FACTOR_COUNT", "sum"),
        avg_impact=("AVG_IMPACT", "mean")
    ).reset_index()

    heatmap = alt.Chart(factor_heat).mark_rect().encode(
        x=alt.X("FACTOR_TYPE:N", title="Factor Type"),
        y=alt.Y("PRODUCT_TYPE:N", title="Product"),
        color=alt.Color("avg_impact:Q", title="Avg Impact", scale=alt.Scale(scheme="redyellowgreen", reverse=True)),
        tooltip=["PRODUCT_TYPE:N", "FACTOR_TYPE:N",
                  alt.Tooltip("count:Q", format=","),
                  alt.Tooltip("avg_impact:Q", format=".2f")]
    ).properties(height=350)
    st.altair_chart(heatmap, use_container_width=True)

with col2:
    st.subheader("Credit Score vs Loss Ratio")
    scatter_df = cust_df[cust_df["TOTAL_CLAIMS"] > 0].copy()
    scatter_df["CREDIT_BAND"] = pd.cut(
        scatter_df["CREDIT_SCORE"],
        bins=[299, 500, 600, 700, 800, 851],
        labels=["300-500", "501-600", "601-700", "701-800", "801-850"]
    )
    band_agg = scatter_df.groupby("CREDIT_BAND", observed=True).agg(
        avg_loss_ratio=("LOSS_RATIO", "mean"),
        customer_count=("CUSTOMER_ID", "count"),
        avg_premium=("TOTAL_PREMIUM", "mean")
    ).reset_index()

    bars = alt.Chart(band_agg).mark_bar().encode(
        x=alt.X("CREDIT_BAND:N", title="Credit Score Band"),
        y=alt.Y("avg_loss_ratio:Q", title="Avg Loss Ratio (%)"),
        color=alt.condition(
            alt.datum.avg_loss_ratio > 50,
            alt.value("#FF4B4B"),
            alt.value("#29B5E8")
        ),
        tooltip=["CREDIT_BAND:N",
                  alt.Tooltip("avg_loss_ratio:Q", format=".1f"),
                  alt.Tooltip("customer_count:Q", format=",")]
    ).properties(height=350)
    st.altair_chart(bars, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Age Band Risk Analysis")
    age_df = cust_df.copy()
    age_df["AGE_BAND"] = pd.cut(
        age_df["CUSTOMER_AGE"],
        bins=[17, 25, 35, 45, 55, 65, 100],
        labels=["18-25", "26-35", "36-45", "46-55", "56-65", "65+"]
    )
    age_agg = age_df.groupby("AGE_BAND", observed=True).agg(
        customers=("CUSTOMER_ID", "count"),
        avg_risk=("AVG_RISK_SCORE", "mean"),
        claim_count=("TOTAL_CLAIMS", "sum"),
        avg_premium=("TOTAL_PREMIUM", "mean")
    ).reset_index()
    age_agg["CLAIM_RATE"] = (age_agg["claim_count"] / age_agg["customers"]).round(2)

    base = alt.Chart(age_agg).encode(x=alt.X("AGE_BAND:N", title="Age Band"))
    bar = base.mark_bar(color="#29B5E8", opacity=0.7).encode(
        y=alt.Y("customers:Q", title="Customers"),
        tooltip=["AGE_BAND:N", alt.Tooltip("customers:Q", format=","),
                  alt.Tooltip("CLAIM_RATE:Q", format=".2f")]
    )
    line = base.mark_line(color="#FF4B4B", strokeWidth=3).encode(
        y=alt.Y("avg_risk:Q", title="Avg Risk Score", scale=alt.Scale(domain=[30, 60])),
        tooltip=["AGE_BAND:N", alt.Tooltip("avg_risk:Q", format=".1f")]
    )
    combo = alt.layer(bar, line).resolve_scale(y="independent").properties(height=300)
    st.altair_chart(combo, use_container_width=True)

with col4:
    st.subheader("Top Risk Factors by Volume")
    top_factors = risk_filtered.groupby("FACTOR_TYPE").agg(
        total=("FACTOR_COUNT", "sum"),
        avg_impact=("AVG_IMPACT", "mean")
    ).reset_index().sort_values("total", ascending=False).head(10)

    bar = alt.Chart(top_factors).mark_bar().encode(
        x=alt.X("total:Q", title="Count"),
        y=alt.Y("FACTOR_TYPE:N", title="Factor", sort="-x"),
        color=alt.Color("avg_impact:Q", scale=alt.Scale(scheme="redyellowgreen", reverse=True), title="Avg Impact"),
        tooltip=["FACTOR_TYPE:N", alt.Tooltip("total:Q", format=","),
                  alt.Tooltip("avg_impact:Q", format=".2f")]
    ).properties(height=300)
    st.altair_chart(bar, use_container_width=True)

st.subheader("High-Risk Policies (Risk Score > 80)")
hr_df = load_high_risk_policies()
display_df = hr_df.drop(columns=["POLICY_ID"], errors="ignore").rename(columns={
    "POLICY_NUMBER": "Policy #", "PRODUCT_TYPE": "Product",
    "CUSTOMER_NAME": "Customer", "CUSTOMER_STATE": "State",
    "COVERAGE_AMOUNT": "Coverage", "PREMIUM_AMOUNT": "Premium",
    "POLICY_RISK_SCORE": "Risk Score", "RISK_CATEGORY": "Risk Cat",
    "DECISION": "Decision", "CREDIT_SCORE": "Credit",
    "ANNUAL_INCOME": "Income",
})
st.dataframe(display_df, use_container_width=True, height=400)
