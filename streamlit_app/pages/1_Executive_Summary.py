import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("📊 Executive Summary")

@st.cache_data(ttl=600)
def load_portfolio_kpis():
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
             WHERE _SNOWFLAKE_DELETED = FALSE) AS total_customers,
            (SELECT SUM("approved_amount") FROM INSURANCE_RAW."insurance_db"."claims"
             WHERE _SNOWFLAKE_DELETED = FALSE) AS total_incurred
    """).to_pandas()

@st.cache_data(ttl=600)
def load_loss_ratio_trend():
    return session.sql("""
        SELECT MONTH, PRODUCT_TYPE, TOTAL_CLAIMS, INCURRED_LOSSES, EARNED_PREMIUM, LOSS_RATIO_PCT
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_LOSS_RATIO
        ORDER BY MONTH
    """).to_pandas()

@st.cache_data(ttl=600)
def load_product_mix():
    return session.sql("""
        SELECT "product_type" AS PRODUCT_TYPE,
               COUNT(*) AS POLICY_COUNT,
               ROUND(SUM("premium_amount"), 0) AS TOTAL_PREMIUM,
               ROUND(SUM("coverage_amount"), 0) AS TOTAL_COVERAGE
        FROM INSURANCE_RAW."insurance_db"."policies"
        WHERE _SNOWFLAKE_DELETED = FALSE
        GROUP BY "product_type"
        ORDER BY TOTAL_PREMIUM DESC
    """).to_pandas()

@st.cache_data(ttl=600)
def load_geographic():
    return session.sql("""
        SELECT STATE, TOTAL_POLICIES, TOTAL_PREMIUM, TOTAL_CLAIMS, TOTAL_APPROVED, LOSS_RATIO_PCT
        FROM INSURANCE_ANALYTICS.DASHBOARDS.V_GEOGRAPHIC_SUMMARY
        ORDER BY LOSS_RATIO_PCT DESC
    """).to_pandas()

@st.cache_data(ttl=600)
def load_monthly_premium_vs_losses():
    return session.sql("""
        WITH monthly_premium AS (
            SELECT DATE_TRUNC('month', "effective_date") AS MONTH,
                   SUM("premium_amount") AS PREMIUM
            FROM INSURANCE_RAW."insurance_db"."policies"
            WHERE _SNOWFLAKE_DELETED = FALSE
            GROUP BY 1
        ),
        monthly_losses AS (
            SELECT DATE_TRUNC('month', "reported_date") AS MONTH,
                   SUM("approved_amount") AS LOSSES
            FROM INSURANCE_RAW."insurance_db"."claims"
            WHERE _SNOWFLAKE_DELETED = FALSE
            GROUP BY 1
        )
        SELECT COALESCE(p.MONTH, l.MONTH) AS MONTH,
               COALESCE(p.PREMIUM, 0) AS WRITTEN_PREMIUM,
               COALESCE(l.LOSSES, 0) AS INCURRED_LOSSES
        FROM monthly_premium p
        FULL OUTER JOIN monthly_losses l ON p.MONTH = l.MONTH
        ORDER BY MONTH
    """).to_pandas()

kpis = load_portfolio_kpis()
row = kpis.iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Active Policies", f"{int(row['ACTIVE_POLICIES']):,}")
c2.metric("Total Premium", f"${int(row['TOTAL_PREMIUM']):,.0f}")
c3.metric("Open Claims", f"{int(row['OPEN_CLAIMS']):,}")
c4.metric("Overall Loss Ratio", f"{row['LOSS_RATIO']:.1f}%")
c5.metric("Customers", f"{int(row['TOTAL_CUSTOMERS']):,}")

st.divider()

trend_df = load_monthly_premium_vs_losses()
trend_df["MONTH"] = pd.to_datetime(trend_df["MONTH"])

col1, col2 = st.columns(2)

with col1:
    st.subheader("Monthly Premium vs Incurred Losses")
    melted = trend_df.melt(id_vars=["MONTH"], value_vars=["WRITTEN_PREMIUM", "INCURRED_LOSSES"],
                           var_name="Metric", value_name="Amount")
    chart = alt.Chart(melted).mark_line(strokeWidth=2).encode(
        x=alt.X("MONTH:T", title="Month"),
        y=alt.Y("Amount:Q", title="Amount ($)"),
        color=alt.Color("Metric:N", scale=alt.Scale(
            domain=["WRITTEN_PREMIUM", "INCURRED_LOSSES"],
            range=["#29B5E8", "#FF4B4B"]
        )),
        tooltip=["MONTH:T", "Metric:N", alt.Tooltip("Amount:Q", format="$,.0f")]
    ).properties(height=350)
    st.altair_chart(chart, use_container_width=True)

with col2:
    st.subheader("Loss Ratio by Product")
    lr_df = load_loss_ratio_trend()
    product_lr = lr_df.groupby("PRODUCT_TYPE").agg(
        INCURRED=("INCURRED_LOSSES", "sum"),
        PREMIUM=("EARNED_PREMIUM", "sum")
    ).reset_index()
    product_lr["LOSS_RATIO"] = (product_lr["INCURRED"] / product_lr["PREMIUM"] * 100).round(1)
    product_lr = product_lr.sort_values("LOSS_RATIO", ascending=False)

    bar = alt.Chart(product_lr).mark_bar().encode(
        x=alt.X("PRODUCT_TYPE:N", title="Product", sort="-y"),
        y=alt.Y("LOSS_RATIO:Q", title="Loss Ratio (%)"),
        color=alt.condition(
            alt.datum.LOSS_RATIO > 50,
            alt.value("#FF4B4B"),
            alt.value("#29B5E8")
        ),
        tooltip=["PRODUCT_TYPE:N", alt.Tooltip("LOSS_RATIO:Q", format=".1f")]
    ).properties(height=350)

    rule = alt.Chart(pd.DataFrame({"y": [40]})).mark_rule(color="#FFD700", strokeDash=[5, 5]).encode(
        y="y:Q"
    )
    st.altair_chart(bar + rule, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("Product Mix by Premium")
    mix_df = load_product_mix()
    donut = alt.Chart(mix_df).mark_arc(innerRadius=60).encode(
        theta=alt.Theta("TOTAL_PREMIUM:Q"),
        color=alt.Color("PRODUCT_TYPE:N", title="Product"),
        tooltip=["PRODUCT_TYPE:N",
                  alt.Tooltip("TOTAL_PREMIUM:Q", format="$,.0f"),
                  alt.Tooltip("POLICY_COUNT:Q", format=",")]
    ).properties(height=350)
    st.altair_chart(donut, use_container_width=True)

with col4:
    st.subheader("Geographic Loss Ratio (Top 15 States)")
    geo_df = load_geographic().head(15)
    geo_bar = alt.Chart(geo_df).mark_bar().encode(
        x=alt.X("STATE:N", title="State", sort="-y"),
        y=alt.Y("LOSS_RATIO_PCT:Q", title="Loss Ratio (%)"),
        color=alt.condition(
            alt.datum.LOSS_RATIO_PCT > 40,
            alt.value("#FF4B4B"),
            alt.value("#29B5E8")
        ),
        tooltip=["STATE:N",
                  alt.Tooltip("LOSS_RATIO_PCT:Q", format=".1f"),
                  alt.Tooltip("TOTAL_POLICIES:Q", format=","),
                  alt.Tooltip("TOTAL_CLAIMS:Q", format=",")]
    ).properties(height=350)
    st.altair_chart(geo_bar, use_container_width=True)
