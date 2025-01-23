import os
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from google.cloud import bigquery

st.title("Revenue")

# 1. Retrieve GCP credentials from Streamlit secrets
credentials_dict = {
    "type": os.environ["type"],
    "project_id": os.environ["project_id"],
    "private_key_id": os.environ["private_key_id"],
    "private_key": os.environ["private_key"],
    "client_email": os.environ["client_email"],
    "client_id": os.environ["client_id"],
    "auth_uri": os.environ["auth_uri"],
    "token_uri": os.environ["token_uri"],
    "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
    "client_x509_cert_url": os.environ["client_x509_cert_url"]
}

# 2. Create a BigQuery client from service account info
client = bigquery.Client.from_service_account_info(credentials_dict)

# 3. Calculate financial year range: 1st October to 'today'
today = datetime.today()
if today.month >= 10:
    start_of_financial_year = datetime(today.year, 10, 1)
else:
    start_of_financial_year = datetime(today.year - 1, 10, 1)
end_of_financial_year = today

# Previous year's range
start_of_previous_financial_year = pd.Timestamp(start_of_financial_year) - pd.DateOffset(years=1)
end_of_previous_financial_year = pd.Timestamp(end_of_financial_year) - pd.DateOffset(years=1)

# Convert date ranges to strings
start_date_str = start_of_financial_year.strftime('%Y-%m-%d')
end_date_str = end_of_financial_year.strftime('%Y-%m-%d')
prev_start_date_str = start_of_previous_financial_year.strftime('%Y-%m-%d')
prev_end_date_str = end_of_previous_financial_year.strftime('%Y-%m-%d')

# --- A) Queries for monthly totals ---
current_query = f"""
SELECT 
    FORMAT_TIMESTAMP('%Y-%m', issue_date) AS month,
    SUM(line_item.amount) AS total_amount
FROM 
    `marketing-434610.harvest.Invoices`,
    UNNEST(line_items) AS line_item
WHERE 
    issue_date BETWEEN '{start_date_str}' AND '{end_date_str}'
    AND state IN ('open', 'paid', 'draft')
GROUP BY 
    month
ORDER BY 
    month ASC;
"""

previous_query = f"""
SELECT 
    FORMAT_TIMESTAMP('%Y-%m', issue_date) AS month,
    SUM(line_item.amount) AS total_amount
FROM 
    `marketing-434610.harvest.Invoices`,
    UNNEST(line_items) AS line_item
WHERE 
    issue_date BETWEEN '{prev_start_date_str}' AND '{prev_end_date_str}'
    AND state IN ('open', 'paid', 'draft')
GROUP BY 
    month
ORDER BY 
    month ASC;
"""

# --- B) Queries for client-level data ---
current_clients_query = f"""
SELECT
  client.name AS client_name,
  SUM(line_item.amount) AS revenue_current
FROM
  `marketing-434610.harvest.Invoices`,
  UNNEST(line_items) AS line_item
WHERE
  issue_date BETWEEN '{start_date_str}' AND '{end_date_str}'
  AND state IN ('open', 'paid', 'draft')
GROUP BY
  client_name
ORDER BY
  client_name;
"""

previous_clients_query = f"""
SELECT
  client.name AS client_name,
  SUM(line_item.amount) AS revenue_previous
FROM
  `marketing-434610.harvest.Invoices`,
  UNNEST(line_items) AS line_item
WHERE
  issue_date BETWEEN '{prev_start_date_str}' AND '{prev_end_date_str}'
  AND state IN ('open', 'paid', 'draft')
GROUP BY
  client_name
ORDER BY
  client_name;
"""

try:
    # 1) Monthly Totals for Chart & Metrics
    current_data = pd.DataFrame([dict(row) for row in client.query(current_query)])
    previous_data = pd.DataFrame([dict(row) for row in client.query(previous_query)])

    current_data['month'] = pd.to_datetime(current_data['month'], format='%Y-%m')
    months_in_fy = pd.date_range(start=start_of_financial_year, end=end_of_financial_year, freq="MS")
    months_df = pd.DataFrame({
        "month": months_in_fy,
        "month_label": [m.strftime("%b-%Y") for m in months_in_fy]
    })

    # Merge & fill missing months
    current_data = months_df.merge(current_data, on='month', how='left')
    current_data['total_amount'] = current_data['total_amount'].fillna(0)
    current_data = current_data.sort_values(by='month')

    total_invoiced_current = current_data['total_amount'].sum()
    total_invoiced_previous = previous_data['total_amount'].sum() if not previous_data.empty else 0

    # % difference for dashboard metric
    if total_invoiced_previous == 0:
        percent_diff = 0.0
    else:
        diff = total_invoiced_current - total_invoiced_previous
        percent_diff = (diff / total_invoiced_previous) * 100

    # 2) Client-Level Data
    current_clients_df = pd.DataFrame([dict(row) for row in client.query(current_clients_query)])
    prev_clients_df = pd.DataFrame([dict(row) for row in client.query(previous_clients_query)])
    clients_merged = pd.merge(current_clients_df, prev_clients_df, on='client_name', how='outer').fillna(0)

    # a) Calculate £ difference
    clients_merged["Difference"] = clients_merged["revenue_current"] - clients_merged["revenue_previous"]

    # b) Calculate % difference
    def calc_percentage_diff(row):
        if row["revenue_previous"] == 0:
            return None
        return ((row["revenue_current"] - row["revenue_previous"]) / row["revenue_previous"]) * 100
    clients_merged["% Difference"] = clients_merged.apply(calc_percentage_diff, axis=1)

    # c) Rename columns
    clients_merged.rename(columns={
        'client_name': 'Client name',
        'revenue_current': 'Revenue YTD',
        'revenue_previous': 'Revenue previous YTD'
    }, inplace=True)

    # d) Sort by largest YTD revenue
    clients_merged.sort_values(by='Revenue YTD', ascending=False, inplace=True)

    # e) Insert Totals row
    total_difference = total_invoiced_current - total_invoiced_previous
    totals_row = {
        'Client name': 'Total',
        'Revenue YTD': total_invoiced_current,
        'Revenue previous YTD': total_invoiced_previous,
        'Difference': total_difference,
        '% Difference': percent_diff
    }
    clients_merged = pd.concat([clients_merged, pd.DataFrame([totals_row])], ignore_index=True)

    # 3) Dashboard: Metrics
    col1, col2, col3 = st.columns(3)

    # Current metric
    with col1:
        st.metric(
            label=f"Current ({start_of_financial_year.strftime('%d %b %Y')} - {end_of_financial_year.strftime('%d %b %Y')})",
            value=f"£{total_invoiced_current:,.2f}",
        )

    # Previous metric (FIX: show correct range)
    with col2:
        st.metric(
            label=(
                f"Previous ("
                f"{start_of_previous_financial_year.strftime('%d %b %Y')} - "
                f"{end_of_previous_financial_year.strftime('%d %b %Y')})"
            ),
            value=f"£{total_invoiced_previous:,.2f}",
        )

    # % difference metric
    with col3:
        st.metric("YOY % Change", f"{percent_diff:,.1f}%")

    st.write("")  # spacing

    # 4) Chart
    chart = alt.Chart(current_data).mark_bar().encode(
        x=alt.X('month_label:N', title="Month", sort=list(current_data['month_label'])),
        y=alt.Y('total_amount:Q', title="Total Invoiced (£)"),
        tooltip=[
            alt.Tooltip('month_label:N', title='Month'),
            alt.Tooltip('total_amount:Q', title='Total Invoiced (£)', format=',.2f')
        ]
    ).properties(
        title="Invoiced Amount by Month (From 1st Oct to Today)",
        width="container",
        height=400
    )
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Revenue by Client")

    # 5) Build Table: now has Difference + % Difference columns
    display_columns = ["Client name", "Revenue YTD", "Revenue previous YTD", "Difference", "% Difference"]
    df_for_styling = clients_merged[display_columns].copy()

    # Accountancy-style currency formatting
    def accountancy_format(val):
        """Format negative numbers in parentheses, else normal. Two decimals, currency sign."""
        if not isinstance(val, (int, float)):
            return val
        if val < 0:
            return f"(£{abs(val):,.2f})"
        return f"£{val:,.2f}"

    # Conditional styling for negative/positive
    def highlight_vals(val):
        """Color negative red, positive green, else default."""
        if val is None or not isinstance(val, (float, int)):
            return ""
        if val < 0:
            return "color: red"
        elif val > 0:
            return "color: green"
        return ""

    styled_df = df_for_styling.style \
        .format(
            {
                "Revenue YTD": accountancy_format,
                "Revenue previous YTD": accountancy_format,
                "Difference": accountancy_format,
                "% Difference": lambda x: "-" if pd.isnull(x) else f"{x:,.1f}%"
            }
        ) \
        .applymap(highlight_vals, subset=["Difference"]) \
        .applymap(highlight_vals, subset=["% Difference"])

    # Right-align numeric columns
    numeric_cols = ["Revenue YTD", "Revenue previous YTD", "Difference", "% Difference"]
    styled_df.set_properties(**{"text-align": "right"}, subset=numeric_cols)

    # Thicker top border & bold text for last row (the totals row)
    styled_df.set_table_styles([
        {"selector": "th.row_heading", "props": [("display", "none")]},
        {"selector": "th.blank", "props": [("display", "none")]},
        {
            "selector": "tbody tr:last-child",
            "props": [
                ("font-weight", "bold"),
                ("border-top", "3px solid black")
            ]
        }
    ], overwrite=False)

    st.write(styled_df.to_html(), unsafe_allow_html=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")