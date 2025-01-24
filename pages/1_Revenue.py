import os
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime

st.title("Revenue")

# 1. Retrieve GCP credentials from Streamlit secrets
credentials_dict = {
    "type": st.secrets["gcp"]["type"],
    "project_id": st.secrets["gcp"]["project_id"],
    "private_key_id": st.secrets["gcp"]["private_key_id"],
    "private_key": st.secrets["gcp"]["private_key"],
    "client_email": st.secrets["gcp"]["client_email"],
    "client_id": st.secrets["gcp"]["client_id"],
    "auth_uri": st.secrets["gcp"]["auth_uri"],
    "token_uri": st.secrets["gcp"]["token_uri"],
    "auth_provider_x509_cert_url": st.secrets["gcp"]["auth_provider_x509_cert_url"],
    "client_x509_cert_url": st.secrets["gcp"]["client_x509_cert_url"]
}

# 2. Create a BigQuery client from service account info
from google.cloud import bigquery
client = bigquery.Client.from_service_account_info(credentials_dict)

# 3. Calculate financial year range: 1st October to 'today'
today = datetime.today()
if today.month >= 10:
    start_of_financial_year = datetime(today.year, 10, 1)
else:
    start_of_financial_year = datetime(today.year - 1, 10, 1)

end_of_financial_year = today  # up to 'today'

start_of_previous_financial_year = pd.Timestamp(start_of_financial_year) - pd.DateOffset(years=1)
end_of_previous_financial_year = pd.Timestamp(end_of_financial_year) - pd.DateOffset(years=1)

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
    # --- 1) Monthly Totals for Chart & Metrics ---
    # Current year
    current_rows = list(client.query(current_query))
    current_data = pd.DataFrame([dict(row) for row in current_rows])
    current_data['month'] = pd.to_datetime(current_data['month'], format='%Y-%m')

    # Fill missing months
    months_in_fy = pd.date_range(start=start_of_financial_year, end=end_of_financial_year, freq="MS")
    months_df = pd.DataFrame({
        "month": months_in_fy,
        "month_label": [m.strftime("%b-%Y") for m in months_in_fy]
    })

    current_data = months_df.merge(current_data, on='month', how='left')
    current_data['total_amount'] = current_data['total_amount'].fillna(0)
    current_data = current_data.sort_values(by='month')
    total_invoiced_current = current_data['total_amount'].sum()

    # Previous year
    previous_rows = list(client.query(previous_query))
    previous_data = pd.DataFrame([dict(row) for row in previous_rows])
    total_invoiced_previous = previous_data['total_amount'].sum()

    # YOY % difference for metrics
    if total_invoiced_previous == 0:
        percent_diff = 0.0
    else:
        diff = total_invoiced_current - total_invoiced_previous
        percent_diff = (diff / total_invoiced_previous) * 100

    # --- 2) Client-Level Data for Table ---
    current_clients_rows = list(client.query(current_clients_query))
    prev_clients_rows = list(client.query(previous_clients_query))

    current_clients_df = pd.DataFrame([dict(row) for row in current_clients_rows])
    prev_clients_df = pd.DataFrame([dict(row) for row in prev_clients_rows])

    # Merge on client_name
    clients_merged = pd.merge(
        current_clients_df, 
        prev_clients_df, 
        on='client_name', 
        how='outer'
    ).fillna(0)

    # Convert numeric difference into a % difference for the table
    def calc_percentage_diff(row):
        if row["revenue_previous"] == 0:
            return None  # We'll display a dash instead of 0%
        return ((row["revenue_current"] - row["revenue_previous"]) / row["revenue_previous"]) * 100

    clients_merged["% Difference"] = clients_merged.apply(calc_percentage_diff, axis=1)

    # Rename columns for final display
    clients_merged.rename(columns={
        'client_name': 'Client name',
        'revenue_current': 'Revenue YTD',
        'revenue_previous': 'Revenue previous YTD'
    }, inplace=True)

    # Sort by largest YTD revenue
    clients_merged.sort_values(by='Revenue YTD', ascending=False, inplace=True)

    # (A) Insert totals row at bottom
    # We'll match the top-level metrics:
    totals_row = {
        'Client name': 'Total',
        'Revenue YTD': total_invoiced_current,
        'Revenue previous YTD': total_invoiced_previous,
        '% Difference': percent_diff  # matches the third metric exactly
    }
    # Use concat or append to place this row at the end
    clients_merged = pd.concat([clients_merged, pd.DataFrame([totals_row])], ignore_index=True)

    # --- 3) Display UI ---

    # A) Metrics Row
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label=f"Current (1 Oct - {end_of_financial_year.strftime('%d %b %Y')})",
            value=f"£{total_invoiced_current:,.2f}",
        )
    with col2:
        st.metric(
            label=f"Previous (1 Oct - {end_of_previous_financial_year.strftime('%d %b %Y')})",
            value=f"£{total_invoiced_previous:,.2f}",
        )
    with col3:
        st.metric(
            label="YOY % Change",
            value=f"{percent_diff:,.1f}%",
        )

    # B) Vertical padding between metrics and chart
    st.write("")

    # C) Chart
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

    # D) Table (Styled with Pandas Styler)
    st.subheader("Revenue by Client")

    display_columns = ["Client name", "Revenue YTD", "Revenue previous YTD", "% Difference"]
    df_for_styling = clients_merged[display_columns].copy()

    # Conditional styling function for % Difference
    def highlight_diff(val):
        """Color % green if positive, red if negative."""
        # If it's None or not numeric, do nothing
        if val is None or not isinstance(val, (float, int)):
            return ""
        return "color: green" if val > 0 else ("color: red" if val < 0 else "")

    # 1) For None values in "% Difference", we want to show '-'
    # 2) Convert numeric to one decimal place + '%'
    # 3) Currency formatting for 'Revenue YTD' and 'Revenue previous YTD'
    styled_df = df_for_styling.style \
        .hide_index() \
        .format({
            "Revenue YTD": "£{:,.2f}",
            "Revenue previous YTD": "£{:,.2f}",
            "% Difference": lambda x: "-" if pd.isnull(x) else f"{x:,.1f}%"
        }) \
        .applymap(highlight_diff, subset=["% Difference"])

    st.write(styled_df.to_html(), unsafe_allow_html=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")