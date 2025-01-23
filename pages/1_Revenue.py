import os
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
import json

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
client = bigquery.Client.from_service_account_info(credentials_dict)

# 3. Calculate the current financial year start/end
today = datetime.today()
if today.month >= 10:
    start_of_financial_year = datetime(today.year, 10, 1)
    end_of_financial_year = datetime(today.year + 1, 9, 30)
else:
    start_of_financial_year = datetime(today.year - 1, 10, 1)
    end_of_financial_year = datetime(today.year, 9, 30)

# 4. Calculate the previous financial year dates
start_of_previous_financial_year = pd.Timestamp(start_of_financial_year) - pd.DateOffset(years=1)
end_of_previous_financial_year = pd.Timestamp(end_of_financial_year) - pd.DateOffset(years=1)

months_in_fy = pd.date_range(
    start=start_of_financial_year,
    end=end_of_financial_year,
    freq="MS"
).to_pydatetime()

months_df = pd.DataFrame({
    "month": months_in_fy,
    "month_label": [m.strftime("%b-%Y") for m in months_in_fy]
})

start_date_str = start_of_financial_year.strftime('%Y-%m-%d')
end_date_str = end_of_financial_year.strftime('%Y-%m-%d')
prev_start_date_str = start_of_previous_financial_year.strftime('%Y-%m-%d')
prev_end_date_str = end_of_previous_financial_year.strftime('%Y-%m-%d')

# 5. Define queries
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

try:
    # Execute current year query
    current_query_job = client.query(current_query)
    current_rows = list(current_query_job)
    current_data = pd.DataFrame([dict(row) for row in current_rows])
    current_data['month'] = pd.to_datetime(current_data['month'], format='%Y-%m')
    current_data = months_df.merge(current_data, on='month', how='left')
    current_data['total_amount'] = current_data['total_amount'].fillna(0)
    current_data = current_data.sort_values(by='month')
    total_invoiced_current = current_data['total_amount'].sum()

    # Execute previous year query
    previous_query_job = client.query(previous_query)
    previous_rows = list(previous_query_job)
    previous_data = pd.DataFrame([dict(row) for row in previous_rows])
    total_invoiced_previous = previous_data['total_amount'].sum()

    # Display metrics
    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            label=f"Total Invoiced ({start_of_financial_year.strftime('%d %b %Y')} - {end_of_financial_year.strftime('%d %b %Y')})",
            value=f"£{total_invoiced_current:,.2f}",
        )
    with col2:
        st.metric(
            label=f"Total Invoiced ({start_of_previous_financial_year.strftime('%d %b %Y')} - {end_of_previous_financial_year.strftime('%d %b %Y')})",
            value=f"£{total_invoiced_previous:,.2f}",
        )

    # Create a bar chart with Altair
    chart = alt.Chart(current_data).mark_bar().encode(
        x=alt.X('month_label:N', title="Month", sort=list(current_data['month_label'])),
        y=alt.Y('total_amount:Q', title="Total Invoiced (£)"),
        tooltip=[
            alt.Tooltip('month_label:N', title='Month'),
            alt.Tooltip('total_amount:Q', title='Total Invoiced (£)', format=',.2f')
        ]
    ).properties(
        title="Invoiced Amount by Month (YTD)",
        width="container",
        height=400
    )

    st.altair_chart(chart, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")