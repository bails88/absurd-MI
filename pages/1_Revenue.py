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

# 3. Calculate start dates for the most recent 1st October up to today
today = datetime.today()
if today.month >= 10:
    start_of_financial_year = datetime(today.year, 10, 1)
else:
    start_of_financial_year = datetime(today.year - 1, 10, 1)

end_of_financial_year = today  # up to today

# Calculate previous year's date range
start_of_previous_financial_year = pd.Timestamp(start_of_financial_year) - pd.DateOffset(years=1)
end_of_previous_financial_year = pd.Timestamp(end_of_financial_year) - pd.DateOffset(years=1)

# Create a monthly range from 1st Oct to 'today'
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

# 4. Queries
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
    # Current year data
    current_rows = list(client.query(current_query))
    current_data = pd.DataFrame([dict(row) for row in current_rows])
    current_data['month'] = pd.to_datetime(current_data['month'], format='%Y-%m')
    current_data = months_df.merge(current_data, on='month', how='left')
    current_data['total_amount'] = current_data['total_amount'].fillna(0)
    current_data = current_data.sort_values(by='month')
    total_invoiced_current = current_data['total_amount'].sum()

    # Previous year data
    previous_rows = list(client.query(previous_query))
    previous_data = pd.DataFrame([dict(row) for row in previous_rows])
    total_invoiced_previous = previous_data['total_amount'].sum()

    # Compute % difference: ((current - previous) / previous) * 100
    # Handle the case if previous is zero
    if total_invoiced_previous == 0:
        percent_diff = 0.0
    else:
        diff = total_invoiced_current - total_invoiced_previous
        percent_diff = (diff / total_invoiced_previous) * 100

    # Display metrics
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
            label="Year-Over-Year % Change",
            value=f"{percent_diff:,.1f}%",
        )

    # Altair bar chart
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

except Exception as e:
    st.error(f"An error occurred: {str(e)}")