import os
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from google.cloud import bigquery

def load_credentials():
    credentials_dict = {
        "type": os.environ["GCP_TYPE"],
        "project_id": os.environ["GCP_PROJECT_ID"],
        "private_key_id": os.environ["GCP_PRIVATE_KEY_ID"],
        "private_key": os.environ["GCP_PRIVATE_KEY"],
        "client_email": os.environ["GCP_CLIENT_EMAIL"],
        "client_id": os.environ["GCP_CLIENT_ID"],
        "auth_uri": os.environ["GCP_AUTH_URI"],
        "token_uri": os.environ["GCP_TOKEN_URI"],
        "auth_provider_x509_cert_url": os.environ["GCP_AUTH_PROVIDER_X509_CERT_URL"],
        "client_x509_cert_url": os.environ["GCP_CLIENT_X509_CERT_URL"]
    }
    return bigquery.Client.from_service_account_info(credentials_dict)

def load_cashflow_data(client, start_date):
    """
    Query total amounts by 'due_date', day-by-day, for all data AFTER start_date.
    No upper bound, so any future due dates are included.
    """
    query = f"""
SELECT
  due_date,
  SUM(due_amount) AS total_amount
FROM
  `marketing-434610.harvest.Invoices`
WHERE
  due_date >= '{start_date}'
  AND state IN ('open', 'paid', 'draft')
GROUP BY
  due_date
ORDER BY
  due_date ASC;
"""
    job = client.query(query)
    rows = list(job)
    df = pd.DataFrame([dict(r) for r in rows])
    return df

def main():
    st.title("Cashflow")

    # 1. BigQuery client
    client = load_credentials()

    # 2. Determine "start_of_financial_year" for the current year
    # (if we are in or after October, it's 1 Oct this year; otherwise 1 Oct last year).
    today = datetime.today()
    if today.month >= 10:
        start_of_financial_year = datetime(today.year, 10, 1)
    else:
        start_of_financial_year = datetime(today.year - 1, 10, 1)

    # Convert to string for the query
    start_date_str = start_of_financial_year.strftime('%Y-%m-%d')

    # 3. Load data from 1st October onward (no upper bound)
    cashflow_df = load_cashflow_data(client, start_date_str)

    # 4. Convert 'due_date' to datetime if we have data
    if not cashflow_df.empty:
        cashflow_df['due_date'] = pd.to_datetime(cashflow_df['due_date'])
    else:
        # create empty columns to avoid errors in the chart
        cashflow_df['due_date'] = []
        cashflow_df['total_amount'] = []

    st.write("## Cashflow by day")
    st.write(f"Showing all invoice line-item amounts by `due_date`, starting from {start_date_str} onward. Please note this excludes VAT, so receivables will be higher.")

    # 5. Day-by-day line chart
    if cashflow_df.empty:
        st.info("No cashflow data found after 1 Oct.")
    else:
        chart = alt.Chart(cashflow_df).mark_line(
    point=alt.OverlayMarkDef(color='#FF4B4B'),  # color for points
    color='#FF4B4B'  # color for the line
).encode(
    x=alt.X('due_date:T', title="Due Date"),
    y=alt.Y('total_amount:Q', title="Total Invoiced (£)"),
    tooltip=[
        alt.Tooltip('due_date:T', title='Due Date'),
        alt.Tooltip('total_amount:Q', title='Total Invoiced (£)', format=',.2f')
    ]
).properties(
    width="container",
    height=400
)
        st.altair_chart(chart, use_container_width=True)

if __name__ == "__main__":
    main()