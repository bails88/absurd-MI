import os
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from google.cloud import bigquery

def load_credentials():
    # Same environment approach as your other pages:
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
        "client_x509_cert_url": os.environ["GCP_CLIENT_X509_CERT_URL"],
    }
    return bigquery.Client.from_service_account_info(credentials_dict)

def load_cashflow_data(client, start_date, end_date):
    """
    Query total amounts by 'due_date', day-by-day.
    """
    query = f"""
    SELECT
      due_date,
      SUM(line_item.amount) AS total_amount
    FROM
      `marketing-434610.harvest.Invoices`,
      UNNEST(line_items) AS line_item
    WHERE
      due_date BETWEEN '{start_date}' AND '{end_date}'
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

    # 1. Initialize BigQuery client
    client = load_credentials()

    # 2. Calculate date range (1 Oct -> Today)
    today = datetime.today()
    if today.month >= 10:
        start_of_financial_year = datetime(today.year, 10, 1)
    else:
        start_of_financial_year = datetime(today.year - 1, 10, 1)
    end_of_financial_year = today

    start_date_str = start_of_financial_year.strftime('%Y-%m-%d')
    end_date_str = end_of_financial_year.strftime('%Y-%m-%d')

    # 3. Load data based on 'due_date'
    cashflow_df = load_cashflow_data(client, start_date_str, end_date_str)

    # 4. Convert to datetime for day-by-day
    if not cashflow_df.empty:
        cashflow_df['due_date'] = pd.to_datetime(cashflow_df['due_date'])
    else:
        # If empty, create columns to avoid errors
        cashflow_df['due_date'] = []
        cashflow_df['total_amount'] = []

    # 5. Create a line chart (day by day)
    st.write("## Cashflow by Day")
    st.write("Showing total revenue amounts based on `due_date`, day by day.")

    if cashflow_df.empty:
        st.info("No cashflow data found for this period.")
    else:
        chart = alt.Chart(cashflow_df).mark_line(point=True).encode(
            x=alt.X('due_date:T', title="Due Date (Day)"),
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

# Standard pattern for multi-page apps
if __name__ == "__main__":
    main()