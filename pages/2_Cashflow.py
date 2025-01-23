# pages/2_Cashflow.py

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

def load_day_by_day_data(client, start_date):
    """
    Return day-by-day sums of 'due_amount' from 1 Oct onward.
    We only filter on `due_date >= start_date`, no upper bound.
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
      due_date ASC
    """
    job = client.query(query)
    rows = list(job)
    df = pd.DataFrame([dict(r) for r in rows])
    return df

def load_month_by_month_data(client, start_date):
    """
    Return month-year sums of 'due_amount' from 1 Oct onward
    so we can pivot them into columns.
    """
    query = f"""
    SELECT
      FORMAT_TIMESTAMP('%Y-%m', due_date) AS month_year,
      SUM(due_amount) AS total_amount
    FROM
      `marketing-434610.harvest.Invoices`
    WHERE
      due_date >= '{start_date}'
      AND state IN ('open', 'paid', 'draft')
    GROUP BY
      month_year
    ORDER BY
      month_year ASC
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
    #    (If we are in or after October, it's 1 Oct this year; otherwise 1 Oct last year).
    today = datetime.today()
    if today.month >= 10:
        start_of_financial_year = datetime(today.year, 10, 1)
    else:
        start_of_financial_year = datetime(today.year - 1, 10, 1)

    start_date_str = start_of_financial_year.strftime('%Y-%m-%d')

    # 3. Day-by-Day data
    day_df = load_day_by_day_data(client, start_date_str)
    if not day_df.empty:
        day_df['due_date'] = pd.to_datetime(day_df['due_date'])
    else:
        day_df['due_date'] = []
        day_df['total_amount'] = []

    st.write(f"Data from **{start_date_str}** onwards, day by day, using `due_date` and summing `due_amount`.")

    if day_df.empty:
        st.info("No day-by-day data found.")
    else:
        # A. Make a day-by-day line chart
        chart = alt.Chart(day_df).mark_line(
            point=alt.OverlayMarkDef(color='#FF4B4B'),  # optional red color for points
            color='#FF4B4B'  # red line (Streamlit brand color)
        ).encode(
            x=alt.X('due_date:T', title="Due Date (Daily)"),
            y=alt.Y('total_amount:Q', title="Total (£)"),
            tooltip=[
                alt.Tooltip('due_date:T', title='Date'),
                alt.Tooltip('total_amount:Q', title='Total (£)', format=',.2f')
            ]
        ).properties(
            width="container",
            height=400
        )
        st.altair_chart(chart, use_container_width=True)

    # 4. Month-by-Month Pivot
    st.subheader("Month-by-Month Totals")

    month_df = load_month_by_month_data(client, start_date_str)
    if not month_df.empty:
        # Convert e.g. "2023-01" to a datetime, then to "Jan-2023"
        month_df['month_year'] = pd.to_datetime(month_df['month_year'], format='%Y-%m')
        month_df['month_label'] = month_df['month_year'].dt.strftime('%b-%Y')
    else:
        month_df['month_label'] = []
        month_df['total_amount'] = []

    if month_df.empty:
        st.info("No monthly data found.")
    else:
        # Pivot so each month_label is a column, single row
        pivot_table = month_df.pivot_table(
            index=[],  # no row index
            columns='month_label',
            values='total_amount',
            aggfunc='sum',  # sum in case of duplicates
            fill_value=0
        )

        # If you want only the last 12 columns, you can slice:
        # pivot_table = pivot_table.iloc[:, -12:]

        # Hide the index
        pivot_table = pivot_table.reset_index(drop=True)

        # Format as currency
        pivot_table = pivot_table.applymap(lambda x: f"£{x:,.2f}")

        st.table(pivot_table)

if __name__ == "__main__":
    main()