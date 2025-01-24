# pages/2_Cashflow.py

import os
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime
from google.cloud import bigquery

# Front Matter for custom page title with emoji (optional)
# ---
# title: "Cashflow 💸"
# ---

# 1. Cache the BigQuery client as a resource
@st.cache_resource
def load_credentials():
    """
    Initialize and return a BigQuery client using service account credentials.
    This function is cached as a resource because it returns a non-serializable object.
    """
    credentials_dict = {
        "type": os.environ.get("GCP_TYPE"),
        "project_id": os.environ.get("GCP_PROJECT_ID"),
        "private_key_id": os.environ.get("GCP_PRIVATE_KEY_ID"),
        "private_key": os.environ.get("GCP_PRIVATE_KEY"),
        "client_email": os.environ.get("GCP_CLIENT_EMAIL"),
        "client_id": os.environ.get("GCP_CLIENT_ID"),
        "auth_uri": os.environ.get("GCP_AUTH_URI"),
        "token_uri": os.environ.get("GCP_TOKEN_URI"),
        "auth_provider_x509_cert_url": os.environ.get("GCP_AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.environ.get("GCP_CLIENT_X509_CERT_URL")
    }

    # Validate that all required environment variables are present
    missing_vars = [key for key, value in credentials_dict.items() if value is None]
    if missing_vars:
        st.error(f"Missing environment variables: {', '.join(missing_vars)}")
        st.stop()

    return bigquery.Client.from_service_account_info(credentials_dict)

# 2. Cache data loading functions
@st.cache_data(ttl=600)  # Cache expires after 10 minutes
def load_day_by_day_data(client, start_date):
    """
    Fetch and return day-by-day sums of 'due_amount' and 'tax_amount' from 1 Oct onward.
    No upper bound, so any future due dates are included.
    """
    query = f"""
    SELECT
      due_date,
      SUM(due_amount) + SUM(tax_amount) AS total_amount
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

@st.cache_data(ttl=600)  # Cache expires after 10 minutes
def load_month_by_month_data(client, start_date):
    """
    Fetch and return month-year sums of 'due_amount' and 'tax_amount' from 1 Oct onward
    to pivot into columns.
    """
    query = f"""
    SELECT
      FORMAT_TIMESTAMP('%Y-%m', due_date) AS month_year,
      SUM(due_amount) + SUM(tax_amount) AS total_amount
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
    # Set page configuration with emoji and wide layout
    st.set_page_config(page_title="Cashflow 💸", layout="wide")
    
    st.title("Cashflow 💸")

    # 3. Initialize BigQuery client
    client = load_credentials()

    # 4. Determine the start of the current financial year
    today = datetime.today()
    if today.month >= 10:
        start_of_financial_year = datetime(today.year, 10, 1)
    else:
        start_of_financial_year = datetime(today.year - 1, 10, 1)

    start_date_str = start_of_financial_year.strftime('%Y-%m-%d')

    st.markdown(f"**Data from {start_date_str} onwards, day by day, using `due_date` and summing `due_amount` and `tax_amount`.**")

    # 5. Load day-by-day data from BigQuery with caching
    with st.spinner("Loading day-by-day cashflow data..."):
        day_df = load_day_by_day_data(client, start_date_str)

    # 6. Convert 'due_date' to datetime
    if not day_df.empty:
        day_df['due_date'] = pd.to_datetime(day_df['due_date'])
    else:
        # Create empty DataFrame with correct columns to avoid errors in the chart
        day_df = pd.DataFrame(columns=['due_date', 'total_amount'])

    # 7. Day-by-day line chart
    if day_df.empty:
        st.info("No cashflow data found after 1 Oct.")
    else:
        chart = alt.Chart(day_df).mark_line(
            point=alt.OverlayMarkDef(color='#FF4B4B'),  # red color for points
            color='#FF4B4B'  # red line (Streamlit brand color)
        ).encode(
            x=alt.X('due_date:T', title="Due Date (Daily)"),
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

    # 8. Load month-by-month data with caching
    with st.spinner("Loading month-by-month cashflow data..."):
        month_df = load_month_by_month_data(client, start_date_str)

    # 9. Process month-by-month data
    if not month_df.empty:
        # Convert "2023-01" to datetime, then to "Jan-2023"
        month_df['month_year'] = pd.to_datetime(month_df['month_year'], format='%Y-%m')
        month_df['month_label'] = month_df['month_year'].dt.strftime('%b-%Y')
    else:
        # Create empty DataFrame with correct columns to avoid errors
        month_df = pd.DataFrame(columns=['month_year', 'total_amount'])

    # 10. Display month-by-month pivot table
    st.subheader("Month-by-Month Totals")

    if month_df.empty:
        st.info("No monthly data found.")
    else:
        # Pivot so each month_label is a column, single row
        pivot_table = month_df.pivot_table(
            index=[],  # no row index
            columns='month_label',
            values='total_amount',
            aggfunc='sum',
            fill_value=0
        )

        # Sort the columns in chronological order
        sorted_columns = sorted(pivot_table.columns, key=lambda x: datetime.strptime(x, '%b-%Y'))
        pivot_table = pivot_table[sorted_columns]

        # Hide the index
        pivot_table = pivot_table.reset_index(drop=True)

        # Format as currency
        pivot_table = pivot_table.applymap(lambda x: f"£{x:,.2f}")

        # Display the table without an index
        st.table(pivot_table)

if __name__ == "__main__":
    main()