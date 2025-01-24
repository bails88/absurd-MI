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

# 1. Cache credentials to avoid reloading on every rerun
@st.cache_data
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

# 2. Cache day-by-day data to speed up frontend
@st.cache_data
def load_day_by_day_data(client, start_date):
    """
    Return day-by-day sums of 'due_amount' from 1 Oct onward.
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
      due_date ASC
    """
    job = client.query(query)
    rows = list(job)
    df = pd.DataFrame([dict(r) for r in rows])
    return df

# 3. Cache month-by-month data for the pivot table
@st.cache_data
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
    # 4. Set page configuration with emoji and wide layout
    st.set_page_config(page_title="Cashflow 💸", layout="wide")
    
    st.title("Cashflow 💸")

    # 5. Initialize BigQuery client
    client = load_credentials()

    # 6. Determine the start of the current financial year
    today = datetime.today()
    if today.month >= 10:
        start_of_financial_year = datetime(today.year, 10, 1)
    else:
        start_of_financial_year = datetime(today.year - 1, 10, 1)

    start_date_str = start_of_financial_year.strftime('%Y-%m-%d')

    st.markdown(f"**Data from {start_date_str} onwards, day by day, using `due_date` and summing `due_amount`.**")

    # 7. Load day-by-day data from BigQuery with caching
    with st.spinner("Loading day-by-day cashflow data..."):
        day_df = load_day_by_day_data(client, start_date_str)

    # 8. Convert 'due_date' to datetime
    if not day_df.empty:
        day_df['due_date'] = pd.to_datetime(day_df['due_date'])
    else:
        # Create empty columns to avoid errors in the chart
        day_df = pd.DataFrame(columns=['due_date', 'total_amount'])

    # 9. Day-by-day line chart
    if day_df.empty:
        st.info("No cashflow data found after 1 Oct.")
    else:
        chart = alt.Chart(day_df).mark_line(
            point=alt.OverlayMarkDef(color='#FF4B4B'),  # Red color for points
            color='#FF4B4B'  # Red line (Streamlit brand color)
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

    # 10. Load month-by-month data from BigQuery with caching
    with st.spinner("Loading month-by-month cashflow data..."):
        month_df = load_month_by_month_data(client, start_date_str)

    # 11. Process month-by-month data
    if not month_df.empty:
        # Convert "2023-01" to datetime, then to "Jan-2023"
        month_df['month_year'] = pd.to_datetime(month_df['month_year'], format='%Y-%m')
        month_df['month_label'] = month_df['month_year'].dt.strftime('%b-%Y')
    else:
        # Create empty columns to avoid errors
        month_df = pd.DataFrame(columns=['month_year', 'total_amount'])

    # 12. Display month-by-month pivot table
    st.subheader("Month-by-Month Totals")

    if month_df.empty:
        st.info("No monthly data found.")
    else:
        # Pivot so each month_label is a column, single row
        pivot_table = month_df.pivot_table(
            index=[],  # No row index
            columns='month_label',
            values='total_amount',
            aggfunc='sum',
            fill_value=0
        )

        # Sort the columns in chronological order
        try:
            sorted_columns = sorted(pivot_table.columns, key=lambda x: datetime.strptime(x, '%b-%Y'))
            pivot_table = pivot_table[sorted_columns]
        except Exception as e:
            st.error(f"Error sorting columns: {e}")

        # Hide the index
        pivot_table = pivot_table.reset_index(drop=True)

        # Format as currency
        pivot_table = pivot_table.applymap(lambda x: f"£{x:,.2f}")

        # Display the table without an index
        st.table(pivot_table)

if __name__ == "__main__":
    main()