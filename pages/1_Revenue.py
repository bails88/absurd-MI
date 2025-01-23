import os
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime

st.title("Revenue")
# Your existing revenue tracking code


# Set up BigQuery credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bigquery.json"

# Initialize BigQuery client
client = bigquery.Client()

# Calculate the current financial year start and end dates
today = datetime.today()
if today.month >= 10:  # October to December: current year is the financial year start
    start_of_financial_year = datetime(today.year, 10, 1)
    end_of_financial_year = datetime(today.year + 1, 9, 30)
else:  # January to September: previous year is the financial year start
    start_of_financial_year = datetime(today.year - 1, 10, 1)
    end_of_financial_year = datetime(today.year, 9, 30)

# Calculate the previous financial year start and end dates
start_of_previous_financial_year = start_of_financial_year - pd.DateOffset(years=1)
end_of_previous_financial_year = end_of_financial_year - pd.DateOffset(years=1)

# Generate a list of all months in the financial year
months_in_fy = pd.date_range(
    start=start_of_financial_year,
    end=end_of_financial_year,
    freq="MS"  # Month Start Frequency
).to_pydatetime()

# Convert to a DataFrame for joining later
months_df = pd.DataFrame({
    "month": months_in_fy,
    "month_label": [m.strftime("%b-%Y") for m in months_in_fy]
})

# Convert dates to strings for BigQuery
start_date_str = start_of_financial_year.strftime('%Y-%m-%d')
end_date_str = end_of_financial_year.strftime('%Y-%m-%d')

# Convert previous financial year dates to strings
prev_start_date_str = start_of_previous_financial_year.strftime('%Y-%m-%d')
prev_end_date_str = end_of_previous_financial_year.strftime('%Y-%m-%d')

# Query to calculate the total line item amounts by month
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
    # Execute the current year query and get results
    current_query_job = client.query(current_query)
    current_rows = list(current_query_job)
    
    # Convert to DataFrame
    current_data = [dict(row) for row in current_rows]
    current_result = pd.DataFrame(current_data)

    # Convert 'month' to datetime for proper handling
    current_result['month'] = pd.to_datetime(current_result['month'], format='%Y-%m')

    # Merge the query result with the full financial year months to fill in missing months
    current_result = months_df.merge(current_result, on='month', how='left')

    # Replace NaN values in 'total_amount' with 0 for months with no data
    current_result['total_amount'] = current_result['total_amount'].fillna(0)

    # Ensure proper ordering of months
    current_result = current_result.sort_values(by='month')

    # Calculate the total invoiced for the current financial year
    total_invoiced_current = current_result['total_amount'].sum()

    # Execute the previous year query and get results
    previous_query_job = client.query(previous_query)
    previous_rows = list(previous_query_job)

    # Convert to DataFrame
    previous_data = [dict(row) for row in previous_rows]
    previous_result = pd.DataFrame(previous_data)

    # Calculate the total invoiced for the previous financial year
    total_invoiced_previous = previous_result['total_amount'].sum()

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
    chart = alt.Chart(current_result).mark_bar().encode(
        x=alt.X('month_label:N', title="Month", sort=list(current_result['month_label'])),  # Explicit order
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

    # Display the chart
    st.altair_chart(chart, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")