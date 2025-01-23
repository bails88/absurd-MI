import os
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime

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

# Query to calculate the total line item amounts by month
query = f"""
SELECT 
    FORMAT_TIMESTAMP('%Y-%m', issue_date) AS month,
    SUM(line_item.amount) AS total_amount
FROM 
    `marketing-434610.harvest.Invoices`,
    UNNEST(line_items) AS line_item
WHERE 
    issue_date BETWEEN '{start_date_str}' AND '{end_date_str}'
    AND state IN ('open', 'paid', 'draft') -- Include open, paid, and draft invoices
GROUP BY 
    month
ORDER BY 
    month ASC;
"""

try:
    # Execute the query and get results
    query_job = client.query(query)
    rows = list(query_job)
    
    # Convert to DataFrame
    data = [dict(row) for row in rows]
    result = pd.DataFrame(data)

    # Convert 'month' to datetime for proper handling
    result['month'] = pd.to_datetime(result['month'], format='%Y-%m')

    # Merge the query result with the full financial year months to fill in missing months
    result = months_df.merge(result, on='month', how='left')

    # Replace NaN values in 'total_amount' with 0 for months with no data
    result['total_amount'] = result['total_amount'].fillna(0)

    # Ensure proper ordering of months
    result = result.sort_values(by='month')

    # Display in Streamlit
    st.title("Absurd Management Information")
    st.header("Invoicing")
    
    # Show total invoiced for the financial year to date
    total_invoiced = result['total_amount'].sum()
    st.metric(
        label=f"Total Invoiced ({start_of_financial_year.strftime('%d %b %Y')} - {end_of_financial_year.strftime('%d %b %Y')})",
        value=f"£{total_invoiced:,.2f}",
    )

    # Create a bar chart with Altair
    chart = alt.Chart(result).mark_bar().encode(
        x=alt.X('month_label:N', title="Month", sort=list(result['month_label'])),  # Explicit order
        y=alt.Y('total_amount:Q', title="Total Invoiced (£)"),
        tooltip=[
            alt.Tooltip('month_label:N', title='Month'),
            alt.Tooltip('total_amount:Q', title='Total Invoiced (£)', format=',.2f')
        ]
    ).properties(
        title="Invoiced Amount by Month",
        width="container",
        height=400
    )

    # Display the chart
    st.altair_chart(chart, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred: {str(e)}")