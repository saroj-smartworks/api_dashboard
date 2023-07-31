import pandas as pd
import plotly.express as px
import streamlit as st
import plotly.graph_objects as go
import psycopg2
import numpy as np
from datetime import datetime

# Establish connection to Redshift
conn = psycopg2.connect(
    host=st.secrets.db_credentials.host,
    port= st.secrets.db_credentials.port,
    database=st.secrets.db_credentials.db,
    user=st.secrets.db_credentials.db_username,
    password=st.secrets.db_credentials.db_password
)

# Specify the table name to read
table_name = 'prod_api_logs_summary'
fr_table_name = 'prod_fr_api_logs_summary'

# Read the table into a DataFrame
query = f"select trigger_date as date, api_name, method, response_status as responseStatus," \
        f"no_of_requests as count from {table_name}"

query_1 = f"select trigger_date as date, api_name, method, response_status as responseStatus," \
          f"no_of_requests as count from {fr_table_name}"

df = pd.read_sql(query, conn)

fr_df = pd.read_sql(query_1, conn)

# df = pd.read_csv('api_log_20230619.csv')

st.set_page_config(page_title='Dashboard Title',
                   page_icon=":crossed_swords:",
                   layout="wide")

# Align the title to the center using CSS styling
st.markdown(
    """
    <h1 style="text-align: center;">API Requests Dashboard</h1>
    """,
    unsafe_allow_html=True
)

# Pivot the table
# pivot_table = df.pivot_table(index=['date', 'method', 'api_name'], columns='responseStatus', values='count', aggfunc='sum', fill_value=0, margins=True, margins_name='Total')

# Pivot the table
pivot_table = df.pivot_table(index=['date', 'method', 'api_name'], columns='responsestatus',
                             values='count', aggfunc='sum', fill_value=0)

fr_pivot_table = fr_df.pivot_table(index=['date', 'method', 'api_name'], columns='responsestatus',
                             values='count', aggfunc='sum', fill_value=0)

pivot_table.reset_index(inplace=True)
fr_pivot_table.reset_index(inplace=True)

# Find the minimum and maximum date values
min_date = df['date'].min()
max_date = df['date'].max()

# Convert min_date and max_date to datetime objects
min_date = pd.to_datetime(min_date)
max_date = pd.to_datetime(max_date)

# Convert date column to datetime type
pivot_table['date'] = pd.to_datetime(pivot_table['date']).dt.date
fr_pivot_table['date'] = pd.to_datetime(fr_pivot_table['date']).dt.date

print(fr_pivot_table)

# Sort the pivot table by the 'Total' column in descending order
# pivot_table = pivot_table.sort_values(by='Total', ascending=False)


# Calculate the sum of the first two columns and the sum of the remaining two columns
fail_sum = pivot_table[[400, 401, 406, 409]].sum(axis=1)
total_sum = pivot_table[[200, 204, 206, 210, 400, 401, 406, 409]].sum(axis=1)

fr_fail_sum = fr_pivot_table[[401, 404]].sum(axis=1)
fr_total_sum = fr_pivot_table[[200, 401, 404]].sum(axis=1)

pivot_table['Total Request'] = total_sum
pivot_table['Total Failures'] = fail_sum

fr_pivot_table['Total Request'] = fr_total_sum
fr_pivot_table['Total Failures'] = fr_fail_sum

# Divide the sum of the first two columns by the sum of the remaining two columns
pivot_table['Fail %'] = (fail_sum / total_sum)*100
pivot_table['Fail %'] = pivot_table['Fail %'].fillna(-1)
pivot_table['Fail %'] = np.where(np.isinf(pivot_table['Fail %']), -1, pivot_table['Fail %'])
pivot_table['Fail %'] = pivot_table['Fail %'].astype(int)

fr_pivot_table['Fail %'] = (fr_fail_sum / fr_total_sum)*100
fr_pivot_table['Fail %'] = fr_pivot_table['Fail %'].fillna(-1)
fr_pivot_table['Fail %'] = np.where(np.isinf(fr_pivot_table['Fail %']), -1, fr_pivot_table['Fail %'])
fr_pivot_table['Fail %'] = fr_pivot_table['Fail %'].astype(int)

# Extract the values for api_type and api_name_extract
pivot_table['api_type'] = pivot_table['api_name'].str.split('/').str[2]
pivot_table['api_name_extract'] = pivot_table['api_name'].str.split('/').str[3]

# Define the custom position for the new columns
position = 1

# Insert the new columns at the custom position
pivot_table.insert(position, 'api_type', pivot_table.pop('api_type'))
pivot_table.insert(position + 1, 'api_name_extract', pivot_table.pop('api_name_extract'))

# Convert values in the 'Name' column to lowercase
pivot_table['api_type'] = pivot_table['api_type'].str.lower()

pivot_table = pivot_table.drop(columns='api_name')

# Reset index to make it false
pivot_table = pivot_table.reset_index(drop=True)

fr_pivot_table = fr_pivot_table.reset_index(drop=True)


# Remove 'Total' from the unique attribute values
attribute_values = pivot_table['method'].unique()
attribute_values = attribute_values[attribute_values != 'Total']

# Add a date filter using Streamlit
with st.sidebar:
    start_date = st.date_input('Start Date', value=min_date, min_value=min_date, max_value=max_date)
    end_date = st.date_input('End Date', value=max_date, min_value=min_date, max_value=max_date)

# Add a filter widget
selected_method = st.sidebar.multiselect('Select Method', options=attribute_values, default=attribute_values)


# Remove 'Total' from the unique attribute values
attribute_values_api_type = pivot_table['api_type'].unique()

# Add a filter widget
selected_type = st.sidebar.multiselect('Select API Type', options=attribute_values_api_type, default=attribute_values_api_type)


# Filter the DataFrame based on the selected attribute
df_selection = pivot_table.query(
    "method == @selected_method & api_type == @selected_type & @start_date <= date <= @end_date"
)

fr_df_selection = fr_pivot_table.query(
    "@start_date <= date <= @end_date & method == @selected_method"
)

# Specify the columns to combine using sum
columns_to_combine_fail = [400, 401, 406, 409]

columns_to_combine_success = [200, 204, 206, 210]

columns_to_combine_total = [200, 204, 206, 210, 400, 401, 406, 409]

columns_to_combine_fail_fr = [401, 404]

columns_to_combine_success_fr = [200]

columns_to_combine_total_fr = [200, 401, 404]

df_line = df_selection.copy()

df_line_fr = fr_df_selection.copy()

#df_line[[200, 204]] = df_line[[200, 204]].apply(pd.to_numeric)

df_line['success'] = df_line[columns_to_combine_success].sum(axis=1)

df_line['fail'] = df_line[columns_to_combine_fail].sum(axis=1)
#
df_line['total'] = df_line[columns_to_combine_total].sum(axis=1)

df_line_fr['success'] = df_line_fr[columns_to_combine_success_fr].sum(axis=1)

df_line_fr['fail'] = df_line_fr[columns_to_combine_fail_fr].sum(axis=1)
#
df_line_fr['total'] = df_line_fr[columns_to_combine_total_fr].sum(axis=1)

# Group by date and calculate the sum of 'success', 'fail', and 'total'
grouped_data = df_line.groupby('date').agg({'success': 'sum', 'fail': 'sum', 'total': 'sum'}).reset_index()
grouped_data['Fail %'] = (grouped_data['fail']/grouped_data['total'])*100
# Calculate the sum of the specified columns
total_fails = df_selection[columns_to_combine_fail].sum().sum()

# Calculate the sum of the specified columns
total_success = df_selection[columns_to_combine_success].sum().sum()

# Calculate the sum of the specified columns
total_requests = df_selection[columns_to_combine_total].sum().sum()


# Group by date and calculate the sum of 'success', 'fail', and 'total'
grouped_data_fr = df_line_fr.groupby('date').agg({'success': 'sum', 'fail': 'sum', 'total': 'sum'}).reset_index()
grouped_data_fr['Fail %'] = (grouped_data_fr['fail']/grouped_data_fr['total'])*100
# Calculate the sum of the specified columns
total_fails_fr = fr_df_selection[columns_to_combine_fail_fr].sum().sum()

# Calculate the sum of the specified columns
total_success_fr = fr_df_selection[columns_to_combine_success_fr].sum().sum()

# Calculate the sum of the specified columns
total_requests_fr = fr_df_selection[columns_to_combine_total_fr].sum().sum()


# creating a single-element container
placeholder_non_fr = st.empty()

with placeholder_non_fr.container():
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Total Request",
            value=total_requests
        )

    with col2:
        st.metric(
            label="Total Success",
            value=total_success
        )

    with col3:
        st.metric(
            label="Total Fail",
            value=total_fails
        )

    with col4:
        st.metric(
            label="Fail %",
            value=int((total_fails/total_requests)*100)
        )

placeholder = st.empty()

with placeholder.container():
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="Total Request[FR]",
            value=total_requests_fr
        )

    with col2:
        st.metric(
            label="Total Success[FR]",
            value=total_success_fr
        )

    with col3:
        st.metric(
            label="Total Fail[FR]",
            value=total_fails_fr
        )

    with col4:
        st.metric(
            label="Fail %[FR]",
            value=int((total_fails_fr/total_requests_fr)*100)
        )


###############################################################################################
    # Display the DataFrame using st.dataframe
    st.write("<h3 style='text-align: center;'>Mobile App API Logs Summary</h3>", unsafe_allow_html=True)
    st.dataframe(df_selection)
    st.write("<h3 style='text-align: center;'>FR API Logs Summary</h3>", unsafe_allow_html=True)
    st.dataframe(fr_df_selection,use_container_width=True)
###############################################################################################

    # Display the timeline chart

    # Create the line chart
    fig_fail_percent = px.line(grouped_data, x="date", y="Fail %", title="Fail Percentage Over Time[Mobile App]")
    # Update the layout of the figure
    fig_fail_percent.update_layout(barmode='group', title=dict(text='Fail Percentage Over Time[Mobile App]', x=0.4), autosize=True,
                      margin=dict(l=0, r=0, t=50, b=50, autoexpand=True))
    st.plotly_chart(fig_fail_percent, use_container_width=True)

    fig_fail_percent_fr = px.line(grouped_data_fr, x="date", y="Fail %", title="Fail Percentage Over Time [FR]")
    # Update the layout of the figure
    fig_fail_percent_fr.update_layout(barmode='group', title=dict(text='Fail Percentage Over Time[FR]', x=0.4), autosize=True,
                                   margin=dict(l=0, r=0, t=50, b=50, autoexpand=True))
    st.plotly_chart(fig_fail_percent_fr, use_container_width=True)

    # Create the side-by-side bar chart with values inside bars
    fig = go.Figure()
    for col in ['total', 'success', 'fail']:
        fig.add_trace(go.Bar(x=grouped_data['date'], y=grouped_data[col], name=col,
                             text=grouped_data[col], textposition='inside'))

    fig.update_layout(barmode='group', title=dict(text='API Request Timeline', x=0.4), autosize=True,
                      margin=dict(l=0, r=0, t=50, b=50, autoexpand=True))
    fig.update_xaxes(type='category')
    st.plotly_chart(fig, use_container_width=True)
