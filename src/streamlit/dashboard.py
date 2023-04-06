"""

Simple dashboard for the taxi data based on Streamlit.

It re-uses through hugly imports the code from the quack.py script, and use seaborn to plot the data.

"""

import sys
import os
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import streamlit as st
from dotenv import load_dotenv


# get the environment variables from the .env file
load_dotenv('../.env')
assert os.environ['S3_BUCKET'], "S3_BUCKET is not set"

# import from the bauplan script
sys.path.insert(0,'..')
from quack import invoke_lambda

# build up the dashboard
st.markdown("# Trip Dashboard")
st.write("This dashboard shows KPIs for our taxi business.")
st.header("Trips by pickup location (map id)")
# hard code the columns for now
COLS = ['PICKUP_LOCATION_ID', '# TRIPS']


def parse_response(response, cols):
    if 'data' in response and response['data']['results']:
        # for debug
        # print(response)
        return pd.DataFrame(response['data']['results'], columns=cols)

    return None


# get the total row count
target_parquet_file = 's3://{}/{}/{}.parquet'.format(
        S3_DUMP_BUCKET,
        S3_NAMESPACE_FOLDER,
        'counts_single'  # TODO: make this configurable
    )
count_query = "SELECT COUNT(*) as C FROM read_parquet(['{}'])".format(target_parquet_file)
response = invoke_serverless_duckdb(
        function_name=LAMBDA_NAME,
        arguments={ 'q': count_query },
        serverless_framework='lambda'
    )
df = parse_response(response, cols=['C'])
st.write("Total row count: {}".format(df['C'][0]))

# get the interactive chart
base_query = "SELECT * FROM read_parquet(['{}']) ORDER BY 2 DESC".format(target_parquet_file)
top_k_products = st.text_input('# of pickup locations', '5')
query_template = "{} LIMIT {};".format(base_query, top_k_products)
final_query = query_template.format(top_k_products)
response =invoke_serverless_duckdb(
        function_name=LAMBDA_NAME,
        arguments={ 'q': final_query },
        serverless_framework='lambda'
    )
df = parse_response(response,  cols=COLS)

if df is not None:
    fig = plt.figure(figsize=(10,5))
    sns.barplot(
        x = COLS[0],
        y = COLS[1],
        data = df,
        order=None)
    plt.xticks(rotation=70)
    plt.tight_layout()
    st.pyplot(fig)
else:
    st.write("Sorry, something went wrong :-(")
