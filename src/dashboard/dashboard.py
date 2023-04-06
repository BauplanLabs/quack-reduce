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
S3_BUCKET = os.environ['S3_BUCKET']
# note: this is the same file we exported in the top_pickup_locations.sql query
# as part of our data transformation pipeline
PARQUET_FILE = 's3://{}/dashboard/my_view.parquet'.format(S3_BUCKET)

# import querying functoin from the runner
sys.path.insert(0,'..')
from quack import fetch_all
# build up the dashboard
st.markdown("# Trip Dashboard")
st.write("This dashboard shows KPIs for our taxi business.")
st.header("Top pickup locations (map id) by number of trips")
# hardcode the columns
COLS = ['PICKUP_LOCATION_ID', 'TRIPS']

# get the total row count
query = "SELECT COUNT(*) AS C FROM read_parquet(['{}'])".format(PARQUET_FILE)
df, metadata = fetch_all(query, limit=1, display=False, is_debug=False)
st.write("Total row count: {}".format(df['C'][0]))

# get the interactive chart
base_query = """
    SELECT 
        location_id AS {}, 
        counts AS {} 
    FROM 
        read_parquet(['{}'])
    """.format(COLS[0], COLS[1], PARQUET_FILE).strip()
top_k = st.text_input('# of pickup locations', '5')
# add a limit to the query based on the user input
final_query = "{} LIMIT {};".format(base_query, top_k).format(top_k)
df, metadata = fetch_all(final_query, limit=int(top_k), display=False, is_debug=False)

# if no error is returned, we plot the data
if df is not None:
    fig = plt.figure(figsize=(10,5))
    sns.barplot(
        x = COLS[0],
        y = COLS[1],
        data = df,
        order=df.sort_values(COLS[1],ascending = False)[COLS[0]])
    plt.xticks(rotation=70)
    plt.tight_layout()
    st.pyplot(fig)
else:
    st.write("Sorry, something went wrong :-(")

# display metadata
st.write("Roundtrip ms: {}".format(metadata['roundtrip_time']))
st.write("Query exec. time ms: {}".format(metadata['timeMs']))
st.write("Lambda is warm: {}".format(metadata['warm']))
        
