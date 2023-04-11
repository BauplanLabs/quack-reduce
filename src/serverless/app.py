import uuid
import os
import time
import duckdb
import pandas as pd


con = None # global conn object - we re-use this across calls
DEFAULT_LIMIT = 20 # if we don't specify a limit, we will return at most 20 results


def return_duckdb_connection():
    """
    Return a duckdb connection object
    """
    duckdb_connection = duckdb.connect(database=':memory:')
    duckdb_connection.execute("""
        LOAD httpfs;
        SET s3_region='{}';
        SET s3_session_token='{}';
    """.format(os.environ['AWS_REGION'], os.environ['AWS_SESSION_TOKEN'])
    )

    return duckdb_connection


def handler(event, context):
    """
    Run a SQL query in a memory db as a serverless function
    """
    is_warm = False
    # run a timer for info
    start = time.time()
    global con
    if not con:
        # create a new connection
        con = return_duckdb_connection()
    else:
        # return to the caller the status of the lambda
        is_warm = True

    # get the query to be executed from the payload
    event_query = event.get('q', None)
    limit = int(event.get('limit', DEFAULT_LIMIT))
    results = []
    if not event_query:
        print("No query provided, will return empty results")
    else:
        # execute the query and return a pandas dataframe
        _df = con.execute(event_query).df()
        # take rows up the limit, to avoid crashing the lambda
        # by returning too many results
        _df = _df.head(limit)
        results = convert_records_to_json(_df)
    
    # return response to the client with metadata
    return wrap_response(start, event_query, results, is_warm)


def convert_records_to_json(_df):
    if len(_df) > 0:
        # convert timestamp to string to avoid serialization issues
        cols = [col for col in _df.columns if _df[col].dtype == 'datetime64[ns]']
        _df = _df.astype({_: str for _ in cols})

    return _df.to_dict('records')


def wrap_response(start, event_query, results, is_warm):
    """
    Wrap the response in a format that can be used by the client
    """
    return {
        "metadata": {
            "timeMs": int((time.time() - start) * 1000.0),
            "epochMs": int(time.time() * 1000),
            "eventId": str(uuid.uuid4()),
            "query": event_query,
            "warm": is_warm
        },
        "data": {
            "records": results
        }
    }
