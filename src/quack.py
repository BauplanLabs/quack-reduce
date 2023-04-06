"""

Python script to interact with the serverless architecture. Query and limit parameters
can be passed through the command line, or the script can be run without parameters
to check the status of the lambda (it will return the results from a pre-defined query).


Check the README.md file for more details.

"""


import os
import boto3
import json
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv


# get the environment variables from the .env file
load_dotenv()
# we don't allow to display more than 10 rows in the terminal
MAX_ROWS_IN_TERMINAL = 10
# instantiate the boto3 client to communicate with the lambda
lambda_client = boto3.client(
   'lambda', 
   aws_access_key_id=os.environ['S3_USER'],
   aws_secret_access_key=os.environ['S3_ACCESS'],
   region_name='us-east-1'
   )


def invoke_lambda(json_payload_as_str: str):
    """
    Invoke our duckdb lambda function. Note that the payload is a string,
    so the method should be called with json.dumps(payload)
    """
    response = lambda_client.invoke(
        # the name of the lambda function should match what you have in your console
        # if you did not change the serverless.yml file, it should be this one:
        FunctionName='quack-reduce-lambda-dev-duckdb',
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=json_payload_as_str
    )

    # return response as dict
    return json.loads(response['Payload'].read().decode("utf-8"))


def fetch_all(
    query: str,
    limit: int,
    is_debug = False
):
    """
    Get results from lambda and display them
    """
    if is_debug:
        print("Running query: {}, with limit: {}".format(query, limit))
    # run the query
    response = invoke_lambda(json.dumps({'q': query, 'limit': limit}))
    # check for errors first
    if 'errorMessage' in response:
        print("Error: {}".format(response['errorMessage']))
        return
    # no error returned, display the results
    if is_debug:
        print("Debug reponse: {}".format(response))

    rows = response['data']['records']
    console = Console()
    display_query_metadata(console, response['metadata'])
    display_table(console, rows)
    

    return


def display_query_metadata(
        console: Console, 
        metadata: dict
        ):
    """
    Display the metadata returned by the lambda - we receive a dictionary with
    few properties (total time, echo of the query, is warm, etc.)
    """
    # NOTE: we cut to 25 max the field values, to avoid the table to be too wide
    values = [{ 'Field': k, 'Value': str(v)[:50] } for k, v in metadata.items()]
    display_table(console, values, title="Metadata", color="cyan")

    return


def display_table(
        console: Console,
        rows: list, 
        title: str="My query", 
        color: str="green"
        ):
    """
    We receive a list of rows, each row is a dict with the column names as keys.

    We use rich (https://rich.readthedocs.io/en/stable/tables.html) to display a nice table in the terminal
    """
    # build the table
    table = Table(title=title)
    # buld the header
    cols = list(rows[0].keys())
    for col in cols:
        table.add_column(col, justify="left", style=color, no_wrap=True)
    # add the rows
    for row in rows[:MAX_ROWS_IN_TERMINAL]:
        # NOTE: we need to render str
        table.add_row(*[str(row[col]) for col in cols])
    # diplay the table
    console.print(table)

    return


def runner(
    bucket: str,
    query: str=None,
    limit: int=10,
    is_debug: bool = False
):
    """
    Run queries against our serverless (and stateless) database.

    We basically use duckdb not as a database much, but as an engine, and use
    object storage to store artifacts, like tables.

    If query and limits are not specified, we overwrite them with sensible choices.
    """
    # if no query is specified, we run a simple count to verify that the lambda is working
    if query is None:
        # NOTE: the file path, after the bucket, should be the same as the one we have
        # in the run_me_first.py script. If you changed it there, you should change it here
        target_file = 's3://{}/dataset/taxi_2019_04.parquet'.format(bucket)
        query = "SELECT COUNT(*) AS COUNTS FROM read_parquet(['{}'])".format(target_file)
        # since this is a test query, we force debug to be True
        rows = fetch_all(query, limit, is_debug=True)

    return


if __name__ == "__main__":
   # get args from command line

   # run the main function
   runner(
       bucket=os.environ['S3_BUCKET'],
       query=None,
       limit=10,
       is_debug=False
   )