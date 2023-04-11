"""

This Python script benchmarks the performance of running queries using duckdb as engine and 
an object storage as data source. 

The script is minimal and not very configurable, but should be enough to give you a 
feeling of how the different setups perform compared to each other,
and the trade-offs involved.

Please note we basically start with 2019-04-01 and based on the -d flag we add days to the date.
So by increasing -d you will increase the amount of data to be processed. The map reduce version
is a manually unpacks the queries into queries-by-date and then runs them in parallel.

Due to cold start, the first run of the serverless version may be slower than the others, so you should
re-run the same script multiple times to get a better idea of the performance.

"""

import os
import duckdb
import json
import statistics
import time
from fastcore.parallel import parallel
from quack import invoke_lambda, display_table
from dotenv import load_dotenv
from collections import defaultdict
from rich.console import Console


# get the environment variables from the .env file
load_dotenv()


def run_benchmarks(
    bucket: str,
    repetitions: int,
    threads: int,
    days: int,
    is_debug: bool = False
):
    test_location_id = 237
    execution_times = []
    # NOTE: as usual we re-use the same naming convention as in the setup script
    # and all the others
    partitioned_dataset_scan = 's3://{}/partitioned/*/*.parquet'.format(bucket)
    # run the map reduce version
    repetition_times = []
    print("\n====> Running map reduce version")
    for i in range(repetitions):
        start_time = time.time()
        map_reduce_results = run_map_reduce(
            bucket=bucket,
            days=days,
            threads=threads,
            is_debug=is_debug
        )
        repetition_times.append(time.time() - start_time)
        time.sleep(3)
    
    execution_times.append({
        'type': 'map_reduce',
        'mean': round(sum(repetition_times) / len(repetition_times), 3),
        'std': round(statistics.stdev(repetition_times), 3),
        'test location': map_reduce_results[test_location_id]
    })
    # run the standard serverless version
    repetition_times = []
    print("\n====> Running serverless duckdb")
    for i in range(repetitions):
        start_time = time.time()
        results = run_serverless_lambda(
            partitioned_dataset_scan=partitioned_dataset_scan,
            days=days,
            is_debug=is_debug
        )
        repetition_times.append(time.time() - start_time)
        time.sleep(3)

    execution_times.append({
        'type': 'serverless',
        'mean': round(sum(repetition_times) / len(repetition_times), 3),
        'std': round(statistics.stdev(repetition_times), 3),
        'test location': results[test_location_id]
    })
    # run a local db querying the data lake
    repetition_times = []
    print("\n====> Running local duckdb")
    for i in range(repetitions):
        start_time = time.time()
        # just re-use the code inside the lambda without thinking too much ;-)
        con = duckdb.connect(database=':memory:')
        con.execute("""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_region='{}';
            SET s3_access_key_id='{}';
            SET s3_secret_access_key='{}';
        """.format(os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'), os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY']))
        local_results = run_local_db(
            con=con,
            partitioned_dataset_scan=partitioned_dataset_scan,
            days=days,
            is_debug=is_debug
        )
        del con
        repetition_times.append(time.time() - start_time)
        time.sleep(3)
    
    execution_times.append({
        'type': 'local',
        'mean': round(sum(repetition_times) / len(repetition_times), 3),
        'std': round(statistics.stdev(repetition_times), 3),
        'test location': local_results[test_location_id]
    })
    # make sure the results are the same
    assert results[test_location_id] == map_reduce_results[test_location_id] == local_results[test_location_id], "The results are not the same!"

    # display results in a table
    console = Console()
    display_table(console, execution_times, title="Benchmarks", color="cyan")

    # all done, say goodbye
    print("All done! See you, duck cowboy!")
    return     


def run_local_db(
    con,
    partitioned_dataset_scan: str,
    days: int,
    is_debug: bool
):
    single_query = """
        SELECT 
            pickup_location_id AS location_id, 
            COUNT(*) AS counts 
        FROM 
            parquet_scan('{}', HIVE_PARTITIONING=1)
        WHERE 
            DATE >= '2019-04-01' AND DATE < '2019-04-{}'
        GROUP BY 1
    """.format(
        partitioned_dataset_scan,
        "{:02d}".format(1 + days)
    )
    if is_debug:
        print(single_query)
    # just re-use the code inside the lambda with no particular changes
    _df = con.execute(single_query).df()
    _df = _df.head(1000)
    records = _df.to_dict('records')

    return { row['location_id']: row['counts'] for row in records }


def run_serverless_lambda(
    partitioned_dataset_scan: str,
    days: int,
    is_debug: bool
):
    single_query = """
        SELECT 
            pickup_location_id AS location_id, 
            COUNT(*) AS counts 
        FROM 
            parquet_scan('{}', HIVE_PARTITIONING=1)
        WHERE 
            DATE >= '2019-04-01' AND DATE < '2019-04-{}'
        GROUP BY 1
    """.format(
        partitioned_dataset_scan,
        "{:02d}".format(1 + days)
    )
    if is_debug:
        print(single_query)
    response = invoke_lambda(json.dumps({ 'q': single_query, 'limit': 1000}))
    if 'errorMessage' in response:
        print(response['errorMessage'])
        raise Exception("There was an error in the serverless invocation")
    records = response['data']['records']

    return { row['location_id']: row['counts'] for row in records }


def run_map_reduce(
    bucket: str,
    days: int,
    threads: int,
    is_debug: bool
):
    query = """
        SELECT 
            pickup_location_id AS location_id, 
            COUNT(*) AS counts 
        FROM 
            read_parquet('{}', HIVE_PARTITIONING=1)
        WHERE 
            DATE >= '2019-04-{}' AND DATE < '2019-04-{}'
        GROUP BY 1
    """.strip()
    # prepare the queries for the map step
    queries = prepare_map_queries(query, bucket, days)
    if is_debug:
        print(queries[:3])
    assert len(queries) == days, "The number of queries is not correct"
    # run the queries in parallel
    payloads = [json.dumps({'q': q, 'limit': 1000 }) for q in queries]
    _results = parallel(
            invoke_lambda, 
            payloads,
            n_workers=threads)
    # check for errors in ANY response
    if any(['errorMessage' in response for response in _results]):
        print(next(response['errorMessage'] for response in _results if 'errorMessage' in response))
        raise Exception("There was an error in the parallel invocation")
    # do the "reduce" step in code
    results = defaultdict(lambda: 0)
    # loop over the results
    for response in _results:
        records = response['data']['records']
        for row in records:
            results[row['location_id']] += row['counts']
        
    return results


def prepare_map_queries(
        query: str,
        bucket: str,
        days: int
        ):
    # template for parquet scan
    queries = []
    for i in range(1, days + 1):
        start_day_as_str = "{:02d}".format(i)
        end_day_as_str = "{:02d}".format(i + 1)
        parquet_scan = 's3://{}/partitioned/date=2019-04-{}/*.parquet'.format(bucket, start_day_as_str)
        queries.append(query.format(parquet_scan, start_day_as_str, end_day_as_str))
    
    return queries

if __name__ == "__main__":
    # make sure the envs are set
    assert os.environ['S3_BUCKET_NAME'], "Please set the S3_BUCKET_NAME environment variable"
    assert os.environ['AWS_ACCESS_KEY_ID'], "Please set the AWS_ACCESS_KEY_ID environment variable"
    assert os.environ['AWS_SECRET_ACCESS_KEY'], "Please set the AWS_SECRET_ACCESS_KEY environment variable"
    # get args from command line
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        type=int,
        help="number of repetitions", 
        default=3)
    # note: without reserved concurrency, too much concurrency will cause errors
    parser.add_argument(
        "-t",
        type=int,
        help="concurrent queries for map reduce", 
        default=20)
    parser.add_argument(
        "-d",
        type=int,
        help="number of days in April to query", 
        default=20)
    parser.add_argument(
        "--debug", 
        action="store_true",
        help="increase output verbosity",
        default=False)
    args = parser.parse_args()
    # run the main function
    run_benchmarks(
        bucket=os.environ['S3_BUCKET_NAME'],
        repetitions=args.n,
        threads=args.t,
        days=args.d,
        is_debug=args.debug
    )