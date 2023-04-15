"""

Python script to run a one-time setup for testing the serverless duckdb architecture.

Check the README.md for more details and for the prerequisites.

"""


import os

import boto3
import requests
import pandas as pd
from dotenv import load_dotenv


# get the environment variables from the .env file
load_dotenv()


def donwload_data(url: str, target_file: str):
    """
    Download a file from a url and save it to a target file.
    """
    r = requests.get(url)
    open(target_file, 'wb').write(r.content)

    return True


def download_taxi_data():
    """
    Download the taxi data from the duckdb repo - if the file disappears, 
    you can of course replace it with any other version of the same dataset.
    """
    print('Downloading the taxi dataset')
    
    url = 'https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet'
    file_name = 'data/taxi_2019_04.parquet'
    donwload_data(url, file_name)

    return file_name


def upload_file_to_bucket(s3_client, file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket.
    """
    from botocore.exceptions import ClientError
    
    try:
        print(f"Uploading {object_name}")
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(f"Error uploading file {file_name} to bucket {bucket} with error {e}")
        return False
    
    return True


def upload_datasets(s3_client, bucket: str, taxi_dataset_path: str):
    """
    Upload the datasets to the bucket, first as one parquet file, then as
    a directory of parquet files with hive partitioning.
    """
    file_name = os.path.basename(taxi_dataset_path)
    # upload file as is, a single parquet file in the data/ folder of the target bucket
    is_uploaded = upload_file_to_bucket(
        s3_client, 
        taxi_dataset_path,
        bucket,
        object_name=f"dataset/{file_name}"
    )
    is_uploaded = upload_partioned_dataset(
        bucket, 
        taxi_dataset_path
        )

    return


def upload_partioned_dataset(
        bucket: str,
        taxi_dataset_path: str,
        partition_col: str = 'date'
        ):
    """
    Use pandas to read the parquet file, then save it again as a directory
    on our s3 bucket. The final directory will have a subdirectory for each
    value of the partition column, and each subdirectory will contain parquet files.
    """

    df = pd.read_parquet(taxi_dataset_path)
    df[partition_col] = pd.to_datetime(df['pickup_at']).dt.date
    target_folder = os.path.join('s3://', bucket, 'partitioned')
    print(f"Saving data with hive partitioning ({partition_col}) in {target_folder}")
    df.to_parquet(target_folder, partition_cols=[partition_col])
    
    return True


def setup_project():
    # check vars are ok
    assert 'S3_BUCKET_NAME' in os.environ, "Please set the S3_BUCKET_NAME environment variable"
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
    AWS_PROFILE = os.environ.get('AWS_PROFILE')
    assert (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY) or AWS_PROFILE, "Please set the AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY (or the AWS_PROFILE) environment variables"
    
    # first download the data
    taxi_dataset_path = download_taxi_data()
    # upload the data to the bucket
    s3_client = boto3.client('s3')
    upload_datasets(
        s3_client,
        os.environ['S3_BUCKET_NAME'],
        taxi_dataset_path
        )
    # all done
    print("All done! See you, duck cowboy!")
    return     


if __name__ == "__main__":
    setup_project()