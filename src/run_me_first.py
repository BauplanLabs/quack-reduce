"""

Python script to run a one-time setup for testing the serverless duckdb architecture.

Check the README.md file for more details and for the prerequisites.

"""


import os
import boto3
from dotenv import load_dotenv


# get the environment variables from the .env file
load_dotenv()


def donwload_data(url: str, target_file: str):
    """
    Download a file from a url and save it to a target file
    """
    import requests

    r = requests.get(url)
    open(target_file, 'wb').write(r.content)

    return True


def download_taxi_data():
    """
    Download the taxi data from the duckdb repo - if the file disappears, 
    you can of course replace it with any other version of the same dataset.
    """

    url = 'https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet'
    file_name = 'data/taxi_2019_04.parquet'
    donwload_data(url, file_name)

    return file_name


# def create_bucket_if_not_exists(s3_client, bucket_name: str):
#     """
#     Create an S3 bucket if it does not exist. Return True if a bucket was created.
#     """
#     from botocore.client import ClientError
#
#     try:
#         response = s3_client.head_bucket(Bucket=bucket_name)
#     except ClientError:
#         print("The bucket {} does not exist, creating it now".format(bucket_name))
#         bucket = s3_client.create_bucket(Bucket=bucket_name)
#
#         return True
#
#     return False


def upload_file_to_bucket(s3_client, file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket
    """
    from botocore.exceptions import ClientError
    
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print("Error uploading file {} to bucket {} with error {}".format(file_name, bucket, e))
        return False
    
    return True


def upload_datasets(s3_client, bucket: str, taxi_dataset_path: str):
    """
    Upload the datasets to the bucket, first as one parquet file, then as
    a directory of parquet files with hive partitioning
    """
    file_name = os.path.basename(taxi_dataset_path)
    # upload file as is, a single parquet file in the data/ folder of the target bucket
    is_uploaded = upload_file_to_bucket(
        s3_client, 
        taxi_dataset_path,
        bucket,
        object_name='dataset/{}'.format(file_name)
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
    import pandas as pd

    df = pd.read_parquet(taxi_dataset_path)
    df[partition_col] = pd.to_datetime(df['pickup_at']).dt.date
    target_folder = os.path.join('s3://', bucket, 'partitioned')
    print("Saving data with hive partitioning ({}) in {}".format(partition_col, target_folder))
    df.to_parquet(target_folder, partition_cols=[partition_col])
    
    return True


def setup_project():
    # check vars are ok
    assert os.environ['S3_BUCKET'], "You need to set the S3_BUCKET environment variable"
    # first download the data
    taxi_dataset_path = download_taxi_data()
    # create the bucket if it does not exist
    s3_client = boto3.client(
         's3',
         aws_access_key_id=os.environ['S3_USER'],
         aws_secret_access_key=os.environ['S3_ACCESS']
         )
    # is_created = create_bucket_if_not_exists(s3_client, os.environ['S3_BUCKET'])
    # upload the data to the bucket
    upload_datasets(
        s3_client,
        os.environ['S3_BUCKET'],
        taxi_dataset_path
        )
    # all done
    print("All done! See you, duck cowboy!")
    return     


if __name__ == "__main__":
    setup_project()