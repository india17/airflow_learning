import boto3
from io import BytesIO
from datetime import datetime
import logging
import pandas as pd
import io

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 configuration
s3 = boto3.client("s3")


def create_s3_folder(bucket_name, key):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{key}/")
        if "Contents" not in response:
            logger.info(
                f"Folder '{key}' does not exist in {bucket_name}. Creating folder."
            )
            s3.put_object(Bucket=bucket_name, Key=f"{key}/")
        else:
            logger.info(f"Folder '{key}' already exists in {bucket_name}.")
    except Exception as e:
        logger.error(f"Error checking or creating folder in S3: {e}")
        raise


def upload_data_to_s3(key, table_name, data, current_date):
    try:
        bucket_name, backup_folder = key.split("/")
        create_s3_folder(bucket_name, backup_folder)

        folder_name = table_name
        create_s3_folder(bucket_name, f"{backup_folder}/{folder_name}")

        file_key = f"{backup_folder}/{folder_name}/{table_name}_{current_date.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_buffer = BytesIO()
        data.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

        logger.info(f"Uploaded {file_key} to {bucket_name}.")
    except Exception as e:
        logger.error(f"Error uploading data to S3: {e}")
        raise


def fetch_all_files_from_s3(key, folder_path):
    bucket_name, backup_folder = key.split("/")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{backup_folder}/{folder_path}")

    if "Contents" not in response:
        raise ValueError(
            f"No files found in folder {folder_path} in S3 bucket {bucket_name}"
        )

    files = [content["Key"] for content in response["Contents"][1:]]

    dfs = []
    for file_key in files:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = obj["Body"].read()
        df = pd.read_csv(io.BytesIO(data))
        dfs.append(df)

    if not dfs:
        raise ValueError(
            f"No data found in files from folder {folder_path} in S3 bucket {bucket_name}"
        )

    # Concatenate all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df
