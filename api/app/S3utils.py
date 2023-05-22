import boto3
from io import StringIO
import pandas as pd


def get_data(bucket_name, file_path):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    return pd.read_csv(obj["Body"])


def post_data(bucket_name, file_path, data):
    s3 = boto3.resource("s3")
    buffer = StringIO()
    data.to_csv(buffer, index=False)
    s3.Object(bucket_name, file_path).put(Body=buffer.getvalue())
    print("Upload Success")
