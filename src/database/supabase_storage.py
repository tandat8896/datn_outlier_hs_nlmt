"""
Ket noi Supabase Storage (S3-compatible) va doc CSV truc tiep vao pandas.
Khong can download file ve may — stream thang qua io.BytesIO.

Lay S3 credentials tai:
  Supabase Dashboard -> Storage -> S3 Access Keys -> New access key
"""
import io
import os
import boto3
import pandas as pd
from pathlib import Path
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

S3 = dict(
    endpoint_url=f"https://{os.getenv('SUPABASE_PROJECT_ID')}.supabase.co/storage/v1/s3",
    aws_access_key_id=os.getenv("SUPABASE_S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("SUPABASE_S3_SECRET_KEY"),
    region_name="ap-southeast-1",
    config=Config(signature_version="s3v4"),
)
BUCKET = os.getenv("SUPABASE_BUCKET", "raw-data")


def get_client():
    return boto3.client("s3", **S3)


def list_files(prefix=""):
    s3 = get_client()
    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [obj["Key"] for obj in res.get("Contents", [])]


def read_csv(key: str) -> pd.DataFrame:
    """Doc CSV truc tiep tu Supabase Storage, khong download ve may."""
    s3 = get_client()
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def upload_file(local_path: str, key: str):
    s3 = get_client()
    s3.upload_file(local_path, BUCKET, key)
    print(f"Uploaded: {key}")


if __name__ == "__main__":
    print("Kiem tra ket noi Supabase Storage...")
    files = list_files()
    print(f"Files trong bucket '{BUCKET}': {files}")

    if files:
        print(f"\nDoc thu file: {files[0]}")
        df = read_csv(files[0])
        print(f"Shape: {df.shape}")
        print(df.head(3))
