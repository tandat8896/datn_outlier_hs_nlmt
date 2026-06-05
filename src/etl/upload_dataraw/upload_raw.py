import boto3
import os
from botocore.client import Config

# Config — đổi 3 dòng này khi lên Supabase
ENDPOINT  = "http://localhost:9000"
KEY       = "minioadmin"
SECRET    = "minioadmin"
BUCKET    = "raw-data"
from pathlib import Path
DATA_DIR  = str(Path(__file__).resolve().parents[3] / "data" / "raw")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    config=Config(signature_version="s3v4"),
)

# Tạo bucket nếu chưa có
existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if BUCKET not in existing:
    s3.create_bucket(Bucket=BUCKET)
    print(f"[OK] Created bucket: {BUCKET}")
else:
    print(f"[OK] Bucket already exists: {BUCKET}")

# Lấy danh sách file đang có trên MinIO
existing_files = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}

# Upload từng file CSV
for fname in sorted(os.listdir(DATA_DIR)):
    if not fname.endswith(".csv"):
        continue
    path = os.path.join(DATA_DIR, fname)
    size = os.path.getsize(path) / (1024 * 1024)

    if fname in existing_files:
        s3.delete_object(Bucket=BUCKET, Key=fname)
        print(f"  Deleted existing: {fname}")

    print(f"  Uploading {fname} ({size:.1f} MB)...", end=" ", flush=True)
    s3.upload_file(path, BUCKET, fname)
    print("done")

print("\n[DONE] All files uploaded to MinIO")
