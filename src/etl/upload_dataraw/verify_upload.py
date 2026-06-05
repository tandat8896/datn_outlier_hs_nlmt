import boto3
import os
import io
import hashlib
import csv
from botocore.client import Config

ENDPOINT = "http://localhost:9000"
KEY      = "minioadmin"
SECRET   = "minioadmin"
BUCKET   = "raw-data"
from pathlib import Path
DATA_DIR = str(Path(__file__).resolve().parents[3] / "data" / "raw")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
    config=Config(signature_version="s3v4"),
)

PASS = "[PASS]"
FAIL = "[FAIL]"

def md5(content: bytes) -> str:
    return hashlib.md5(content).hexdigest()

print(f"\n{'='*60}")
print("  VERIFY UPLOAD INTEGRITY: Local vs MinIO")
print(f"{'='*60}\n")

issues = []
objects = {o["Key"]: o["Size"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}

for fname in sorted(os.listdir(DATA_DIR)):
    if not fname.endswith(".csv"):
        continue

    local_path = os.path.join(DATA_DIR, fname)
    print(f"  {fname}")

    # 1. Tồn tại trên MinIO không
    if fname not in objects:
        print(f"    {FAIL} File không tồn tại trên MinIO")
        issues.append(f"{fname}: missing on MinIO")
        continue

    # 2. MD5 checksum — phát hiện bất kỳ thay đổi nào kể cả 1 byte
    with open(local_path, "rb") as f:
        local_bytes = f.read()
    minio_bytes = s3.get_object(Bucket=BUCKET, Key=fname)["Body"].read()

    local_md5 = md5(local_bytes)
    minio_md5 = md5(minio_bytes)
    checksum_ok = local_md5 == minio_md5
    print(f"    {PASS if checksum_ok else FAIL} MD5 checksum: {'match' if checksum_ok else f'MISMATCH  local={local_md5}  minio={minio_md5}'}")
    if not checksum_ok:
        issues.append(f"{fname}: MD5 mismatch — file bị corrupt")

    # Nếu checksum đã fail thì check thêm chi tiết
    local_text = local_bytes.decode("utf-8-sig")
    minio_text = minio_bytes.decode("utf-8-sig")
    local_lines = local_text.splitlines()
    minio_lines = minio_text.splitlines()

    # 3. Row count
    local_rows = len(local_lines) - 1
    minio_rows = len(minio_lines) - 1
    rows_ok = local_rows == minio_rows
    print(f"    {PASS if rows_ok else FAIL} Rows: local={local_rows:,} | minio={minio_rows:,}")
    if not rows_ok:
        issues.append(f"{fname}: row count mismatch")

    # 4. Columns — phát hiện mất cột
    local_cols = local_lines[0].strip().split(",")
    minio_cols = minio_lines[0].strip().split(",")
    cols_ok = local_cols == minio_cols
    print(f"    {PASS if cols_ok else FAIL} Columns ({len(local_cols)}): {'match' if cols_ok else f'MISMATCH'}")
    if not cols_ok:
        missing = set(local_cols) - set(minio_cols)
        extra   = set(minio_cols) - set(local_cols)
        if missing: print(f"          Missing cols: {missing}")
        if extra:   print(f"          Extra cols:   {extra}")
        issues.append(f"{fname}: column mismatch")

    # 5. Sample data — so sánh 3 dòng đầu và 3 dòng cuối
    for label, local_row, minio_row in [
        ("Row 1", local_lines[1] if len(local_lines) > 1 else "", minio_lines[1] if len(minio_lines) > 1 else ""),
        ("Row 2", local_lines[2] if len(local_lines) > 2 else "", minio_lines[2] if len(minio_lines) > 2 else ""),
        ("Last ", local_lines[-1], minio_lines[-1]),
    ]:
        ok = local_row == minio_row
        print(f"    {PASS if ok else FAIL} {label}: {'match' if ok else 'MISMATCH'}")
        if not ok:
            issues.append(f"{fname}: data mismatch at {label}")

    print()

print(f"{'='*60}")
if not issues:
    print(f"  {PASS} Tất cả files toàn vẹn — sẵn sàng ETL!\n")
else:
    print(f"  {FAIL} Có {len(issues)} vấn đề:")
    for i in issues:
        print(f"    - {i}")
    print()
