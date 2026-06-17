"""
Upload raw payment JSON files and Google Ads CSV to MinIO bronze layer
Simulates raw data landing zone
"""
import boto3
from pathlib import Path
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────
MINIO_ENDPOINT  = "http://192.168.0.104:9000"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin"
BUCKET          = "bronze"

DATA_DIR        = Path("generator/output")
PAY_DIR         = DATA_DIR / "payments"
ADS_DIR         = DATA_DIR / "ads"

# ── S3 client pointing at MinIO ───────────────────────────────────
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
)

def upload_folder(local_dir: Path, file_prefix: str, s3_prefix: str):
    files = list(local_dir.glob(f"{file_prefix}*"))
    print(f"\n→ Uploading {len(files)} files to s3://{BUCKET}/{s3_prefix}/")
    for f in tqdm(files, desc=f"  {s3_prefix}"):
        s3_key = f"{s3_prefix}/{f.name}"
        s3.upload_file(str(f), BUCKET, s3_key)
    print(f"  ✓ {len(files)} files uploaded")

# Upload payments — filtered by actual filename prefix this time
upload_folder(PAY_DIR, "payme_", "payments/payme")
upload_folder(PAY_DIR, "click_", "payments/click")

# Upload Google Ads CSV
print(f"\n→ Uploading Google Ads CSV...")
s3.upload_file(
    str(ADS_DIR / "google_ads_performance.csv"),
    BUCKET,
    "ads/google_ads_performance.csv"
)
print(f"  ✓ google_ads_performance.csv uploaded")

print("\n✓ Bronze layer populated")