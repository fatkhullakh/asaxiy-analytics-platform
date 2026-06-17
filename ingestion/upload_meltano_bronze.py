"""
Upload Meltano-extracted JSONL files to MinIO bronze layer
Source: PostgreSQL (WooCommerce simulation) via tap-postgres
"""

import boto3
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────
MINIO_ENDPOINT  = "http://192.168.0.104:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET  = "minioadmin"
BUCKET          = "bronze"

MELTANO_OUTPUT = Path("meltano/output")

# Map local filename -> bronze destionation prefix
FILES_TO_UPLOAD = {
    "public-customers.jsonl":   "woocommerce/customers/customers.jsonl",
    "public-products.jsonl":    "woocommerce/products/products.jsonl",
    "public-orders.jsonl":      "woocommerce/orders/orders.jsonl",
    "public-order_items.jsonl": "woocommerce/order_items/order_items.jsonl",
}

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
)

for local_name, s3_key in FILES_TO_UPLOAD.items():
    local_path = MELTANO_OUTPUT / local_name
    size_mb = local_path.stat().st_size / 1024 / 1024
    print(f"→ Uploading {local_name} ({size_mb:.1f} MB) → s3://{BUCKET}/{s3_key}")
    s3.upload_file(str(local_path), BUCKET, s3_key)
    print(f"  ✓ done")

print("\n✓ All Meltano bronze files uploaded")