"""
Silver layer transformation: orders
Bronze (raw, dirty) -> Silver (clean, deduplicated, UTC timestamps)
"""

from pyspark.sql.functions import when, col, to_utc_timestamp, current_timestamp
from utils.spark_session import get_spark_session

spark = get_spark_session("silver_orders")

# Read raw orders from bronze layer
print("→ Reading raw orders from bronze layer...")
df = spark.read.json("s3a://bronze/woocommerce/orders/orders.jsonl")
print(f"  ✓ {df.count()} records read")

# Transformations:
# -- 1 Deduplicate
print("→ Deduplicating orders...")
df = df.dropDuplicates()
print(f"  After dedup: {df.count():,}")

# -- 2 Normalize timestamps to UTC
print("→ Normalizing timestamps to UTC...")
df = df.withColumn(
    "ordered_at_utc",
    when(
        col("tz_label") == "Asia/Tashkent",
        to_utc_timestamp(col("ordered_at"), "Asia/Tashkent")
    ).otherwise(col("ordered_at"))
)

# -- 3 Add ingestion metadata
df = df.withColumn("silver_ingested_at", current_timestamp())

# Write to Silver (Delta format)
print("→ Writing cleaned orders to silver layer (Delta format)...")
df.write.format("delta").mode("overwrite").save("s3a://silver/orders/")

print("\n✓ Silver orders transformation complete")
print(f" Final row count: {df.count():,}")

spark.stop()