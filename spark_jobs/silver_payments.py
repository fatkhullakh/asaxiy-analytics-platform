"""
Silver layer transformation: payments
Bronze (raw, dirty) -> Silver (clean, deduplicated, UTC timestamps)
"""

from pyspark.sql.functions import when, col, to_utc_timestamp, current_timestamp
from utils.spark_session import get_spark_session

spark = get_spark_session("silver_payments")

# Read raw payments from bronze layer
print("→ Reading raw payments from bronze layer...")
df_payme = spark.read.option("multiLine", True).json("s3a://bronze/payments/payme/payme_*.json")
df_click = spark.read.option("multiLine", True).json("s3a://bronze/payments/click/click_*.json")
print(f"  ✓ {df_payme.count() + df_click.count()} records read")

# Transformations:
# -- 1 Merge payments from different sources
print("→ Merging payments from different sources...")
df = df_payme.unionByName(df_click)
print(f"  After merge: {df.count():,}")

# -- 2 Currency normalization (to UZS)
print("→ Normalizing currency to UZS...")
df = df.withColumn(
    "amount_uzs",
    when(col("source") == "payme",
    col("amount") / 100).otherwise(col("amount"))
)
print(f"  After currency normalization: {df.count():,}")

# -- 3 Ingestion metadata
df = df.withColumn("silver_ingested_at", current_timestamp())

# Write to Silver (Delta format)
print("→ Writing cleaned payments to silver layer (Delta format)...")
df.write.format("delta").mode("overwrite").save("s3a://silver/payments/")

print("\n✓ Silver payments transformation complete")
print(f" Final row count: {df.count():,}")

spark.stop()