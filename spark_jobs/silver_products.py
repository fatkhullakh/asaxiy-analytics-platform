"""
Silver layer transformation: products
Bronze (raw, dirty) -> Silver (USD price errors detected via heuristic + corrected)
"""
from pyspark.sql.functions import when, col, current_timestamp
from utils.spark_session import get_spark_session

spark = get_spark_session("silver_products")

# ── Read Bronze ──────────────────────────────────────────────────
print("→ Reading raw products from bronze layer...")
df = spark.read.json("s3a://bronze/woocommerce/products/products.jsonl")
print(f"  ✓ {df.count():,} records read")
print("→ Deduplicating...")
df = df.dropDuplicates()
print(f"  After dedup: {df.count():,}")

# ── Detect likely USD price errors via category-aware thresholds ──
# Real prices are always well above these thresholds. Anything below
# is almost certainly a UZS price accidentally divided by ~12,500
# (the USD/UZS rate), based on PRICE_RANGES_UZS used at generation time.
print("→ Detecting USD price errors (heuristic)...")
df = df.withColumn(
    "is_usd_error_detected",
    when((col("category").isin("Elektronika", "Электроника")) & (col("price_listed") < 5000), True)
    .when((col("category").isin("Moda", "Мода", "Fashion")) & (col("price_listed") < 3000), True)
    .when((col("category").isin("Uy jihozlari", "Товары для дома")) & (col("price_listed") < 2000), True)
    .when((col("category").isin("Oziq-ovqat", "Продукты")) & (col("price_listed") < 1000), True)
    .when((col("category").isin("Boshqa", "Другое")) & (col("price_listed") < 1500), True)
    .otherwise(False)
)

# ── Correct the detected USD errors ────────────────────────────────
print("→ Correcting detected USD errors...")
df = df.withColumn(
    "price_uzs_clean",
    when(col("is_usd_error_detected") == True,
         col("price_listed") * 12500
    ).otherwise(col("price_listed"))
)

detected_count = df.filter(col("is_usd_error_detected") == True).count()
print(f"  Flagged {detected_count:,} rows as likely USD errors")

# ── Ingestion metadata ───────────────────────────────────────────
df = df.withColumn("silver_ingested_at", current_timestamp())

# ── Write to Silver (Delta format) ──────────────────────────────
print("→ Writing cleaned products to silver layer (Delta format)...")
df.write.format("delta").mode("overwrite").save("s3a://silver/products/")

print("\n✓ Silver products transformation complete")
print(f"  Final row count: {df.count():,}")

spark.stop()