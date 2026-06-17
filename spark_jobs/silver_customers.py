"""
Silver layer transformation: customers
Bronze (raw, dirty) -> Silver (city names standardized via broadcast join)
"""
from pyspark.sql.functions import current_timestamp, broadcast
from utils.spark_session import get_spark_session

spark = get_spark_session("silver_customers")

# ── Read Bronze ──────────────────────────────────────────────────
print("→ Reading raw customers from bronze layer...")
df = spark.read.json("s3a://bronze/woocommerce/customers/customers.jsonl")
print(f"  ✓ {df.count():,} records read")
print("→ Deduplicating...")
df = df.dropDuplicates()
print(f"  After dedup: {df.count():,}")

# ── Build city lookup table (variant spelling -> canonical name) ──
city_variants = {
    "Tashkent":  ["Tashkent", "Tошкент", "Toshkent"],
    "Samarkand": ["Samarkand", "Самарканд", "Samarqand"],
    "Namangan":  ["Namangan", "Наманган"],
    "Fergana":   ["Fergana", "Фергана", "Farg'ona"],
    "Bukhara":   ["Bukhara", "Бухара", "Buxoro"],
    "Andijan":   ["Andijan", "Андижан", "Andijon"],
}

city_lookup = []
for canonical_name, variants in city_variants.items():
    for variant in variants:
        city_lookup.append((variant, canonical_name))

city_lookup_df = spark.createDataFrame(city_lookup, ["city_variant", "city_canonical"])

# ── Standardize city names via broadcast join ──────────────────
print("→ Standardizing city names (broadcast join)...")
df = df.join(
    broadcast(city_lookup_df),
    df["city"] == city_lookup_df["city_variant"],
    "left"
)

unmatched = df.filter(df["city_canonical"].isNull()).count()
print(f"  Unmatched city values: {unmatched:,}")

# ── Ingestion metadata ───────────────────────────────────────────
df = df.withColumn("silver_ingested_at", current_timestamp())

# ── Write to Silver (Delta format) ──────────────────────────────
print("→ Writing cleaned customers to silver layer (Delta format)...")
df.write.format("delta").mode("overwrite").save("s3a://silver/customers/")

print("\n✓ Silver customers transformation complete")
print(f"  Final row count: {df.count():,}")

spark.stop()