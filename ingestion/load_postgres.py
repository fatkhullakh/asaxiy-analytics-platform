"""
Load generated Parquet files into PostgreSQL (simulates WooCommerce source DB)
"""
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from pathlib import Path
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────
PG_CONN = "postgresql+psycopg2://asaxiy:asaxiy123@192.168.0.104:5432/asaxiy_source"
DATA_DIR = Path("generator/output/postgres")

engine = create_engine(PG_CONN)

tables = {
    "customers":   DATA_DIR / "customers.parquet",
    "products":    DATA_DIR / "products.parquet",
    "orders":      DATA_DIR / "orders.parquet",
    "order_items": DATA_DIR / "order_items.parquet",
}

for table_name, parquet_path in tables.items():
    print(f"\n→ Loading {table_name}...")
    df = pd.read_parquet(parquet_path)
    df.to_sql(
        table_name,
        engine,
        if_exists="replace",
        index=False,
        chunksize=10_000,
        method="multi"
    )
    print(f"  ✓ {len(df):,} rows loaded into {table_name}")

print("\n✓ All tables loaded into PostgreSQL")