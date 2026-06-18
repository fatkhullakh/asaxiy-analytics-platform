select
    product_id,
    name,
    category,
    price_listed,
    price_uzs_clean,
    is_usd_error_detected,
    stock,
    is_active,
    silver_ingested_at
from delta.`s3a://silver/products/`