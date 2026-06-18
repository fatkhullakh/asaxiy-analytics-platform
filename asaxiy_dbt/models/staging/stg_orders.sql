select
    order_id,
    customer_id,
    status,
    payment_method,
    ordered_at,
    ordered_at_utc,
    tz_label,
    shipped_at,
    delivered_at,
    total_uzs,
    silver_ingested_at
from delta.`s3a://silver/orders/`