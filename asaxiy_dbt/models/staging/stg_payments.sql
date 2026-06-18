select
    payment_id,
    order_id,
    amount,
    amount_uzs,
    currency,
    status,
    source,
    paid_at,
    silver_ingested_at
from delta.`s3a://silver/payments/`