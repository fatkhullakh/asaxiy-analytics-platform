select
    customer_id,
    email,
    city,
    city_canonical,
    registered_at,
    is_guest,
    silver_ingested_at
from delta.`s3a://silver/customers/`