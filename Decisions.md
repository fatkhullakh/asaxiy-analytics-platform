# Engineering Decisions

Quick notes on why things are built this way, including the stuff that broke.

## Meltano instead of Fivetran/Airbyte
Fivetran's trial expires, Airbyte was too heavy for a 16GB laptop already running Spark + Airflow + MinIO. Meltano's lighter and `tap-postgres` is a real maintained connector.

Original plan was `target-s3`, but it's broken — depends on `pyarrow==10.0.1` which fails to build on Python 3.12/3.13. Swapped to `target-jsonl` + a small boto3 script to push files into MinIO. Meltano still does the actual extraction, which is the part that matters.

Payme/Click/Google Ads didn't go through Meltano — those are webhook/file drops in real life, not database tables, so there's nothing for a tap to extract.

## Why the Meltano run took an hour
Singer streams one record at a time with full validation per row. Same data as Parquet was 24MB; as JSONL it was 432MB. This is a known Singer tradeoff (built for incremental syncs, not bulk historical loads) — not something I fixed, since it only had to run once.

## Bronze had 2x rows (orders, customers, products)
`target-jsonl` appends instead of overwriting. My first Meltano run crashed partway (syncing Airflow's own tables before I scoped it down), and the retry appended on top instead of starting clean. Fixed by adding `dropDuplicates()` in every Silver job — safe here since the duplicates are byte-for-byte identical.

## Order duplicates use plain dropDuplicates(), no tiebreaker
Orders also have an intentional 2% duplicate rate simulating a webhook retry. The duplicate is an exact copy with no separate timestamp/sequence field, so there's nothing to break a tie on — a real tiebreaker would need a field that doesn't exist here. Plain dedup is the correct call, not a shortcut.

## USD price bug — detected without a flag column
The generator originally tagged bad rows with `is_usd_error`, but I dropped that column earlier when trimming what got loaded into Postgres. Instead of backfilling it, I built a per-category price floor (e.g. nothing in Elektronika legitimately costs under 50,000 UZS) — anything below the floor is almost certainly the real price divided by ~12,500 by mistake. Caught 2,044 of 100,000 rows, basically exactly the 2% the generator injects, so it's working.

## Broadcast join for city names
~18 rows of variant spellings (Tashkent/Тошкент/Toshkent etc) joined against 500k customers. Broadcasting the tiny lookup table avoids shuffling the big customer table across the cluster just to do a lookup.

## What Silver doesn't fix
Pending payments and orphaned order_ids in Click payments are left alone in Silver — they're real business states, not broken data. That's a job for dbt tests and `mart_payment_health`, not for Silver to quietly clean away.