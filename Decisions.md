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

Getting dbt to talk to Delta tables — the Thrift Server saga

dbt doesn't read Parquet/Delta files off S3 by itself — it needs something that speaks SQL in front of the cluster. Spark's Thrift Server is that thing: it's a SQL gateway that sits in front of Spark and lets any SQL client, including dbt, connect like it's a normal database.

Getting it running took a few rounds of debugging, each one a separate bug:


Delta Lake jars were passed in with --packages at startup, but the Thrift Server's entry point loads classes differently than a normal Spark job, so it never actually picked them up. Fix: copy the jar files directly into Spark's own /opt/spark/jars/ folder instead of relying on the package resolver.
start-thriftserver.sh thought a server was already running (it wasn't — it had crashed) and refused to start a new one. Turned out to be a leftover PID file pointing at a dead process. Deleted the file, problem gone.
After fixing the driver, the same error came back from a different machine — turns out the jars only need to go on the master, but the actual query execution happens on the worker containers, which have their own separate filesystem and never got the jars. Had to copy them into both workers too, then restart the workers so they'd pick up the new classpath.


None of this was one bug — it was the same root cause (a jar missing somewhere) showing up in three different places one at a time. Worth remembering for next time: in a multi-container Spark setup, "I added the jar" usually means "I added it to one container," not all of them.