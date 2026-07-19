"""Step 2, the fix: txnAppId + txnVersion make every write idempotent.

Same pipeline as 01_the_problem.py, with two extra lines on the write.
Every JSON file gets its own txnAppId, and the micro-batch id is the
txnVersion. When the failed batch is retried, Delta sees that the write
for orders-003.json at this version already committed and skips it.
"""

import io
import json
import zipfile
from pathlib import Path

from pyspark.sql import functions as F

import helpers
from helpers import ORDER_SCHEMA, TABLE

spark = helpers.get_spark("02-the-fix")


def process_zips(batch_df, batch_id):
    print(f"-- micro-batch {batch_id}")
    for file in sorted(batch_df.select("path", "content").collect()):
        zip_name = Path(file.path).name
        with zipfile.ZipFile(io.BytesIO(file.content)) as zf:
            for member in sorted(zf.namelist()):
                records = json.loads(zf.read(member))
                df = (
                    spark.createDataFrame(records, ORDER_SCHEMA)
                    .withColumn("source", F.lit(f"{zip_name}/{member}"))
                )
                version_before = helpers.latest_table_version(TABLE)
                (
                    df.write.format("delta")
                    .mode("append")
                    .option("txnAppId", f"zip-ingest/{zip_name}/{member}")
                    .option("txnVersion", batch_id)
                    .save(TABLE)
                )
                if helpers.latest_table_version(TABLE) > version_before:
                    print(f"   appended {len(records)} rows from {zip_name}/{member}")
                else:
                    print(f"   SKIPPED {zip_name}/{member}: already committed")


helpers.reset()

helpers.drop_zip(
    "batch-0001.zip",
    {
        "orders-001.json": [
            {"order_id": "o-001", "customer": "alice", "amount": 120.0},
            {"order_id": "o-002", "customer": "bob", "amount": 75.5},
        ],
        "orders-002.json": [
            {"order_id": "o-003", "customer": "carol", "amount": 30.0},
        ],
    },
)
helpers.run_stream(spark, process_zips)
helpers.show_table(spark, "after batch-0001.zip: all good")

helpers.drop_zip(
    "batch-0002.zip",
    {
        "orders-003.json": [
            {"order_id": "o-004", "customer": "dave", "amount": 210.0},
            {"order_id": "o-005", "customer": "erin", "amount": 15.0},
        ],
        "orders-004.json": '[{"order_id": "o-006", "customer": "frank", "amount": ',
    },
)
helpers.run_stream(spark, process_zips)
helpers.show_table(spark, "after the failed run: orders-003.json is already in")

helpers.drop_zip(
    "batch-0002.zip",
    {
        "orders-003.json": [
            {"order_id": "o-004", "customer": "dave", "amount": 210.0},
            {"order_id": "o-005", "customer": "erin", "amount": 15.0},
        ],
        "orders-004.json": [
            {"order_id": "o-006", "customer": "frank", "amount": 99.9},
        ],
    },
)
helpers.run_stream(spark, process_zips)
helpers.show_table(spark, "after the restart: no duplicates this time")
helpers.show_duplicates(spark)

spark.stop()
