"""Step 3, the helper: the same fix behind a small reusable wrapper.

The pipeline code stays about the writing, IdempotentWriter carries the
txnAppId/txnVersion mechanics. Runs the same three acts as before and ends
with no duplicates.
"""

import io
import json
import zipfile
from pathlib import Path

from pyspark.sql import functions as F

import helpers
from helpers import ORDER_SCHEMA, TABLE
from idempotent import IdempotentWriter

spark = helpers.get_spark("03-the-helper")
writer = IdempotentWriter("zip-ingest")


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
                writer.append(df, TABLE, batch_id, task=f"{zip_name}/{member}")
                print(f"   processed {zip_name}/{member}")


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
helpers.show_table(spark, "after the restart: still no duplicates")
helpers.show_duplicates(spark)

spark.stop()
