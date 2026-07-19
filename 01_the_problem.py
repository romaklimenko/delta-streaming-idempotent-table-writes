"""Step 1, the problem: a failed micro-batch leaves partial writes behind.

The pipeline streams zip files from a landing folder. Each zip contains
several JSON files, and every JSON file is appended to a Delta table with
its own write. When one JSON file is malformed, the batch fails after some
writes already made it into the table. The checkpoint does not advance, so
a restart re-processes the whole zip and duplicates the rows that did land.
"""

import io
import json
import zipfile
from pathlib import Path

from pyspark.sql import functions as F

import helpers
from helpers import ORDER_SCHEMA, TABLE

spark = helpers.get_spark("01-the-problem")


def process_zips(batch_df, batch_id):
    print(f"-- micro-batch {batch_id}")
    for file in sorted(batch_df.select("path", "content").collect()):
        zip_name = Path(file.path).name
        with zipfile.ZipFile(io.BytesIO(file.content)) as zf:
            for member in sorted(zf.namelist()):
                records = json.loads(zf.read(member))  # raises on malformed JSON
                df = (
                    spark.createDataFrame(records, ORDER_SCHEMA)
                    .withColumn("source", F.lit(f"{zip_name}/{member}"))
                )
                df.write.format("delta").mode("append").save(TABLE)
                print(f"   appended {len(records)} rows from {zip_name}/{member}")


helpers.reset()

# A well-formed zip arrives and is processed without trouble.
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

# The next zip has a valid first file and a broken second one.
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

# Upstream re-delivers a fixed batch-0002.zip and we restart the stream.
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
helpers.show_table(spark, "after the restart: orders-003.json rows are duplicated")
helpers.show_duplicates(spark)

spark.stop()
