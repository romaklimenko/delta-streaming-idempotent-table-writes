"""Shared plumbing for the demos: Spark session, paths, zip and stream helpers."""

import io
import json
import re
import shutil
import zipfile
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.errors import StreamingQueryException
from pyspark.sql import SparkSession

DATA = Path(__file__).parent / "data"
LANDING = DATA / "landing"
TABLE = str(DATA / "orders")
CHECKPOINT = str(DATA / "checkpoint")

ORDER_SCHEMA = "order_id STRING, customer STRING, amount DOUBLE"


def get_spark(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    # FATAL, because the demos fail streams on purpose and we don't want
    # Spark's ERROR stack traces to drown the demo output.
    spark.sparkContext.setLogLevel("FATAL")
    return spark


def reset():
    """Start every demo from a clean slate: no landing zips, no table, no checkpoint."""
    shutil.rmtree(DATA, ignore_errors=True)
    LANDING.mkdir(parents=True)


def drop_zip(name: str, members: dict):
    """Write a zip of JSON files into the landing folder.

    Member values are lists of records. Pass a plain string to plant malformed JSON.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for member, records in members.items():
            payload = records if isinstance(records, str) else json.dumps(records)
            zf.writestr(member, payload)
    (LANDING / name).write_bytes(buffer.getvalue())
    print(f"\nlanded {name} containing {list(members)}")


def run_stream(spark: SparkSession, process_batch):
    """One 'run' of the pipeline: start the query, drain available input, stop.

    Reuses the same checkpoint every time, so each call behaves like a restart
    of a long-running stream.
    """
    query = (
        spark.readStream.format("binaryFile")
        .schema("path STRING, modificationTime TIMESTAMP, length LONG, content BINARY")
        .option("pathGlobFilter", "*.zip")
        .load(str(LANDING))
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
        print("stream run completed")
    except StreamingQueryException as e:
        reason = next(
            (
                line.strip()
                for line in reversed(str(e).splitlines())
                if re.match(r"\w[\w.]*Error:", line.strip())
            ),
            str(e).splitlines()[0],
        )
        print(f"stream run FAILED, checkpoint not advanced ({reason})")


def latest_table_version(table_path: str) -> int:
    """Highest committed version in the table's _delta_log, -1 if no table yet."""
    log = Path(table_path, "_delta_log")
    versions = [int(f.stem) for f in log.glob("*.json")] if log.exists() else []
    return max(versions, default=-1)


def show_txn_actions(table_path: str):
    """Print every SetTransaction action recorded in the table's _delta_log."""
    print("   SetTransaction actions in _delta_log:")
    for commit in sorted(Path(table_path, "_delta_log").glob("*.json")):
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "txn" in action:
                print(f"   {commit.name}: {json.dumps(action['txn'])}")


def show_table(spark: SparkSession, title: str):
    print(f"\n=== {title}")
    df = spark.read.format("delta").load(TABLE).orderBy("source", "order_id")
    df.show(truncate=False)


def show_duplicates(spark: SparkSession):
    print("\n=== duplicates in the table")
    df = spark.read.format("delta").load(TABLE)
    df.groupBy(df.columns).count().where("count > 1").show(truncate=False)
