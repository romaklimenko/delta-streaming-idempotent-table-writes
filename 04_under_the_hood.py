"""Step 4, under the hood: SetTransaction actions in the Delta log.

No streaming here. Plain batch writes with and without txnAppId/txnVersion,
and a look at what lands in _delta_log after each one.
"""

import helpers

spark = helpers.get_spark("04-under-the-hood")

TABLE = str(helpers.DATA / "orders_batch")


def orders(*rows):
    return spark.createDataFrame(list(rows), helpers.ORDER_SCHEMA)


def append(df, txn_version=None):
    writer = df.write.format("delta").mode("append")
    if txn_version is not None:
        writer = writer.option("txnAppId", "batch-demo").option(
            "txnVersion", txn_version
        )
    writer.save(TABLE)
    count = spark.read.format("delta").load(TABLE).count()
    table_version = helpers.latest_table_version(TABLE)
    print(
        f"   append with txnVersion={txn_version}: "
        f"table is now at version {table_version} with {count} rows"
    )


helpers.reset()

print("\n== 1. a write with txnAppId/txnVersion records a SetTransaction action")
append(orders(("o-001", "alice", 120.0)), txn_version=1)
helpers.show_txn_actions(TABLE)

print("\n== 2. the same rows again, without the options: happily duplicated")
append(orders(("o-001", "alice", 120.0)))

print("\n== 3. the same txnVersion again: no new commit, no new rows")
append(orders(("o-001", "alice", 120.0)), txn_version=1)

print("\n== 4. a LOWER txnVersion is also skipped, even for brand new data")
append(orders(("o-999", "zoe", 1.0)), txn_version=0)
print("   o-999 is silently gone: versions must increase monotonically")

print("\n== 5. a higher txnVersion goes through, and the log keeps the latest version")
append(orders(("o-002", "bob", 75.5)), txn_version=2)
helpers.show_txn_actions(TABLE)

print("\n== 6. plain writeStream into Delta does all of this automatically")
COPY = str(helpers.DATA / "orders_copy")
query = (
    spark.readStream.format("delta")
    .load(TABLE)
    .writeStream.format("delta")
    .option("checkpointLocation", str(helpers.DATA / "copy_checkpoint"))
    .trigger(availableNow=True)
    .start(COPY)
)
query.awaitTermination()
print(f"   streaming query id: {query.id}")
helpers.show_txn_actions(COPY)
print("   the Delta sink uses the query id as txnAppId and the batch id as txnVersion")

spark.stop()
