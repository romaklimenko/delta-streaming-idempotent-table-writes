"""A small wrapper for retry-safe Delta appends inside foreachBatch."""


class IdempotentWriter:
    """Gives every logical write in a foreachBatch a stable identity.

    Delta records a (txnAppId, txnVersion) pair with each commit and skips
    writes it has already seen. The txnAppId is derived from the writer's
    app id and a task key, the micro-batch id becomes the txnVersion.

    The task key must be stable across retries of the same batch: derive it
    from the data (file name, target table), never from a counter or a
    timestamp.
    """

    def __init__(self, app_id: str):
        self.app_id = app_id

    def append(self, df, table_path: str, batch_id: int, task: str):
        (
            df.write.format("delta")
            .mode("append")
            .option("txnAppId", f"{self.app_id}/{task}")
            .option("txnVersion", batch_id)
            .save(table_path)
        )
