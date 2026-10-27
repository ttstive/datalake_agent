# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver orders (parse, data quality, dedup, current order state)
# MAGIC
# MAGIC Streams new rows from `<catalog>.<schema>.bronze_orders` (`trigger(availableNow=True)`, serverless-safe) and,
# MAGIC in one `foreachBatch`, writes three tables:
# MAGIC
# MAGIC | table | grain | write |
# MAGIC |---|---|---|
# MAGIC | `silver_orders` | one row per valid event (`event_id`) | insert-only MERGE on `event_id` |
# MAGIC | `silver_orders_quarantine` | one row per rejected Kafka record (topic, partition, offset) | insert-only MERGE |
# MAGIC | `silver_order_status` | one row per order (`order_id`), current state | upsert MERGE |
# MAGIC
# MAGIC * **Explicit schema**: `from_json` with schema v1 (ADR-001); no inference. Parsing/DQ SQL lives in `orders_silver.py`.
# MAGIC * **Data quality**: first failing rule → `dq_reason`; failed rows go to quarantine with the raw `value`, never to `silver_orders`.
# MAGIC * **Dedup / idempotency**: MERGE on `event_id` against the whole table (not a watermark window), so duplicates are
# MAGIC   dropped no matter how late they arrive, and a replayed batch or a reset checkpoint never inserts twice.
# MAGIC   Late events are accepted; `silver_order_status` orders events by `event_time`, not arrival.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")  # noqa: F821
dbutils.widgets.text("schema", "ecommerce")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip()  # noqa: F821
schema = dbutils.widgets.get("schema").strip()  # noqa: F821

bronze_table = f"{catalog}.{schema}.bronze_orders"
silver_table = f"{catalog}.{schema}.silver_orders"
quarantine_table = f"{catalog}.{schema}.silver_orders_quarantine"
status_table = f"{catalog}.{schema}.silver_order_status"
checkpoint = f"/Volumes/{catalog}/{schema}/landing/_checkpoints/silver_orders"

print(
    f"source={bronze_table}\ncheckpoint={checkpoint}\ntargets={silver_table}, {quarantine_table}, {status_table}"
)

# COMMAND ----------

import os
import sys

# orders_silver.py sits next to this notebook (workspace file synced by the bundle).
sys.path.insert(0, os.getcwd())
import orders_silver as rules

# Render all SQL on the driver as plain strings: the foreachBatch function below only closes over
# strings, so it pickles cleanly on serverless (Spark Connect) without needing this module on the worker.
PARSED_VIEW = "_silver_parsed_batch"
BATCH_VIEW = "_bronze_batch"
parse_sql = rules.parsed_select_sql(BATCH_VIEW)
quarantine_merge = rules.quarantine_merge_sql(quarantine_table, PARSED_VIEW)
silver_merge = rules.silver_orders_merge_sql(silver_table, PARSED_VIEW)
status_merge = rules.order_status_merge_sql(
    status_table, f"({rules.valid_events_sql(PARSED_VIEW)})"
)

print("DQ rules (first failing rule wins):")
for reason, cond in rules.DQ_RULES:
    print(f"  {reason:<28} fails when: {cond}")

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {silver_table} (
      event_id          STRING        NOT NULL COMMENT 'Unique event id (uuid). Dedup key: each event_id appears once',
      order_id          STRING        NOT NULL COMMENT 'Order id (uuid); one order emits several status events',
      customer_id       STRING        COMMENT 'Synthetic customer pseudo-identifier (e.g. cust_1234). Fake data, but treat as a pseudonymous identifier: do not join to real people',
      product           STRING        COMMENT 'Product name (synthetic catalog)',
      category          STRING        COMMENT 'Product category',
      quantity          INT           COMMENT 'Units ordered (1-5)',
      unit_price        DECIMAL(10,2) COMMENT 'Unit price',
      line_total        DECIMAL(12,2) COMMENT 'quantity * unit_price (order value carried by this event)',
      status            STRING        COMMENT 'Order status of this event: created | paid | shipped | delivered | cancelled',
      event_time        TIMESTAMP     COMMENT 'When the event happened at the source (UTC)',
      schema_version    INT           COMMENT 'Event schema version (1)',
      kafka_partition   INT           COMMENT 'Kafka partition of the (first) record carrying this event',
      kafka_offset      BIGINT        COMMENT 'Kafka offset of the (first) record carrying this event',
      kafka_timestamp   TIMESTAMP     COMMENT 'Kafka record timestamp (UTC)',
      batch_id          BIGINT        COMMENT 'Local spark-ingestor micro-batch id that landed the record',
      ingested_at       TIMESTAMP     COMMENT 'When the local spark-ingestor processed the record (UTC)',
      _bronze_loaded_at TIMESTAMP     COMMENT 'When the record was loaded into bronze_orders (UTC)',
      _silver_loaded_at TIMESTAMP     COMMENT 'When the event was inserted into silver_orders (UTC)'
    )
    COMMENT 'Silver: parsed, typed and deduplicated order events (synthetic e-commerce data, schema v1). One row per event_id; rows failing data-quality rules are in silver_orders_quarantine instead.'
    """
)

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {quarantine_table} (
      dq_reason         STRING    COMMENT 'First failed data-quality rule: invalid_json | type_mismatch | missing_event_id | missing_order_id | invalid_status | invalid_quantity | invalid_unit_price | missing_event_time | unsupported_schema_version',
      value             STRING    COMMENT 'Raw Kafka message value exactly as in bronze',
      kafka_key         STRING    COMMENT 'Kafka message key',
      topic             STRING    COMMENT 'Kafka topic',
      kafka_partition   INT       COMMENT 'Kafka partition',
      kafka_offset      BIGINT    COMMENT 'Kafka offset (topic + partition + offset identify the record)',
      kafka_timestamp   TIMESTAMP COMMENT 'Kafka record timestamp (UTC)',
      batch_id          BIGINT    COMMENT 'Local spark-ingestor micro-batch id',
      ingested_at       TIMESTAMP COMMENT 'When the local spark-ingestor processed the record (UTC)',
      event_id          STRING    COMMENT 'event_id if it could be parsed, else NULL',
      order_id          STRING    COMMENT 'order_id if it could be parsed, else NULL',
      _bronze_loaded_at TIMESTAMP COMMENT 'When the record was loaded into bronze_orders (UTC)',
      _quarantined_at   TIMESTAMP COMMENT 'When the record was written to quarantine (UTC)'
    )
    COMMENT 'Silver quarantine: bronze records that failed data-quality rules (one row per Kafka record), with the reason and the raw value. These rows are NOT in silver_orders.'
    """
)

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {status_table} (
      order_id        STRING        NOT NULL COMMENT 'Order id (uuid). One row per order',
      customer_id     STRING        COMMENT 'Synthetic customer pseudo-identifier (fake data; treat as pseudonymous, not linkable to real people)',
      product         STRING        COMMENT 'Product name',
      category        STRING        COMMENT 'Product category',
      quantity        INT           COMMENT 'Units ordered (1-5)',
      unit_price      DECIMAL(10,2) COMMENT 'Unit price',
      order_total     DECIMAL(12,2) COMMENT 'quantity * unit_price',
      current_status  STRING        COMMENT 'Status of the latest event (by event_time, ties by created<paid<shipped<delivered<cancelled); cancelled is terminal',
      created_at      TIMESTAMP     COMMENT 'event_time of the created event (NULL if not seen yet)',
      paid_at         TIMESTAMP     COMMENT 'event_time of the paid event (NULL if not seen)',
      shipped_at      TIMESTAMP     COMMENT 'event_time of the shipped event (NULL if not seen)',
      delivered_at    TIMESTAMP     COMMENT 'event_time of the delivered event (NULL if not seen)',
      cancelled_at    TIMESTAMP     COMMENT 'event_time of the cancelled event (NULL if not cancelled)',
      last_event_time TIMESTAMP     COMMENT 'Latest event_time seen for this order (UTC)',
      updated_at      TIMESTAMP     COMMENT 'When this row was last merged (UTC)'
    )
    COMMENT 'Silver: current state of each order (one row per order_id), built by MERGE from valid silver events. Status timestamps keep the earliest time each status was seen.'
    """
)

# COMMAND ----------


def process_batch(batch_df, batch_id):
    """Parse + DQ-split + dedup one bronze micro-batch. Every write is an idempotent MERGE, so a replayed
    batch (retry after failure) produces the same result."""
    session = (
        batch_df.sparkSession
    )  # the batch's own session (required on Spark Connect / serverless)
    batch_df.createOrReplaceTempView(BATCH_VIEW)
    session.sql(parse_sql).createOrReplaceTempView(PARSED_VIEW)
    session.sql(quarantine_merge)
    session.sql(silver_merge)
    session.sql(status_merge)


query = (
    spark.readStream.table(bronze_table)  # noqa: F821
    .writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

# COMMAND ----------

rows_this_run = sum(
    (p["numInputRows"] or 0) for p in query.recentProgress
)  # can be None on serverless
print(f"bronze rows read this run: {rows_this_run}")
for t in (silver_table, quarantine_table, status_table):
    print(f"{t}: {spark.table(t).count()} rows")  # noqa: F821
