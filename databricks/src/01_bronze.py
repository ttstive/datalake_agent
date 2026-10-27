# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze orders (Auto Loader)
# MAGIC
# MAGIC Incrementally loads the Parquet files that the local `spark-ingestor` lands in
# MAGIC `/Volumes/<catalog>/<schema>/landing/orders/` into `<catalog>.<schema>.bronze_orders` (ADR-002).
# MAGIC
# MAGIC * **ELT / raw**: one row per Kafka record. `value` is the raw JSON string — no parsing here (silver does it).
# MAGIC * **Explicit schema** for the landing files; anything unexpected goes to `_rescued_data` (bronze never drops data).
# MAGIC * `trigger(availableNow=True)`: processes all new files then stops — works on serverless, meant for a scheduled job.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")  # noqa: F821
dbutils.widgets.text("schema", "ecommerce")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip()  # noqa: F821
schema = dbutils.widgets.get("schema").strip()  # noqa: F821

landing_path = f"/Volumes/{catalog}/{schema}/landing/orders/"
checkpoint_base = f"/Volumes/{catalog}/{schema}/landing/_checkpoints/bronze_orders"
bronze_table = f"{catalog}.{schema}.bronze_orders"

print(f"source={landing_path}\ncheckpoint={checkpoint_base}\ntarget={bronze_table}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Must match spark/ingest_orders.py LANDING_COLUMNS.
landing_schema = StructType(
    [
        StructField("kafka_key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("batch_id", LongType()),
        StructField("ingested_at", TimestampType()),
    ]
)

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
      kafka_key         STRING    COMMENT 'Kafka message key (order_id), as string',
      value             STRING    COMMENT 'Raw Kafka message value: order event JSON, NOT parsed',
      topic             STRING    COMMENT 'Kafka topic',
      partition         INT       COMMENT 'Kafka partition',
      offset            BIGINT    COMMENT 'Kafka offset within the partition',
      kafka_timestamp   TIMESTAMP COMMENT 'Kafka record timestamp (UTC)',
      batch_id          BIGINT    COMMENT 'Local ingestor Structured Streaming micro-batch id',
      ingested_at       TIMESTAMP COMMENT 'When the local ingestor processed the micro-batch (UTC)',
      _rescued_data     STRING    COMMENT 'Auto Loader rescued data: columns not matching the landing schema',
      _source_file      STRING    COMMENT 'Landing Parquet file this row was loaded from',
      _bronze_loaded_at TIMESTAMP COMMENT 'When Auto Loader loaded this row into bronze (UTC)'
    )
    COMMENT 'Bronze: raw Kafka records from topic orders (synthetic e-commerce events), loaded as-is by Auto Loader from the landing volume. No parsing or dedup; one row per Kafka record (retried landing files are overwritten, not duplicated).'
    """
)

# COMMAND ----------

bronze_stream = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/_schema")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(landing_schema)
    .load(landing_path)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_bronze_loaded_at"),
    )
)

query = (
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_base)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(bronze_table)
)
query.awaitTermination()

# COMMAND ----------

rows_this_run = sum((p["numInputRows"] or 0) for p in query.recentProgress)  # can be None on serverless
total = spark.table(bronze_table).count()  # noqa: F821
print(f"rows loaded this run: {rows_this_run}")
print(f"{bronze_table} total rows: {total}")
