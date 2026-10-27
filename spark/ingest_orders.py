"""Kafka -> Unity Catalog Volume landing ingestor (ADR-002).

PySpark Structured Streaming app that reads raw records from the local Kafka
topic and, every TRIGGER_INTERVAL, writes each micro-batch as ONE Parquet file
and uploads it to a UC Volume through the Databricks Files API:

    /Volumes/<catalog>/<schema>/<volume>/orders/batch_<batch_id:010d>_<query8>.parquet

ELT: the Kafka value is kept as the raw JSON string. No parsing happens here.

Idempotency: the file name depends only on (batch_id, streaming query id), so a
batch that Spark re-runs after a failure overwrites the same file instead of
creating a duplicate. The query id is stored in the checkpoint, so it is stable
across restarts and only changes if the checkpoint is deleted — which prevents a
reset checkpoint (batch ids restarting at 0) from overwriting older files.

Runs inside the apache/spark image (Python 3.10+), so keep syntax 3.10-compatible.

Env vars: see `Config.from_env`. DATABRICKS_HOST / DATABRICKS_TOKEN are read by
databricks-sdk's WorkspaceClient directly; this script never reads or logs the token.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import signal
import sys
import tempfile
import threading
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("ingest_orders")

LANDING_SUBDIR = "orders"
UPLOAD_ATTEMPTS = 3
UPLOAD_BACKOFF_S = 5.0

# Raw Kafka columns kept in the landing files (ELT: value stays a JSON string).
RAW_SELECT_EXPRS = [
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS value",
    "topic",
    "partition",
    "offset",
    "timestamp AS kafka_timestamp",
]
METADATA_COLUMNS = ["batch_id", "ingested_at"]
LANDING_COLUMNS = [
    "kafka_key",
    "value",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    *METADATA_COLUMNS,
]

_IDENT_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested without Spark / Kafka / Databricks)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Config:
    bootstrap_servers: str
    topic: str
    trigger_interval: str
    checkpoint_dir: str
    catalog: str
    schema: str
    volume: str
    starting_offsets: str
    max_offsets_per_trigger: int

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Config:
        env = os.environ if env is None else env
        catalog = env.get("DATABRICKS_CATALOG", "").strip()
        if not catalog:
            raise ValueError("DATABRICKS_CATALOG is required (e.g. 'workspace')")
        cfg = cls(
            bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            topic=env.get("KAFKA_TOPIC", "orders"),
            trigger_interval=env.get("TRIGGER_INTERVAL", "5 minutes"),
            checkpoint_dir=env.get("CHECKPOINT_DIR", "/checkpoints/orders"),
            catalog=catalog,
            schema=env.get("DATABRICKS_SCHEMA", "ecommerce").strip(),
            volume=env.get("LANDING_VOLUME", "landing").strip(),
            starting_offsets=env.get("STARTING_OFFSETS", "earliest"),
            max_offsets_per_trigger=int(env.get("MAX_OFFSETS_PER_TRIGGER", "100000")),
        )
        for name, value in (("catalog", cfg.catalog), ("schema", cfg.schema), ("volume", cfg.volume)):
            validate_identifier(name, value)
        if cfg.max_offsets_per_trigger <= 0:
            raise ValueError("MAX_OFFSETS_PER_TRIGGER must be > 0")
        return cfg


def validate_identifier(name: str, value: str) -> str:
    """UC names end up in a URL path; allow only plain identifiers."""
    if not value or not _IDENT_RE.match(value):
        raise ValueError(f"invalid {name} {value!r}: use letters, digits, '_' or '-'")
    return value


def landing_dir(catalog: str, schema: str, volume: str) -> str:
    """Volume directory that receives the batch files (Auto Loader source path)."""
    for name, value in (("catalog", catalog), ("schema", schema), ("volume", volume)):
        validate_identifier(name, value)
    return f"/Volumes/{catalog}/{schema}/{volume}/{LANDING_SUBDIR}"


def batch_file_name(batch_id: int, query_id: str) -> str:
    """Deterministic per (batch, query): a retried batch overwrites, never duplicates."""
    if batch_id < 0:
        raise ValueError("batch_id must be >= 0")
    short = query_id.replace("-", "")[:8]
    if not short or not re.fullmatch(r"[0-9a-fA-F]+", short):
        raise ValueError(f"invalid query_id {query_id!r}")
    return f"batch_{batch_id:010d}_{short.lower()}.parquet"


def batch_target_path(catalog: str, schema: str, volume: str, batch_id: int, query_id: str) -> str:
    return f"{landing_dir(catalog, schema, volume)}/{batch_file_name(batch_id, query_id)}"


def find_single_part_file(directory: str | Path) -> Path:
    """Return the only `part-*.parquet` file Spark wrote into `directory`."""
    parts = sorted(Path(directory).glob("part-*.parquet"))
    if len(parts) != 1:
        raise RuntimeError(f"expected exactly 1 parquet part file in {directory}, found {len(parts)}")
    return parts[0]


def summarize_offsets(rows: Iterable[Any]) -> str:
    """Rows with (topic, partition, min_offset, max_offset, rows) -> 'orders[0]=10..20 (11)'."""
    items = []
    for r in rows:
        items.append((r["topic"], int(r["partition"]), int(r["min_offset"]), int(r["max_offset"]), int(r["rows"])))
    items.sort()
    return ", ".join(f"{t}[{p}]={lo}..{hi} ({n})" for t, p, lo, hi, n in items) or "-"


def add_batch_metadata(df: Any, batch_id: int) -> Any:
    """Append ingestion metadata columns and fix the landing column order."""
    from pyspark.sql import functions as F

    return df.withColumns(
        {"batch_id": F.lit(batch_id).cast("long"), "ingested_at": F.current_timestamp()}
    ).select(*LANDING_COLUMNS)


def offset_stats(df: Any) -> Any:
    """Per topic/partition row count and offset range of a batch DataFrame."""
    from pyspark.sql import functions as F

    return df.groupBy("topic", "partition").agg(
        F.min("offset").alias("min_offset"),
        F.max("offset").alias("max_offset"),
        F.count(F.lit(1)).alias("rows"),
    )


# ---------------------------------------------------------------------------
# Databricks I/O
# ---------------------------------------------------------------------------
def make_workspace_client() -> Any:
    from databricks.sdk import WorkspaceClient

    # Reads DATABRICKS_HOST / DATABRICKS_TOKEN from the environment itself.
    return WorkspaceClient()


def verify_landing(w: Any, cfg: Config) -> str:
    """Fail fast if auth or the volume is wrong. Creates `orders/` if missing."""
    target = landing_dir(cfg.catalog, cfg.schema, cfg.volume)
    host = os.environ.get("DATABRICKS_HOST", "<unset>")
    try:
        w.files.create_directory(target)  # idempotent
        w.files.get_directory_metadata(target)
    except Exception as exc:  # noqa: BLE001 - re-raised as a clear startup error
        raise SystemExit(
            f"cannot reach landing volume path {target} on {host}: {type(exc).__name__}: {exc}. "
            f"Check DATABRICKS_HOST/DATABRICKS_TOKEN and that volume "
            f"{cfg.catalog}.{cfg.schema}.{cfg.volume} exists and you have WRITE VOLUME on it."
        ) from None
    return target


def upload_file(w: Any, local_path: Path, target: str) -> None:
    """Upload with a few retries; raise on final failure so Spark fails the batch."""
    last: Exception | None = None
    for attempt in range(1, UPLOAD_ATTEMPTS + 1):
        try:
            with open(local_path, "rb") as fh:
                w.files.upload(target, fh, overwrite=True)
            return
        except Exception as exc:  # noqa: BLE001
            last = exc
            log.warning("upload attempt %d/%d to %s failed: %s", attempt, UPLOAD_ATTEMPTS, target, exc)
            if attempt < UPLOAD_ATTEMPTS:
                time.sleep(UPLOAD_BACKOFF_S * attempt)
    raise RuntimeError(f"upload to {target} failed after {UPLOAD_ATTEMPTS} attempts") from last


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------
class BatchWriter:
    """foreachBatch callback: one Parquet file per micro-batch, uploaded to the Volume."""

    def __init__(self, cfg: Config, w: Any) -> None:
        self.cfg = cfg
        self.w = w
        self.query_id: str | None = None
        self._query_id_ready = threading.Event()

    def set_query_id(self, query_id: str) -> None:
        self.query_id = query_id
        self._query_id_ready.set()

    def __call__(self, df: Any, batch_id: int) -> None:
        start = time.monotonic()
        if not self._query_id_ready.wait(timeout=120):
            raise RuntimeError("streaming query id not available")
        assert self.query_id is not None

        df = add_batch_metadata(df, batch_id).persist()
        tmp = tempfile.mkdtemp(prefix=f"orders_batch_{batch_id}_")
        try:
            stats = offset_stats(df).collect()
            count = sum(int(r["rows"]) for r in stats)
            if count == 0:
                log.info("batch %d: empty, skipped", batch_id)
                return

            out_dir = os.path.join(tmp, "out")
            df.coalesce(1).write.mode("overwrite").parquet(out_dir)
            part = find_single_part_file(out_dir)
            target = batch_target_path(self.cfg.catalog, self.cfg.schema, self.cfg.volume, batch_id, self.query_id)
            upload_file(self.w, part, target)
            log.info(
                "batch %d: rows=%d offsets=[%s] bytes=%d uploaded=%s elapsed=%.1fs",
                batch_id,
                count,
                summarize_offsets(stats),
                part.stat().st_size,
                target,
                time.monotonic() - start,
            )
        finally:
            df.unpersist()
            shutil.rmtree(tmp, ignore_errors=True)


def build_spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("ingest-orders")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def start_query(spark: Any, cfg: Config, writer: BatchWriter) -> Any:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap_servers)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", str(cfg.max_offsets_per_trigger))
        .load()
        .selectExpr(*RAW_SELECT_EXPRS)
    )
    query = (
        raw.writeStream.queryName("ingest_orders_to_landing")
        .foreachBatch(writer)
        .option("checkpointLocation", cfg.checkpoint_dir)
        .trigger(processingTime=cfg.trigger_interval)
        .start()
    )
    writer.set_query_id(str(query.id))
    return query


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)

    try:
        cfg = Config.from_env()
    except ValueError as exc:
        log.error("config error: %s", exc)
        return 2

    w = make_workspace_client()
    target_dir = verify_landing(w, cfg)
    log.info(
        "starting: kafka=%s topic=%s trigger=%r checkpoint=%s landing=%s host=%s",
        cfg.bootstrap_servers,
        cfg.topic,
        cfg.trigger_interval,
        cfg.checkpoint_dir,
        target_dir,
        os.environ.get("DATABRICKS_HOST", "<unset>"),
    )

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    writer = BatchWriter(cfg, w)

    stop_requested = threading.Event()

    def _on_signal(signum: int, _frame: Any) -> None:
        log.info("received signal %d, stopping query...", signum)
        stop_requested.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    query = start_query(spark, cfg, writer)
    log.info("query started id=%s runId=%s", query.id, query.runId)

    failure: Exception | None = None
    try:
        while not stop_requested.is_set():
            if query.awaitTermination(5):
                break  # terminated on its own (e.g. stopped externally)
    except Exception as exc:  # noqa: BLE001 - StreamingQueryException: a batch raised
        failure = exc
    finally:
        if query.isActive:
            query.stop()
        spark.stop()

    if failure is not None:
        log.error("query failed; uncommitted batch will be re-run on restart: %s", failure)
        return 1
    log.info("stopped cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
