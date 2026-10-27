"""Silver layer definitions for order events (schema v1, ADR-001), shared by 02_silver.py and tests.

Pure Python, no Spark import: everything is rendered as Spark SQL text so it can be unit-tested
locally and validated read-only on a SQL warehouse. The notebook only wires these into a stream.

Flow per micro-batch (foreachBatch in 02_silver.py):
    bronze rows ─► parsed_select_sql   (from_json with explicit schema, typed columns, dq_reason)
                   ├─ dq_reason IS NOT NULL ─► quarantine_merge_sql      (insert-only on Kafka coordinates)
                   └─ dq_reason IS NULL ────► valid_events_sql           (dedup on event_id inside the batch)
                                              ├─► silver_orders_merge_sql (insert-only on event_id)
                                              └─► order_status_merge_sql  (one row per order_id)
"""

from __future__ import annotations

SCHEMA_VERSION = 1

# Status ranking: breaks ties when two events of an order share event_time.
# cancelled is terminal (producer only cancels from created/paid) so it ranks highest.
STATUS_RANK: dict[str, int] = {
    "created": 1,
    "paid": 2,
    "shipped": 3,
    "delivered": 4,
    "cancelled": 5,
}
ALLOWED_STATUSES: tuple[str, ...] = tuple(STATUS_RANK)
QUANTITY_MIN, QUANTITY_MAX = 1, 5

CORRUPT_COL = "_corrupt_record"

# Explicit JSON schema (never inferred). unit_price and event_time are read as their raw JSON text and
# then try_cast to DECIMAL(10,2) / TIMESTAMP: exact decimal (no float rounding) and bad values become
# NULL -> quarantined instead of failing the stream (serverless runs with ANSI mode on).
# _corrupt_record is filled by from_json when any field has the wrong JSON type (e.g. quantity "x").
EVENT_JSON_SCHEMA = ", ".join(
    [
        "event_id STRING",
        "order_id STRING",
        "customer_id STRING",
        "product STRING",
        "category STRING",
        "quantity INT",
        "unit_price STRING",
        "status STRING",
        "event_time STRING",
        "schema_version INT",
        f"{CORRUPT_COL} STRING",
    ]
)

# (column, SQL expression over the from_json struct `_e`), in silver column order.
TYPED_COLUMNS: list[tuple[str, str]] = [
    ("event_id", "_e.event_id"),
    ("order_id", "_e.order_id"),
    ("customer_id", "_e.customer_id"),
    ("product", "_e.product"),
    ("category", "_e.category"),
    ("quantity", "_e.quantity"),
    ("unit_price", "try_cast(_e.unit_price AS DECIMAL(10,2))"),
    ("status", "_e.status"),
    ("event_time", "try_cast(_e.event_time AS TIMESTAMP)"),
    ("schema_version", "_e.schema_version"),
    (CORRUPT_COL, f"_e.{CORRUPT_COL}"),
]

# Bronze columns carried through (bronze name -> silver name).
BRONZE_COLUMNS: list[tuple[str, str]] = [
    ("kafka_key", "kafka_key"),
    ("value", "value"),
    ("topic", "topic"),
    ("`partition`", "kafka_partition"),
    ("`offset`", "kafka_offset"),
    ("kafka_timestamp", "kafka_timestamp"),
    ("batch_id", "batch_id"),
    ("ingested_at", "ingested_at"),
    ("_bronze_loaded_at", "_bronze_loaded_at"),
]

_STATUS_LIST = ", ".join(f"'{s}'" for s in ALLOWED_STATUSES)

# Data-quality rules, evaluated in order; the FIRST failing rule becomes dq_reason.
# Each condition is TRUE when the row FAILS. Columns are the typed silver columns plus _is_valid_json.
DQ_RULES: list[tuple[str, str]] = [
    ("invalid_json", "NOT _is_valid_json"),
    ("type_mismatch", f"{CORRUPT_COL} IS NOT NULL"),
    ("missing_event_id", "event_id IS NULL OR trim(event_id) = ''"),
    ("missing_order_id", "order_id IS NULL OR trim(order_id) = ''"),
    ("invalid_status", f"status IS NULL OR status NOT IN ({_STATUS_LIST})"),
    (
        "invalid_quantity",
        f"quantity IS NULL OR quantity NOT BETWEEN {QUANTITY_MIN} AND {QUANTITY_MAX}",
    ),
    ("invalid_unit_price", "unit_price IS NULL OR unit_price <= 0"),
    ("missing_event_time", "event_time IS NULL"),
    (
        "unsupported_schema_version",
        f"schema_version IS NULL OR schema_version <> {SCHEMA_VERSION}",
    ),
]

# Final silver_orders column order (must match the DDL in 02_silver.py).
SILVER_ORDERS_COLUMNS: list[str] = [
    "event_id",
    "order_id",
    "customer_id",
    "product",
    "category",
    "quantity",
    "unit_price",
    "line_total",
    "status",
    "event_time",
    "schema_version",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "batch_id",
    "ingested_at",
    "_bronze_loaded_at",
    "_silver_loaded_at",
]

QUARANTINE_COLUMNS: list[str] = [
    "dq_reason",
    "value",
    "kafka_key",
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "batch_id",
    "ingested_at",
    "event_id",
    "order_id",
    "_bronze_loaded_at",
    "_quarantined_at",
]

STATUS_TIMESTAMP_COLUMNS: list[str] = [f"{s}_at" for s in ALLOWED_STATUSES]

ORDER_STATUS_COLUMNS: list[str] = [
    "order_id",
    "customer_id",
    "product",
    "category",
    "quantity",
    "unit_price",
    "order_total",
    "current_status",
    *STATUS_TIMESTAMP_COLUMNS,
    "last_event_time",
    "updated_at",
]


def status_rank(status: str | None) -> int:
    """Rank of a status for tie-breaking; unknown/None -> 0."""
    return STATUS_RANK.get(status or "", 0)


def status_rank_sql(col: str) -> str:
    whens = " ".join(f"WHEN '{s}' THEN {r}" for s, r in STATUS_RANK.items())
    return f"CASE {col} {whens} ELSE 0 END"


def dq_reason_sql() -> str:
    """CASE expression returning the first failing rule name, or NULL when the row is valid."""
    whens = " ".join(f"WHEN ({cond}) THEN '{reason}'" for reason, cond in DQ_RULES)
    return f"CASE {whens} ELSE NULL END"


def parsed_select_sql(source: str) -> str:
    """Parse bronze rows from `source` (table/view name or parenthesised subquery).

    Output: carried bronze columns + typed event columns + line_total + dq_reason.
    """
    bronze = ", ".join(f"{src} AS {dst}" for src, dst in BRONZE_COLUMNS)
    typed = ", ".join(f"{expr} AS {name}" for name, expr in TYPED_COLUMNS)
    return f"""
SELECT *, {dq_reason_sql()} AS dq_reason
FROM (
  SELECT {bronze}, {typed},
         CAST(_e.quantity * try_cast(_e.unit_price AS DECIMAL(10,2)) AS DECIMAL(12,2)) AS line_total,
         _is_valid_json
  FROM (
    SELECT *,
           from_json(value, '{EVENT_JSON_SCHEMA}', map('columnNameOfCorruptRecord', '{CORRUPT_COL}')) AS _e,
           value IS NOT NULL AND try_parse_json(value) IS NOT NULL AS _is_valid_json
    FROM {source} AS _bronze
  ) AS _p
) AS _t"""


def valid_events_sql(parsed: str) -> str:
    """Valid rows from a parsed relation, deduplicated on event_id (keeps the first Kafka record)."""
    cols = ", ".join(c for c in SILVER_ORDERS_COLUMNS if c != "_silver_loaded_at")
    return f"""
SELECT {cols}
FROM (
  SELECT *, row_number() OVER (
           PARTITION BY event_id ORDER BY kafka_timestamp, kafka_partition, kafka_offset) AS _rn
  FROM {parsed} AS _v
  WHERE dq_reason IS NULL
) AS _d
WHERE _rn = 1"""


def silver_orders_merge_sql(target: str, parsed: str) -> str:
    """Insert-only MERGE on event_id: an event already in silver is never inserted twice (idempotent)."""
    cols = ", ".join(SILVER_ORDERS_COLUMNS)
    vals = ", ".join(
        "current_timestamp()" if c == "_silver_loaded_at" else f"s.{c}"
        for c in SILVER_ORDERS_COLUMNS
    )
    return f"""
MERGE INTO {target} AS t
USING ({valid_events_sql(parsed)}) AS s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"""


def quarantine_merge_sql(target: str, parsed: str) -> str:
    """Insert-only MERGE of failed rows, keyed on Kafka coordinates (topic, partition, offset)."""
    cols = ", ".join(QUARANTINE_COLUMNS)
    vals = ", ".join(
        "current_timestamp()" if c == "_quarantined_at" else f"s.{c}"
        for c in QUARANTINE_COLUMNS
    )
    src_cols = ", ".join(c for c in QUARANTINE_COLUMNS if c != "_quarantined_at")
    return f"""
MERGE INTO {target} AS t
USING (
  SELECT DISTINCT {src_cols} FROM {parsed} AS _q WHERE dq_reason IS NOT NULL
) AS s
ON t.topic <=> s.topic AND t.kafka_partition <=> s.kafka_partition AND t.kafka_offset <=> s.kafka_offset
WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"""


def order_status_source_sql(events: str) -> str:
    """Collapse valid events (relation `events`) to one row per order_id.

    latest event = max by (event_time, status rank); cancelled is terminal and always wins.
    <status>_at = earliest event_time seen for that status.
    """
    rank = status_rank_sql("status")
    firsts = ",\n         ".join(
        f"min(CASE WHEN status = '{s}' THEN event_time END) AS {s}_at"
        for s in ALLOWED_STATUSES
    )
    return f"""
SELECT order_id,
       latest.customer_id, latest.product, latest.category, latest.quantity, latest.unit_price,
       CAST(latest.quantity * latest.unit_price AS DECIMAL(12,2)) AS order_total,
       CASE WHEN cancelled_at IS NOT NULL THEN 'cancelled' ELSE latest.status END AS current_status,
       CASE WHEN cancelled_at IS NOT NULL THEN {STATUS_RANK["cancelled"]} ELSE latest.status_rank END AS status_rank,
       {", ".join(STATUS_TIMESTAMP_COLUMNS)},
       last_event_time
FROM (
  SELECT order_id,
         max_by(
           named_struct('customer_id', customer_id, 'product', product, 'category', category,
                        'quantity', quantity, 'unit_price', unit_price, 'status', status,
                        'status_rank', {rank}),
           named_struct('event_time', event_time, 'status_rank', {rank})
         ) AS latest,
         {firsts},
         max(event_time) AS last_event_time
  FROM {events} AS _e
  GROUP BY order_id
) AS _agg"""


def order_status_merge_sql(target: str, events: str) -> str:
    """Upsert the current state of each order. Idempotent and order-independent:

    * <status>_at uses least(): keeps the earliest non-null timestamp ever seen.
    * last_event_time uses greatest().
    * descriptive columns + current_status move only when the incoming latest event is newer
      (event_time, then status rank); once cancelled_at is set the status stays cancelled.
    """
    t_rank = status_rank_sql("t.current_status")
    newer = (
        f"(t.last_event_time IS NULL OR s.last_event_time > t.last_event_time "
        f"OR (s.last_event_time = t.last_event_time AND s.status_rank > {t_rank}))"
    )
    descriptive = [
        "customer_id",
        "product",
        "category",
        "quantity",
        "unit_price",
        "order_total",
    ]
    sets = [f"t.{c} = CASE WHEN {newer} THEN s.{c} ELSE t.{c} END" for c in descriptive]
    sets.append(
        "t.current_status = CASE WHEN coalesce(t.cancelled_at, s.cancelled_at) IS NOT NULL THEN 'cancelled' "
        f"WHEN {newer} THEN s.current_status ELSE t.current_status END"
    )
    sets += [f"t.{c} = least(t.{c}, s.{c})" for c in STATUS_TIMESTAMP_COLUMNS]
    sets.append("t.last_event_time = greatest(t.last_event_time, s.last_event_time)")
    sets.append("t.updated_at = current_timestamp()")
    cols = ", ".join(ORDER_STATUS_COLUMNS)
    vals = ", ".join(
        "current_timestamp()" if c == "updated_at" else f"s.{c}"
        for c in ORDER_STATUS_COLUMNS
    )
    set_sql = ",\n  ".join(sets)
    return f"""
MERGE INTO {target} AS t
USING ({order_status_source_sql(events)}) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
  {set_sql}
WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"""
