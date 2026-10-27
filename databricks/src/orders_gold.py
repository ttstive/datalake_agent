"""Gold layer definitions (shared by 03_gold.py and tests). Pure Python, no Spark import.

Each gold table is a small aggregate fully recomputed every run:
    CREATE TABLE IF NOT EXISTS <t> (<typed columns with comments>) COMMENT ...   -- once
    INSERT OVERWRITE <t> SELECT CAST(col AS type), ... FROM (<select>)            -- every run
(Databricks rejects a column list in CREATE OR REPLACE TABLE ... AS SELECT, so this is how the
column comments survive each refresh. INSERT OVERWRITE is atomic: readers see old or new, never empty.)

SELECTs reference other tables through placeholders ({silver_orders}, {silver_order_status}, ...)
filled by `render`, so the same SQL can be validated against CTEs on a SQL warehouse.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GoldTable:
    name: str
    comment: str
    columns: list[tuple[str, str, str]]  # (name, type, comment)
    select: str  # SELECT producing exactly `columns`, by name


def secs(start: str, end: str) -> str:
    """Seconds (double, ms precision) from timestamp `start` to `end`."""
    return f"(unix_millis({end}) - unix_millis({start})) / 1000.0"


# ---------------------------------------------------------------------------------------------------------
ORDERS_PER_MINUTE = GoldTable(
    name="gold_orders_per_minute",
    comment=(
        "Gold: order activity per minute of event_time (synthetic e-commerce data). One row per minute that "
        "has at least one valid event. Recomputed from silver_orders every run."
    ),
    columns=[
        ("minute", "TIMESTAMP", "event_time truncated to the minute (UTC)"),
        ("orders_created", "BIGINT", "Orders with a created event in this minute"),
        ("orders_paid", "BIGINT", "Orders with a paid event in this minute"),
        ("orders_shipped", "BIGINT", "Orders with a shipped event in this minute"),
        ("orders_delivered", "BIGINT", "Orders with a delivered event in this minute"),
        ("orders_cancelled", "BIGINT", "Orders with a cancelled event in this minute"),
        (
            "revenue_paid",
            "DECIMAL(18,2)",
            "Sum of order totals (quantity * unit_price) of orders paid in this minute",
        ),
        ("events", "BIGINT", "Valid (deduplicated) events in this minute, any status"),
    ],
    select="""
SELECT date_trunc('MINUTE', event_time) AS minute,
       count(DISTINCT CASE WHEN status = 'created'   THEN order_id END) AS orders_created,
       count(DISTINCT CASE WHEN status = 'paid'      THEN order_id END) AS orders_paid,
       count(DISTINCT CASE WHEN status = 'shipped'   THEN order_id END) AS orders_shipped,
       count(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) AS orders_delivered,
       count(DISTINCT CASE WHEN status = 'cancelled' THEN order_id END) AS orders_cancelled,
       coalesce(sum(CASE WHEN status = 'paid' THEN line_total END), 0) AS revenue_paid,
       count(*) AS events
FROM {silver_orders}
GROUP BY date_trunc('MINUTE', event_time)""",
)

REVENUE_BY_CATEGORY = GoldTable(
    name="gold_revenue_by_category",
    comment=(
        "Gold: revenue per product category. Counts orders that reached paid (paid, shipped or delivered); "
        "orders cancelled before payment are excluded, orders cancelled after payment are included and also "
        "counted in orders_cancelled_after_paid. One row per category. Recomputed from silver_order_status."
    ),
    columns=[
        ("category", "STRING", "Product category"),
        ("orders", "BIGINT", "Orders that reached paid or later"),
        ("units", "BIGINT", "Units in those orders"),
        ("revenue", "DECIMAL(18,2)", "Sum of order_total of those orders"),
        ("avg_order_value", "DECIMAL(12,2)", "revenue / orders"),
        (
            "orders_cancelled_after_paid",
            "BIGINT",
            "Of those orders, how many were cancelled after payment",
        ),
    ],
    select="""
SELECT category,
       count(*) AS orders,
       sum(quantity) AS units,
       sum(order_total) AS revenue,
       round(avg(order_total), 2) AS avg_order_value,
       count_if(cancelled_at IS NOT NULL) AS orders_cancelled_after_paid
FROM {silver_order_status}
WHERE coalesce(paid_at, shipped_at, delivered_at) IS NOT NULL
GROUP BY category""",
)

STATUS_FUNNEL = GoldTable(
    name="gold_status_funnel",
    comment=(
        "Gold: order status funnel. One row per status (stage_order 1-5). orders_current = orders whose current "
        "status is this one; orders_reached = orders that got to this stage or beyond (created -> paid -> shipped "
        "-> delivered; cancelled = ever cancelled); pct_of_created = orders_reached / all orders * 100. "
        "Recomputed from silver_order_status."
    ),
    columns=[
        (
            "stage_order",
            "INT",
            "Funnel position: 1 created, 2 paid, 3 shipped, 4 delivered, 5 cancelled",
        ),
        ("status", "STRING", "Order status"),
        ("orders_current", "BIGINT", "Orders whose current_status is this status"),
        (
            "orders_reached",
            "BIGINT",
            "Orders that reached this stage or a later one (cumulative funnel)",
        ),
        (
            "pct_of_created",
            "DECIMAL(5,1)",
            "orders_reached as % of all orders (conversion from created)",
        ),
    ],
    select="""
WITH o AS (
  SELECT current_status,
         coalesce(paid_at, shipped_at, delivered_at) IS NOT NULL AS reached_paid,
         coalesce(shipped_at, delivered_at) IS NOT NULL AS reached_shipped,
         delivered_at IS NOT NULL AS reached_delivered,
         cancelled_at IS NOT NULL OR current_status = 'cancelled' AS reached_cancelled
  FROM {silver_order_status}
),
stages AS (
  SELECT * FROM VALUES (1, 'created'), (2, 'paid'), (3, 'shipped'), (4, 'delivered'), (5, 'cancelled')
    AS v(stage_order, status)
),
counts AS (
  SELECT st.stage_order, st.status,
         count_if(o.current_status = st.status) AS orders_current,
         count_if(CASE st.status
                    WHEN 'created'   THEN o.current_status IS NOT NULL
                    WHEN 'paid'      THEN o.reached_paid
                    WHEN 'shipped'   THEN o.reached_shipped
                    WHEN 'delivered' THEN o.reached_delivered
                    WHEN 'cancelled' THEN o.reached_cancelled
                  END) AS orders_reached
  FROM stages st LEFT JOIN o ON TRUE
  GROUP BY st.stage_order, st.status
)
SELECT stage_order, status, orders_current, orders_reached,
       round(100.0 * orders_reached / nullif(max(CASE WHEN stage_order = 1 THEN orders_reached END) OVER (), 0), 1)
         AS pct_of_created
FROM counts""",
)

# One row per pipeline step. Latency stats are computed over the rows of the MOST RECENT load of each
# step (latency_rows says how many), so each run's snapshot in the history table shows that run's latency.
PIPELINE_HEALTH = GoldTable(
    name="gold_pipeline_health",
    comment=(
        "Gold: pipeline health, one row per step (kafka -> landing -> bronze -> silver/quarantine -> silver_status "
        "-> gold, plus end_to_end). Row counts, last load time, freshness, and latency of the hop INTO each step "
        "(avg / p95 seconds over the rows of that step's most recent load). Recomputed every run; snapshots are "
        "appended to gold_pipeline_health_history."
    ),
    columns=[
        (
            "step_order",
            "INT",
            "Position of the step in the pipeline (1 = kafka ... 8 = end_to_end)",
        ),
        (
            "layer",
            "STRING",
            "kafka | landing | bronze | silver | quarantine | silver_status | gold | end_to_end",
        ),
        ("table_name", "STRING", "Table / location that represents this step"),
        (
            "row_count",
            "BIGINT",
            "Rows in this step (kafka/landing: Kafka records that reached bronze)",
        ),
        (
            "distinct_batches",
            "BIGINT",
            "Distinct local ingestor micro-batches (landing, bronze)",
        ),
        ("distinct_files", "BIGINT", "Distinct landing Parquet files loaded (bronze)"),
        ("last_loaded_at", "TIMESTAMP", "Latest arrival/load time in this step (UTC)"),
        (
            "freshness_s",
            "DOUBLE",
            "Seconds from last_loaded_at to measured_at (how stale this step is)",
        ),
        (
            "latency_hop",
            "STRING",
            "Which timestamps the latency columns measure (from -> to)",
        ),
        (
            "latency_rows",
            "BIGINT",
            "Rows the latency stats were computed over (most recent load of this step)",
        ),
        ("avg_latency_s", "DOUBLE", "Average latency of the hop, seconds"),
        (
            "p95_latency_s",
            "DOUBLE",
            "95th percentile latency of the hop, seconds (approximate)",
        ),
        (
            "duplicates_removed",
            "BIGINT",
            "silver: bronze events with an event_id minus distinct event_ids",
        ),
        (
            "quarantined_rows",
            "BIGINT",
            "quarantine: rows that failed data-quality rules",
        ),
        ("measured_at", "TIMESTAMP", "When these metrics were computed (UTC)"),
    ],
    select=f"""
WITH
bronze AS (SELECT * FROM {{bronze_orders}}),
silver AS (SELECT * FROM {{silver_orders}}),
quar   AS (SELECT * FROM {{silver_orders_quarantine}}),
b AS (
  SELECT count(*) AS n, count(DISTINCT batch_id) AS batches, count(DISTINCT _source_file) AS files,
         max(kafka_timestamp) AS last_kafka, max(ingested_at) AS last_ingested, max(_bronze_loaded_at) AS last_bronze,
         count(get_json_object(value, '$.event_id')) - count(DISTINCT get_json_object(value, '$.event_id')) AS dups
  FROM bronze
),
s AS (SELECT count(*) AS n, max(ingested_at) AS last_ingested, max(_silver_loaded_at) AS last_silver FROM silver),
q AS (SELECT count(*) AS n, max(_quarantined_at) AS last_q FROM quar),
st AS (SELECT count(*) AS n, max(updated_at) AS last_upd FROM {{silver_order_status}}),
g AS (
  SELECT (SELECT count(*) FROM {{gold_orders_per_minute}})
       + (SELECT count(*) FROM {{gold_revenue_by_category}})
       + (SELECT count(*) FROM {{gold_status_funnel}}) AS n
),
-- latency samples: (layer, seconds) over the most recent load of each step
lat AS (
  SELECT 'kafka' AS layer, {secs("event_time", "kafka_timestamp")} AS sec
    FROM silver WHERE ingested_at = (SELECT last_ingested FROM s)
  UNION ALL
  SELECT 'landing', {secs("kafka_timestamp", "ingested_at")}
    FROM bronze WHERE ingested_at = (SELECT last_ingested FROM b)
  UNION ALL
  SELECT 'bronze', {secs("ingested_at", "_bronze_loaded_at")}
    FROM bronze WHERE _bronze_loaded_at = (SELECT last_bronze FROM b)
  UNION ALL
  SELECT 'silver', {secs("_bronze_loaded_at", "_silver_loaded_at")}
    FROM silver WHERE _silver_loaded_at = (SELECT last_silver FROM s)
  UNION ALL
  SELECT 'quarantine', {secs("_bronze_loaded_at", "_quarantined_at")}
    FROM quar WHERE _quarantined_at = (SELECT last_q FROM q)
  UNION ALL
  SELECT 'gold', {secs("(SELECT last_silver FROM s)", "current_timestamp()")}
  UNION ALL
  SELECT 'end_to_end', {secs("event_time", "_silver_loaded_at")}
    FROM silver WHERE _silver_loaded_at = (SELECT last_silver FROM s)
),
lat_stats AS (
  SELECT layer, count(sec) AS latency_rows, avg(sec) AS avg_latency_s,
         percentile_approx(sec, 0.95) AS p95_latency_s
  FROM lat GROUP BY layer
),
steps AS (
  SELECT 1 AS step_order, 'kafka' AS layer, 'Kafka topic orders (local Docker)' AS table_name,
         b.n AS row_count, CAST(NULL AS BIGINT) AS distinct_batches, CAST(NULL AS BIGINT) AS distinct_files,
         b.last_kafka AS last_loaded_at, 'event_time -> kafka_timestamp (producer to Kafka)' AS latency_hop,
         CAST(NULL AS BIGINT) AS duplicates_removed, CAST(NULL AS BIGINT) AS quarantined_rows
  FROM b
  UNION ALL
  SELECT 2, 'landing', 'landing volume /landing/orders (local spark-ingestor)', b.n, b.batches, NULL,
         b.last_ingested, 'kafka_timestamp -> ingested_at (local Spark micro-batch)', NULL, NULL FROM b
  UNION ALL
  SELECT 3, 'bronze', 'bronze_orders', b.n, b.batches, b.files,
         b.last_bronze, 'ingested_at -> _bronze_loaded_at (upload + Auto Loader job)', NULL, NULL FROM b
  UNION ALL
  SELECT 4, 'silver', 'silver_orders', s.n, NULL, NULL,
         s.last_silver, '_bronze_loaded_at -> _silver_loaded_at (silver task)', b.dups, q.n FROM s, b, q
  UNION ALL
  SELECT 5, 'quarantine', 'silver_orders_quarantine', q.n, NULL, NULL,
         q.last_q, '_bronze_loaded_at -> _quarantined_at (silver task)', NULL, q.n FROM q
  UNION ALL
  SELECT 6, 'silver_status', 'silver_order_status', st.n, NULL, NULL,
         st.last_upd, NULL, NULL, NULL FROM st
  UNION ALL
  SELECT 7, 'gold', 'gold_orders_per_minute + gold_revenue_by_category + gold_status_funnel', g.n, NULL, NULL,
         current_timestamp(), 'latest _silver_loaded_at -> gold refresh', NULL, NULL FROM g
  UNION ALL
  SELECT 8, 'end_to_end', 'event -> silver_orders', s.n, NULL, NULL,
         s.last_silver, 'event_time -> _silver_loaded_at (whole pipeline)', b.dups, q.n FROM s, b, q
)
SELECT steps.step_order, steps.layer, steps.table_name, steps.row_count, steps.distinct_batches,
       steps.distinct_files, steps.last_loaded_at,
       {secs("steps.last_loaded_at", "current_timestamp()")} AS freshness_s,
       steps.latency_hop, coalesce(l.latency_rows, 0) AS latency_rows,
       round(l.avg_latency_s, 3) AS avg_latency_s, round(l.p95_latency_s, 3) AS p95_latency_s,
       steps.duplicates_removed, steps.quarantined_rows,
       current_timestamp() AS measured_at
FROM steps LEFT JOIN lat_stats l ON l.layer = steps.layer""",
)

# Rebuilt in this order (pipeline health counts the other gold tables, so it comes last).
GOLD_TABLES: list[GoldTable] = [
    ORDERS_PER_MINUTE,
    REVENUE_BY_CATEGORY,
    STATUS_FUNNEL,
    PIPELINE_HEALTH,
]

HEALTH_HISTORY_NAME = "gold_pipeline_health_history"
HEALTH_HISTORY_COMMENT = (
    "Gold: append-only history of gold_pipeline_health, one snapshot (8 rows, same measured_at) per job run. "
    "Use it to chart row counts, freshness and latency per step over time."
)

# Placeholders available to SELECTs.
SOURCE_TABLES = [
    "bronze_orders",
    "silver_orders",
    "silver_orders_quarantine",
    "silver_order_status",
    *[t.name for t in GOLD_TABLES],
]


def _sql_str(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def render(select: str, tables: dict[str, str]) -> str:
    """Fill table placeholders. `tables` maps every SOURCE_TABLES name to a table name or CTE name."""
    return select.format(**tables)


def fq_tables(catalog: str, schema: str) -> dict[str, str]:
    return {t: f"{catalog}.{schema}.{t}" for t in SOURCE_TABLES}


def create_sql(table: GoldTable, fq_name: str, comment: str | None = None) -> str:
    cols = ",\n  ".join(
        f"{n} {typ} COMMENT {_sql_str(c)}" for n, typ, c in table.columns
    )
    return f"CREATE TABLE IF NOT EXISTS {fq_name} (\n  {cols}\n) COMMENT {_sql_str(comment or table.comment)}"


def projected_select(table: GoldTable, tables: dict[str, str]) -> str:
    """The table's SELECT with each column cast to its declared type, in declared order."""
    cols = ", ".join(f"CAST({n} AS {typ}) AS {n}" for n, typ, _ in table.columns)
    return f"SELECT {cols} FROM ({render(table.select, tables)}) AS _g"


def insert_overwrite_sql(table: GoldTable, fq_name: str, tables: dict[str, str]) -> str:
    return f"INSERT OVERWRITE {fq_name} {projected_select(table, tables)}"


def history_create_sql(fq_name: str) -> str:
    return create_sql(PIPELINE_HEALTH, fq_name, comment=HEALTH_HISTORY_COMMENT)


def history_append_sql(fq_history: str, fq_health: str) -> str:
    cols = ", ".join(n for n, _, _ in PIPELINE_HEALTH.columns)
    return f"INSERT INTO {fq_history} ({cols}) SELECT {cols} FROM {fq_health}"
