# Orders dashboard — build guide (Databricks AI/BI)

Data: synthetic e-commerce order events (fake data). Everything lives in `workspace.ecommerce`.
The `orders_ingestion` job refreshes bronze → silver → gold every run (every 5 min when unpaused),
so set the dashboard to refresh on a similar schedule.

## Tables to use

| table | grain | use it for |
|---|---|---|
| `gold_pipeline_health` | 1 row per pipeline step (8) | current state of each step: rows, freshness, latency |
| `gold_pipeline_health_history` | 8 rows per job run (`measured_at`) | latency / row counts over time |
| `gold_orders_per_minute` | 1 row per minute of `event_time` | order activity and revenue over time |
| `gold_revenue_by_category` | 1 row per category | revenue split |
| `gold_status_funnel` | 1 row per status (5) | funnel created → paid → shipped → delivered, cancellations |
| `silver_orders_quarantine` | 1 row per rejected Kafka record | data-quality drill-down (`dq_reason`, raw `value`) |

All tables and columns have comments (Catalog Explorer → table → Columns).

Pipeline steps in `gold_pipeline_health.layer` (`step_order` 1–8):
`kafka` → `landing` (local spark-ingestor) → `bronze` → `silver` / `quarantine` → `silver_status` → `gold` → `end_to_end`.
`latency_hop` says which two timestamps a row's latency measures. `avg_latency_s` and `p95_latency_s` are computed
over the rows of that step's **most recent load** (`latency_rows` = how many), so the history table shows each run's latency.

## Suggested layout: one section per pipeline step

1. **Pipeline at a glance**: Q1 table (+ Q5 counters on top). The "engineering" view of every step.
2. **Ingestion: Kafka → landing → bronze**: Q2 (latency per hop) and Q3/Q4 filtered to `landing`, `bronze`.
3. **Silver: parsing, dedup, data quality**: Q5 counters, Q6 quarantine by reason, Q4 for `silver` vs `quarantine`.
4. **Gold: business view**: Q7 orders per minute, Q8 revenue by category, Q9 funnel.
5. **End-to-end latency over time**: Q3 (`end_to_end` line), plus freshness from Q1.

Tips: add a dataset per query (Data tab → "Create from SQL"), then add the widget in the Canvas.
Charts over `*_history` grow by 8 rows per run; the 24 h filter in Q3 keeps them light.

## Ready queries (tested against the current data)

### Q1 · Pipeline steps table
_Visualization_: Table (one row per step, in pipeline order)

```sql
SELECT step_order, layer, table_name, row_count, last_loaded_at,
       round(freshness_s / 60, 1) AS freshness_min, latency_hop, avg_latency_s, p95_latency_s
FROM workspace.ecommerce.gold_pipeline_health
ORDER BY step_order
```

### Q2 · Latency per hop (latest run)
_Visualization_: Bar chart: x = step, y = avg_latency_s and p95_latency_s

```sql
SELECT concat(step_order, '. ', layer) AS step, latency_hop, avg_latency_s, p95_latency_s
FROM workspace.ecommerce.gold_pipeline_health
WHERE layer IN ('kafka', 'landing', 'bronze', 'silver', 'gold')
ORDER BY step_order
```

### Q3 · p95 latency over time
_Visualization_: Line chart: x = measured_at, y = p95_latency_s, color = layer

```sql
SELECT measured_at, layer, p95_latency_s
FROM workspace.ecommerce.gold_pipeline_health_history
WHERE layer IN ('landing', 'bronze', 'silver', 'end_to_end')
  AND measured_at >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY measured_at
```

### Q4 · Rows per layer over time
_Visualization_: Line chart: x = measured_at, y = row_count, color = layer

```sql
SELECT measured_at, layer, row_count
FROM workspace.ecommerce.gold_pipeline_health_history
WHERE layer IN ('bronze', 'silver', 'quarantine', 'silver_status')
ORDER BY measured_at
```

### Q5 · Data quality KPIs
_Visualization_: Counter tiles: bronze_rows, silver_events, duplicates_removed, quarantined_rows

```sql
SELECT max(CASE WHEN layer = 'bronze' THEN row_count END)        AS bronze_rows,
       max(CASE WHEN layer = 'silver' THEN row_count END)        AS silver_events,
       max(CASE WHEN layer = 'silver' THEN duplicates_removed END) AS duplicates_removed,
       max(CASE WHEN layer = 'silver' THEN quarantined_rows END)   AS quarantined_rows,
       max(measured_at) AS measured_at
FROM workspace.ecommerce.gold_pipeline_health
```

### Q6 · Quarantine by reason
_Visualization_: Bar chart: x = dq_reason, y = quarantined (or a table)

```sql
SELECT dq_reason, count(*) AS quarantined, max(_quarantined_at) AS last_seen
FROM workspace.ecommerce.silver_orders_quarantine
GROUP BY dq_reason
ORDER BY quarantined DESC
```

### Q7 · Orders per minute (last hour of data)
_Visualization_: Line chart: x = minute, y = orders_created/paid/delivered/cancelled; second line or bar for revenue_paid

```sql
SELECT minute, orders_created, orders_paid, orders_delivered, orders_cancelled, revenue_paid
FROM workspace.ecommerce.gold_orders_per_minute
WHERE minute >= (SELECT max(minute) FROM workspace.ecommerce.gold_orders_per_minute) - INTERVAL 60 MINUTES
ORDER BY minute
```

### Q8 · Revenue by category
_Visualization_: Bar chart: x = category, y = revenue

```sql
SELECT category, revenue, orders, avg_order_value
FROM workspace.ecommerce.gold_revenue_by_category
ORDER BY revenue DESC
```

### Q9 · Order status funnel
_Visualization_: Funnel or bar chart: x = status (sort by stage_order), y = orders_reached; label pct_of_created

```sql
SELECT stage_order, status, orders_reached, pct_of_created, orders_current
FROM workspace.ecommerce.gold_status_funnel
ORDER BY stage_order
```
## Reading the numbers

- First run after deploying silver/gold: silver loads the whole bronze backlog in one go, so the `silver` and
  `end_to_end` latency of that first snapshot is large (hours). From the next runs on it reflects normal latency.
- `kafka` latency is ~0 because the producer stamps the Kafka record with the event time.
- `duplicates_removed` = bronze events carrying an `event_id` minus distinct `event_id`s (the producer sends ~1% duplicates).
- `customer_id` is a synthetic pseudo-identifier. Don't build widgets that list customers individually.
