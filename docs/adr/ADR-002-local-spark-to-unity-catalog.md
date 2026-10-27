# ADR-002: Local Spark micro-batches into Unity Catalog (no hosted Kafka)

Date: 2026-10-27 · Status: Accepted · Supersedes ADR-001 decision 1 (Confluent Cloud)

## Context
Databricks (cloud) cannot reach Kafka on the laptop, and we won't use a hosted Kafka.

## Decision
```
producer ─► Kafka (Docker) ─► spark-ingestor (Docker, PySpark Structured Streaming, trigger every 5 min)
                                   │ foreachBatch: write micro-batch as Parquet, upload via Databricks Files API
                                   ▼
             UC Volume  /Volumes/<catalog>/ecommerce/landing/orders/      (landing zone, raw files)
                                   │ Databricks job every 5 min: Auto Loader, trigger(availableNow)
                                   ▼
             <catalog>.ecommerce.bronze_orders  ─► silver ─► gold ─► dashboard
```
- **Push, not pull**: Kafka stays private on the laptop; only outbound HTTPS to Databricks.
- **ELT**: landing files hold raw Kafka records (key, value as string, topic, partition, offset, kafka timestamp, batch id, ingested_at). All parsing happens in Databricks.
- **Exactly-once-ish**: Spark checkpoint tracks Kafka offsets; file names are deterministic per batch so a retried batch overwrites instead of duplicating; silver dedups on `event_id` anyway.
- **Latency**: ~5–10 min end to end (ingestor trigger + job schedule). Near real time, as agreed.
- Spark runs in Docker (`apache/spark` image) so it doesn't need local Java and doesn't conflict with `databricks-connect` in the uv env.

## Consequences
- The Databricks token reaches the ingestor container through docker compose env interpolation from `.env` (never baked into the image). Prefer OAuth/service principal later (security finding #8).
- A 5-min scheduled job uses serverless compute 288×/day; deployed paused until the user confirms.
