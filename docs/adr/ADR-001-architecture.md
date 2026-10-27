# ADR-001: Streaming architecture

Date: 2026-10-27 · Status: Accepted

## Decisions
1. **Kafka**: local single-node Kafka in Docker for development; a hosted Kafka (Confluent Cloud) that Databricks reads from in Phase 2. The producer writes to either one by switching env config only.
2. **Latency target**: near real time. Databricks streams run as micro-batches (`trigger(availableNow=True)` on a scheduled job every few minutes, or `processingTime` if the workspace allows always-on compute). The dashboard auto-refreshes.
3. **Domain**: e-commerce orders. Topic `orders`, JSON value, key = `order_id`.

## Event schema (v1)
| field | type | notes |
|---|---|---|
| event_id | string (uuid) | unique per event, used for dedup |
| order_id | string (uuid) | same order emits several status events |
| customer_id | string | synthetic |
| product | string | |
| category | string | |
| quantity | int | 1–5 |
| unit_price | decimal(10,2) | |
| status | string | created → paid → shipped → delivered, or cancelled |
| event_time | timestamp (ISO-8601 UTC) | |
| schema_version | int | 1 |

## Consequences
- Phase 1 is fully local and free.
- Phase 2 needs a Confluent Cloud account (user action) and Kafka credentials in a Databricks secret scope.
