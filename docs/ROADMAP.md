# Roadmap

Goal: a simple streaming ELT on Databricks, with a dashboard that shows both the data and how it moves through each step.

```
faker producer ──► Kafka topic ──► Spark Structured Streaming (Databricks)
                                      │
                                      ├─► bronze  (raw Kafka records)
                                      ├─► silver  (parsed, typed, deduped)
                                      └─► gold    (aggregates + pipeline health)
                                                    │
                                                    └─► Databricks AI/BI dashboard
```

Status legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` blocked

## Decisions
- [ADR-001](adr/ADR-001-architecture.md): e-commerce orders, event schema, near real time.
- [ADR-002](adr/ADR-002-local-spark-to-unity-catalog.md): local Spark reads Kafka every 5 min → UC Volume landing → Databricks job → bronze (no hosted Kafka).

## Phase 0 — Foundations (lead)
- [x] Agent team defined in `.claude/agents/`
- [x] Leader instructions in `CLAUDE.md`
- [x] Resolve open decisions → ADR-001

## Phase 1 — Local streaming (devops + data-engineer)
- [x] devops: `docker-compose.yml` with single-node Kafka (KRaft) + topic `orders` + Kafka UI
- [x] devops: `Makefile` (`kafka-up`, `kafka-down`, `produce`)
- [x] data-engineer: fake-data producer with rate/duration flags + unit tests
- [x] security review: compose file, ports, deps → [report](security/2026-10-27-phase1-review.md)

## Phase 2 — Spark ingestion into Databricks (devops + data-engineer)
- [x] devops: UC schema `workspace.ecommerce` + volume `landing` (`make db-setup`)
- [x] devops: `spark-ingestor` Docker service in compose (PySpark + Kafka connector + databricks-sdk)
- [x] data-engineer: Spark Structured Streaming ingestor, 5-min trigger, Parquet batches → landing volume
- [x] data-engineer: bronze notebook (Auto Loader, availableNow) → `ecommerce.bronze_orders`
- [x] devops: Asset Bundle job `orders_ingestion` every 5 min (deployed, PAUSED)
- [x] end-to-end run: producer → Kafka → landing → bronze (374 rows / 2 batches, no gaps or dups, 2026-10-27)
- [!] security review: token handling in container, volume/schema grants → [report](security/2026-10-27-phase2-review.md) (blocked on #1)
- [ ] user: confirm unpausing the 5-min schedule

## Phase 3 — ELT layers (data-engineer)
- [ ] silver: explicit schema, dedup, watermark
- [ ] gold: orders per minute, revenue by category, order status funnel
- [ ] gold: pipeline health (rows and latency per layer)
- [ ] data quality expectations

## Phase 4 — Dashboard (data-engineer + devops)
- [ ] dashboard datasets + layout (one section per pipeline step)
- [ ] deploy dashboard via bundle, auto-refresh
- [ ] security review: dashboard sharing, UC grants

## Phase 5 — Polish
- [ ] CI: lint, tests, `bundle validate`
- [ ] README walkthrough of the whole flow
