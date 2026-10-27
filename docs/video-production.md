# From agent harness to streaming lakehouse

Repository review and production brief · 2026-10-27

## Evidence and current state

This is an e-commerce streaming ELT learning project, despite its historical
`datalake_duckdb` package name. The implemented pipeline does not use DuckDB.

| Area | Evidence | What the video can claim |
| --- | --- | --- |
| Development harness | `CLAUDE.md`, `.claude/agents/`, `.claude/settings.json`, `Makefile`, roadmap, ADRs | A leader coordinates DevOps, data engineering, and security review, with explicit ownership, tools, tests, and durable project context. These are coding agents, not agents processing each order. |
| Producer | `src/datalake_duckdb/producer.py` | Synthetic order lifecycle events, keyed by order ID, explicit v1 JSON schema, controllable rate, seeded generation, deliberate duplicate injection (default 1%). This is a custom generator, not an actual Faker dependency. |
| Kafka | `docker-compose.yml` | Local single-node KRaft broker, three partitions for `orders`, localhost UI and host listener, persistent volume. Replication factor one is a development configuration. |
| Local Spark bridge | `spark/ingest_orders.py` | Structured Streaming reads raw Kafka records; each nonempty batch becomes one Snappy Parquet file and is uploaded via the Databricks Files API. Default trigger: five minutes. |
| Recovery | Same ingestor, compose restart policy | Checkpointed offsets, three upload attempts, deterministic batch/query filenames, upload failure propagates to fail the batch. Do not present this as a proven global exactly-once guarantee. |
| Cloud ingestion | `databricks/src/01_bronze.py` | Auto Loader reads landing Parquet with an explicit schema, adds source/load metadata, appends to a Delta bronze table, and stops after available data is processed. |
| Scheduling | `databricks/resources/orders_ingestion.job.yml` | Five-minute UTC schedule configured as PAUSED. It must not be depicted as an active always-on cloud stream. |
| Demonstrated result | `docs/ROADMAP.md` | A prior end-to-end run is recorded as 374 rows / two batches, without gaps or duplicates. This review did not independently rerun cloud ingestion. |
| Silver, gold, dashboard | Roadmap Phase 3–4; dbt example files | Planned. Existing dbt models are starter examples, not order transformations. Show future layers with a visible PLANNED label. |
| Coding telemetry | `observability/claude_mlflow_hook.py`, `.claude/settings.json`, `docs/observability.md` | Local MLflow parent/child runs, token counts, context fill, cache ratios, compaction events. Context loss is a token-count estimate, not a measurement of knowledge lost. |
| Workflow evaluations | `observability/agent_evals.py`, `tests/test_agent_evals.py`, `Makefile` | Eight heuristic workflow checks, one row per subagent plus leader, MLflow `claude-agents-evals` runs, per-task rationales and `results.json`. Failed `genai.evaluate` falls back to metrics/artifacts. These checks do not establish output correctness or causal improvement. |
| Cowork room | `src/datalake_duckdb/cowork.py`, `cowork.html`, `docs/cowork-monitor.md` | Local classroom-style room, grouped application desks, reported model-session desks, process CPU/memory, explicit usage-event API, budget/rate/context alerts. Usage adapters are not connected automatically; actual generation and task progress remain unknown. |
| Evaluation API | `src/datalake_duckdb/agent_monitor.py` | Separate provider-agnostic metadata API with caller-supplied quality scores and explicit paired comparisons. It does not automatically evaluate the coding agents or demonstrate learning. |

## Actual data path

```text
LAPTOP / DOCKER                            DATABRICKS
synthetic orders → Kafka → local Spark
                             │
                             └─ Parquet / outbound HTTPS → UC Volume landing
                                                            ↓
                                                   scheduled Auto Loader
                                                            ↓
                                                      Delta bronze
                                                            ┊ planned
                                                    silver → gold → BI
```

ADR-002 supersedes ADR-001's hosted Kafka design. Databricks does not connect
inbound to the laptop's Kafka broker. The README currently focuses on the agent
monitor API, while the roadmap's opening diagram still resembles the old direct
Kafka-to-cloud-Spark design; use ADR-002 and the implementation as video sources.

Near real time means a design estimate of roughly 5–10 minutes, plus processing
and scheduling overhead, when the scheduled pipeline is enabled. It is not a
measured latency SLA, and the current paused job requires manual execution.

## Findings relevant to the story

- There are two systems to explain: the development control loop and the data
  execution path. Animate their connection, without making the agents look like
  services inside the Kafka stream.
- Retry-safe filenames and checkpoints are concrete engineering details worth
  showing. They do not deduplicate business events; event-ID dedup is planned
  for silver. The reader uses `failOnDataLoss=false`, so missing retained Kafka
  offsets need explicit monitoring rather than an unconditional no-loss claim.
- One Parquet file per batch and a single broker make the bridge understandable
  for a local demo. They do not establish enterprise throughput or availability.
- The security report records unresolved token rotation, identity/grant, and
  hardening findings. A security review is a gate in the harness; do not show a
  blanket production security approval.
- Counts-only agent monitoring and conversation tracing are different. Full
  tracing is enabled in the checked-in Claude settings. Recreate telemetry
  visuals using labeled illustrative values rather than exposing raw traces.
- Existing verification during this review: 63 Python tests passed; Ruff passed.
  These checks do not validate a live cloud deployment or recovery semantics.

## Creative direction

Working title: **Build the pipeline. Evaluate the builders.**

Vertical 1080 × 1920, 30 fps, 100 seconds. English is the
language, confirmed by the user. Use a charcoal canvas, warm ivory
typography, amber for orchestration, red-orange for event movement, and bronze,
silver, and gold accents for the lakehouse layers. Large editorial headlines,
thin technical connectors, rounded infrastructure objects, and generous space.

Follow one orange event through the architecture. Zoom into it for the JSON
schema, fan it into Kafka partitions, gather events into a batch, then carry
the batch across the local/cloud boundary. Frame-based movement must remain
deterministic and seekable. Hold important states long enough to read.

Reference reel: https://www.instagram.com/reel/De-bPOHxA7y/

Web retrieval initially failed, but the reel was subsequently opened in Chrome.
Observed frames combine an upper dark technical animation with a lower presenter
shot, amber/red highlights, small monospace labels, and prominent captions. The
draft adopts the dark palette and technical diagrams while remaining entirely
animated. Audio, full shot timing, and the complete reel were not analyzed.

## Storyboard and narration draft

Pacing revision: 100 seconds (previously 150). Every scene has a shorter hold;
37 seconds remain dedicated to evaluations. Entrance animations retain their
original timing, leaving the remaining scene time for reading. The narration
below is condensed to match; voiceover timing remains unverified.

| Time | On screen / movement | Narration |
| --- | --- | --- |
| 0–5s | Order event, FROM PROMPT TO PIPELINE. | Follow one order through the pipeline—and meet the agents building it. |
| 5–12s | Leader → DevOps, data engineering, security reviewer. | The leader plans and verifies. Specialists own infrastructure, data engineering, and security review. |
| 12–20s | Animated cowork classroom, four application desks. Persistent recreation label. | The cowork room gives AI tools a desk. Inspect observed signals; movement does not prove generation. |
| 20–26s | MLflow parent/child usage runs and illustrative context chart. | MLflow telemetry tracks tokens, context, and caching. Resource use alone does not establish quality. |
| 26–35s | Telemetry → separate evaluation experiment. Three concrete workflow questions. | Eight workflow scorers ask: did agents stay in their lanes, attempt verification, and follow changes with review? |
| 35–44s | WITHOUT EVALUATION RUNS above WITH MLFLOW EVALUATION RUNS. | Without evaluation runs, reviewers reconstruct behavior from logs. With them, task-level scores and rationales give a structured record to compare. |
| 44–55s | Recreated MLflow task scorecard. Example leader review failure opens into its rationale. | In claude-agents-evals, inspect pass rates and task rationales. This illustrative failure flags infrastructure changed without a later security run. It identifies a process gap. |
| 55–63s | Inspect → adjust harness → re-evaluate → compare. Git/session tags. | Inspect the flag, improve instructions, then evaluate comparable work. Check for regressions. Keep tests and human review. |
| 63–68s | Event JSON fans into Kafka partitions. | Synthetic orders enter Kafka, keyed by order ID. |
| 68–74s | Kafka records → Spark micro-batch → deterministic Parquet tile. | Local Spark writes raw micro-batches to Parquet and checkpoints progress. |
| 74–80s | Outbound HTTPS bridge into Unity Catalog landing volume. | The laptop pushes files over HTTPS into Unity Catalog landing. |
| 80–87s | Auto Loader fills Delta bronze. PAUSED schedule badge. | Auto Loader appends files to Delta bronze. The five-minute schedule is currently paused. |
| 87–94s | Implemented bronze → planned silver, gold, BI. | Bronze is implemented. Silver, gold, and the BI dashboard remain planned. |
| 94–100s | Cowork → evaluation runs → lakehouse. Evidence-focused closing. | Cowork makes activity visible. MLflow evaluations make workflow evidence inspectable. Build trust through evidence. |

## Evaluation story: evidence boundaries

The comparison is **without versus with structured evaluation runs**, not a
measured experiment demonstrating a quality gain. Both workflows can include
tests, review, and logs. No pass-rate improvement, dollar savings, or automatic
learning is claimed. The scorecard contains deliberately illustrative results,
not an export from a live MLflow run.

The eight checks are `stays_in_lane`, `reviewer_read_only`,
`no_secret_exposure`, `verified_work`, `permission_friction`,
`tool_error_rate`, `security_gate`, and `roadmap_updated`. N/A values are
excluded from pass rates. Extraction is best effort. In particular,
`verified_work` detects a non-denied verification command, not that all tests
passed; `security_gate` detects a later review start, not that review findings
were resolved. Secret-pattern checks are limited heuristics, not a security
certification. A permission denial is a friction signal, not a reason to weaken
permissions.

The evaluation run records `git_commit`, `sessions`, and `evaluate_path` tags.
`genai.evaluate` assessments are available on its successful path; fallback
mode retains plain pass-rate metrics and the results artifact. Default code
scorers evaluate derived features locally; some labels and paths are retained
as metadata. The optional report-quality judge is off by default and sends
report text to a model when explicitly enabled. It was not invoked for this
video update. The older provider-neutral evaluation API remains independent.

## Verification for this revision

- Reviewed the new evaluation implementation, scorer tests, Makefile entry,
  observability documentation, and cowork implementation.
- `npm run lint` (ESLint + TypeScript) and `npm run build` passed.
- Evaluation and cowork suites: 21 tests passed.
- Inspected the cowork recreation, without/with comparison, and MLflow failure
  example in Remotion Studio. Animations use frame-based deterministic motion.
- No live evaluation, paid judge, pipeline job, or cloud deployment was run.
- Existing Remotion/Zod version warning remains; final MP4 export is not verified.

## Production notes

Use Remotion for precise diagrams and typography. The official
`remotion-best-practices` skill has been installed globally for the next turn.
Keep the video project isolated in `video/` so its dependencies do not affect
the Python or Databricks environments. The updated deliverable is a silent 100-second animated Studio preview with
explanatory text. The narration above is a script; recorded voiceover, music,
final captions, and MP4 export have not been added.

Avoid filming real credentials, personal workspace identity, raw agent traces,
or private log contents. Telemetry charts in the animation are illustrative.
Use the prior 374-row result only with the caption “recorded demo run,” never
as a fresh benchmark. No cloud resources or schedules need changing to make
this animation.
