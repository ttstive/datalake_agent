# Agent evaluation and monitoring

`datalake_duckdb.agent_monitor` is a provider-agnostic instrumentation layer for
agent applications and supported telemetry adapters. It records run metadata,
usage, timing, outcome, and a caller-evaluated task-quality score in MLflow.
Quality uses a documented **0.0–1.0** scale (0 = unacceptable, 1 = fully
correct); choose and apply a consistent rubric for your tasks.

The module is metadata-only by design: it does not accept or log prompts,
responses, or context content. Do not put content, secrets, or personal data in
identifier fields such as `task_case_id` or `context_source`. Logged identifiers
must be opaque tokens containing only letters, numbers, periods, underscores,
colons, slashes, or hyphens; do not encode personal or confidential information
in them.

## Configure and log a run

Install the project's dependencies with `uv sync`. By default the monitor uses
the MLflow `databricks` tracking URI and the `agent-monitoring` experiment.
Override the URI with `AGENT_MONITOR_TRACKING_URI` (or `MLFLOW_TRACKING_URI`);
override the experiment with `AGENT_MONITOR_EXPERIMENT` or the
`experiment_name` argument to `AgentMonitor`. Configure workspace
authentication using the Databricks-supported environment or secret
mechanism; do not put credentials in code.

```python
from datalake_duckdb.agent_monitor import AgentMonitor, AgentRunRecord

# Fill these fields from your agent's own measurements and evaluator.
record = AgentRunRecord(
    agent_id="faq-agent",
    provider="provider-name",
    model="model-name",
    task_id="answer-faq",
    task_case_id="faq-case-017",
    duration_seconds=1.42,
    outcome="success",  # success, failure, or partial
    input_tokens=240,
    output_tokens=80,
    total_tokens=320,  # must equal input_tokens + output_tokens
    context_tokens=100,  # subset of input_tokens
    context_variant="with_context",  # or "without_context"
    context_source="product-docs-index",
    quality_score=0.9,
)

run_id = AgentMonitor().log_record(record)
print(run_id)  # safe MLflow run ID, not content or credentials
```

For `without_context`, set `context_tokens=0` and `context_source=None`.
`AgentRunRecord` validates identifiers, counts, duration, outcome, context
metadata, and quality score; invalid values raise `ValueError` rather than
being silently normalized (type errors raise `TypeError`). Runs log
provider/model/task identifiers as parameters/tags and numeric metrics for
duration, token counts, and quality. The experiment URI and name are
configured before each write.

## Explicit with/without-context comparison

The monitor does not infer context learning from individual runs. To compare
the same task case, explicitly provide both run IDs and the scores evaluated
with the same rubric. The comparison is logged as a separate run; a positive
`quality_delta` means the with-context score was higher.

```python
from datalake_duckdb.agent_monitor import AgentMonitor, QualityComparison

comparison = QualityComparison(
    task_case_id="faq-case-017",
    without_context_run_id="baseline-run-id",
    with_context_run_id="context-run-id",
    without_context_score=0.65,
    with_context_score=0.9,
)
comparison_run_id = AgentMonitor().log_quality_comparison(comparison)
```

This is an explicitly supplied paired quality delta, not a causal or
generalized learning claim. The caller is responsible for matching task cases,
evaluation conditions, and scoring rubrics. Coding-agent telemetry (Copilot,
Claude, or others) is not collected automatically; integrate a supported
provider export or a separately implemented adapter that maps available
metadata and measurements into this API.