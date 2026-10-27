# Observability: Claude Code agents in MLflow

The Claude Code team on this repo (leader session + `devops-engineer`, `data-engineer`,
`cybersecurity` subagents) logs its token use and context-window fill to a local, open-source
MLflow server. The same server is meant for the project's MLOps work later.

- MLflow UI: http://127.0.0.1:5000 (`make mlflow-ui`). Bound to localhost only.
- Server: `mlflow` service in `docker-compose.yml` (`ghcr.io/mlflow/mlflow:v3.8.1`, SQLite +
  artifacts on the `mlflow-data` volume). Starts with `docker compose up -d` or `make mlflow-up`.
- Collector: `observability/claude_mlflow_hook.py`, run by the hooks in `.claude/settings.json`
  (Stop, SubagentStop, PreCompact, SessionEnd).

## What is tracked

Experiment **`claude-agents`**: counts only. No prompt, reply or tool content.

| Run | Name | Tags |
|---|---|---|
| One parent run per session | `leader-<session id[:8]>` | `session_id`, `agent=leader`, `model`, `git_branch`, `compaction_triggers` |
| One nested child run per subagent | `<role>-<agent id[:8]>`, e.g. `devops-engineer-ae3db425` | `agent_id`, `agent=subagent`, `agent_role` (matched from `.claude/agents/*.md`), `subagent_type`, `description` (the leader's short task label), `model` |

The role comes from the subagent's brief: the hook finds the first agent name from `.claude/agents/` that appears in it. `subagent_type` is often `general-purpose`.

Metrics are logged once per API response. `step` is the response's position in the run, and each value carries the response's timestamp:

| Metric | Meaning |
|---|---|
| `context_tokens` | input + cache_read + cache_creation: how full the context window was for that call |
| `context_pct` | `context_tokens / CLAUDE_CONTEXT_WINDOW` (default 200000) |
| `max_context_pct`, `max_context_tokens` | peak so far |
| `output_tokens` | output of that call |
| `cache_hit_ratio` | cache_read / context_tokens |
| `total_input_tokens` | cumulative **uncached** input tokens |
| `total_cache_read_tokens`, `total_cache_creation_tokens`, `total_output_tokens` | cumulative |
| `total_tokens` | sum of the four cumulative counters |
| `tool_calls` | cumulative tool_use blocks |
| `assistant_messages`, `duration_s` | run summary |
| `team_total_tokens`, `team_total_output_tokens` | leader run only: leader + all subagents |
| `compactions` | cumulative compaction count |
| `context_tokens_before_compaction` | context size when a compaction started |
| `context_tokens_after_compaction`, `context_tokens_lost`, `context_tokens_lost_total` | size at the first call after the compaction, and how much was dropped |

The hook counts a compaction from either source: the PreCompact hook (its trigger, `auto` or `manual`, is stored in the `compaction_triggers` tag) or the `compact_boundary` / `isCompactSummary` markers in the transcript. Seeing both for one compaction still counts it once. Subagent runs are set to FINISHED when the subagent stops. The leader run is set to FINISHED at SessionEnd.

Experiment **`claude-agents-traces`** (optional) holds MLflow's built-in Claude Code conversation trace for each leader turn (`mlflow.claude_code`). **These traces contain the full prompt, reply and tool input/output text**, so anything that appears in the conversation, including secrets, is stored in the local MLflow database.

## Reading it in the UI

1. **Experiments → claude-agents.** Use the table view. Expand a leader run to see its subagents. Add the columns `agent_role`, `total_tokens`, `total_output_tokens`, `max_context_pct` and `compactions`.
2. **How much did each agent use?** Sort the table by `total_tokens`, or open the **Chart** view and plot `total_tokens` as a bar chart across runs, grouped by the `agent_role` tag. On the leader run, `team_total_tokens` shows the whole team. Most input is normally `total_cache_read_tokens`, which is cheap. Compare `total_output_tokens` and `total_cache_creation_tokens` to see real work.
3. **When did we lose context?** Open a run and plot `context_pct` against step or wall time. The line climbs as the conversation grows and drops sharply at each compaction. `context_tokens_lost` marks those drops, and `compactions` counts them. A `max_context_pct` near 1.0 means the agent was close to auto-compaction.
4. **Caching health:** plot `cache_hit_ratio`. A sudden low point means the prompt cache was rebuilt, for example after a compaction or a long pause.

## Operations

- Backfill or catch up on all transcripts of this project: `make agent-metrics-backfill`. It is idempotent: per-transcript offsets and message ids are kept in `observability/.state/` (git-ignored).
- Errors go to `observability/.state/hook.log`. The built-in tracer writes its own log to `.claude/mlflow/claude_tracing.log`.
- Safety: the hook returns in about 30 ms. It passes a whitelisted payload (ids, paths, event name) to a detached worker. The worker has a 60 s hard timeout. If MLflow is unreachable (1.5 s check), the worker skips silently, and the next hook or a backfill catches up.
- Context window: set `CLAUDE_CONTEXT_WINDOW` in `.claude/settings.json` `env`, for example to `1000000` for 1M-context models.

## Disable

- Everything, for one shell: `export CLAUDE_MLFLOW_HOOK_DISABLED=1` before starting `claude`.
- Content traces only: set `"MLFLOW_CLAUDE_TRACING_ENABLED": "false"` in `.claude/settings.json`.
- Permanently: remove the `hooks` block, or only the `--trace` entry, from `.claude/settings.json`.
- Stop the server: `docker compose stop mlflow`. The hooks then do nothing.
- Wipe the data: `docker compose rm -sf mlflow && docker volume rm datalake-kafka_mlflow-data`, and `rm -rf observability/.state`.
