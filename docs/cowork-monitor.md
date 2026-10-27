# Cowork: local AI observatory

Run from the repository root:

```sh
uv run python -m datalake_duckdb.cowork
```

Open http://127.0.0.1:8799. The page refreshes every three seconds. Stop the
server with Ctrl+C. This is a local application; nothing is published or sent to
an external tracking service. It requires no additional Python dependencies.

## Available now

- Recognized executable detection for ChatGPT, Codex, Claude, Gemini, Ollama,
  and LM Studio, with PID, process CPU percentage and resident memory.
- Provider-neutral `CoworkMonitor` class with validated per-response usage events.
- SQLite persistence and event-ID deduplication across restarts.
- Per-session token budget, trailing 60-second usage and context-pressure alerts.
- Classroom-style cowork room with an animated desk for each detected application
  and each reported model session. Helper processes share an application desk.
- Clickable desk inspector, application/session/alert filters, and a room journal.
- Responsive layout, reduced-motion support, explicit unknown states and telemetry age.

Moving hands indicate at least 1% aggregate process CPU or usage reported in the
last 60 seconds. They do not claim that a model is currently generating. Session
seats fade after 60 seconds without telemetry. The room journal tracks observed
arrivals, departures and new alert changes while the page is open; it is not a
persistent task or tool-action history.

Executable matching is best effort. Renamed or wrapped executables and browser
chats may be missed. Process presence does not mean a model is generating.
Process CPU is the operating system's reported percentage, not a sampled
inference-activity measurement. Tool actions, task progress and generation
state are not collected by this version.

## Feed usage from your code or an adapter

The dashboard reads the SQLite database on every refresh. Another Python
process can report events to the same database while it is open:

```python
from datalake_duckdb.cowork import CoworkMonitor

monitor = CoworkMonitor('observability/.state/cowork.sqlite')
monitor.record_usage({
    'event_id': 'example-provider:response-001',
    'session_id': 'session-001',
    'provider': 'example-provider',
    'model': 'example-model',
    'input_tokens': 1200,
    'output_tokens': 240,
    'context_tokens': 1200,
    'context_window': 32000,
})
monitor.db.close()
```

Use opaque identifiers and actual measured usage. Each event is a **delta for
one response**, not a cumulative session counter. Namespace event IDs across
providers. Replaying an ID is ignored; an existing ID is not revised. Adapters
must wait for final usage before submitting an event. Missing counts default to
zero; adapters should supply both input and output to avoid undercounting.
Input accounting, including cached tokens, must be consistent within an adapter.
A provider/model change creates a separate session group for thresholds.
Context size/window are optional and default to unknown. Token totals include
all imported history, while burn-rate alerts use only the last 60 seconds.
Timestamp is optional Unix seconds; historical events are accepted, future
events beyond five seconds and invalid numbers are rejected.

Alternatively import JSONL (one such object per line) while a server is running:

```sh
uv run python -m datalake_duckdb.cowork --import-events /path/to/usage.jsonl --import-only
```

Imports commit each event independently; on a malformed event fix the file and
reimport it. Already imported IDs will be ignored.

Set thresholds when launching the server:

```sh
uv run python -m datalake_duckdb.cowork --token-budget 100000 --tokens-per-minute 10000
```

Alerts are advisory visual indicators. They do not stop models, enforce budgets
or produce desktop notifications. Cached token pricing and subscription quotas
are not inferred; reported token volume is not a dollar cost estimate.

## Next integrations

Automatic Claude Code and Codex telemetry adapters, correlation of processes
with sessions, action metadata, stall/repeated-tool detection, provider-specific
cost accounting and opt-in desktop notifications remain future work. The
existing Claude MLflow hook remains independent; it does not yet feed Cowork.
ChatGPT desktop/browser generation and subscription remaining quotas require a
supported telemetry source; an open application alone supplies neither.

The server is read-only HTTP, binds to 127.0.0.1 and rejects unexpected Host
headers and cross-site API requests. It has no network event-ingestion endpoint.
No prompts, replies, command arguments or API credentials are accepted by the
usage schema. Do not put private content into identifier fields. Local metadata
is stored in `observability/.state/cowork.sqlite` (ignored by Git). The monitor
runs while its Python server runs; login startup is not installed.
