"""Claude Code -> MLflow: token usage and context-window metrics per agent.

Wired in .claude/settings.json for the Stop, SubagentStop, PreCompact and SessionEnd
hooks. See docs/observability.md.

Modes
  (no args)      hook mode: read the hook JSON from stdin, hand it to a detached worker,
                 exit 0 at once with no output. Never blocks or fails Claude Code.
  --trace        same, but the worker also writes the built-in MLflow conversation trace
                 (mlflow.claude_code) for the Stop event into experiment claude-agents-traces.
  --worker FILE  internal: process one queued hook payload.
  --backfill     process every existing transcript of this project (synchronous).

Only counts, ids, model names, timestamps and the short Agent `description` label are
sent to MLflow by this script. Message content is never read into metrics or logs.
Stdlib + mlflow(-skinny) only.
"""

# Hooks must never raise into Claude Code, hence the deliberate blind excepts.
# ruff: noqa: BLE001, S110
from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = Path(
    os.environ.get("CLAUDE_MLFLOW_STATE_DIR", REPO_ROOT / "observability" / ".state")
)
LOG_FILE = STATE_DIR / "hook.log"

EXPERIMENT = os.environ.get("CLAUDE_MLFLOW_EXPERIMENT", "claude-agents")
TRACES_EXPERIMENT = os.environ.get(
    "CLAUDE_MLFLOW_TRACES_EXPERIMENT", "claude-agents-traces"
)
DEFAULT_URI = "http://127.0.0.1:5000"
WORKER_TIMEOUT_S = 60
STALE_SESSION_S = (
    6 * 3600
)  # backfill: leader runs idle longer than this are marked FINISHED

# Only these hook-input keys are ever written to the queue file (Stop input can carry
# message text, e.g. last_assistant_message — never persisted).
PAYLOAD_KEYS = (
    "session_id",
    "transcript_path",
    "hook_event_name",
    "cwd",
    "agent_id",
    "agent_type",
    "agent_transcript_path",
    "trigger",
    "reason",
)
FINAL_EVENTS = {"SessionEnd"}


# --------------------------------------------------------------------------- utils


def log(msg: str) -> None:
    """Append one line to the git-ignored hook log (rotated at ~1 MB). Never raises."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        if LOG_FILE.exists() and LOG_FILE.stat().st_size > 1_000_000:
            LOG_FILE.replace(LOG_FILE.with_suffix(".log.1"))
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(f"{datetime.now().isoformat(timespec='seconds')} {msg}\n")
    except Exception:
        pass


def context_window() -> int:
    try:
        return max(1, int(os.environ.get("CLAUDE_CONTEXT_WINDOW", "200000")))
    except ValueError:
        return 200000


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_URI


def mlflow_reachable(uri: str, timeout: float = 1.5) -> bool:
    if not uri.startswith(("http://", "https://")):
        return True  # file/sqlite stores (tests, local use)
    try:
        with urllib.request.urlopen(uri.rstrip("/") + "/health", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def parse_ts_ms(ts: str | None) -> int | None:
    if not ts:
        return None
    try:
        return int(datetime.fromisoformat(ts).timestamp() * 1000)
    except (ValueError, TypeError):
        return None


def safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value)[:128]


def projects_dir() -> Path:
    base = Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home() / ".claude"))
    return base / "projects" / re.sub(r"[^A-Za-z0-9]", "-", str(REPO_ROOT))


# ------------------------------------------------------------- transcript parsing


def read_new_lines(path: Path, offset: int) -> tuple[list[dict], int]:
    """Parse complete JSONL lines after byte `offset`. Returns (entries, new_offset)."""
    size = path.stat().st_size
    if offset > size:  # file was truncated/rewritten: start over
        offset = 0
    entries: list[dict] = []
    with path.open("rb") as fh:
        fh.seek(offset)
        data = fh.read()
    end = data.rfind(b"\n")
    if end < 0:
        return entries, offset
    for raw in data[: end + 1].splitlines():
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        if isinstance(obj, dict):
            entries.append(obj)
    return entries, offset + end + 1


def extract_events(entries: list[dict]) -> list[dict]:
    """Turn raw transcript entries into ordered events (counts only, no content).

    One API response is written as several entries sharing `message.id` (one per content
    block, usage repeated and output_tokens growing), so they are merged by id.
    Events: {"kind": "message", ...usage...} and {"kind": "compaction", ...}.
    """
    events: list[dict] = []
    by_id: dict[str, dict] = {}
    for e in entries:
        etype = e.get("type")
        if etype == "assistant":
            msg = e.get("message") or {}
            usage = msg.get("usage")
            if not isinstance(usage, dict):
                continue
            mid = msg.get("id") or e.get("uuid")
            content = msg.get("content")
            n_tools = (
                sum(
                    1
                    for b in content
                    if isinstance(b, dict) and b.get("type") == "tool_use"
                )
                if isinstance(content, list)
                else 0
            )
            ev = by_id.get(mid)
            if ev is None:
                ev = {
                    "kind": "message",
                    "id": mid,
                    "ts": parse_ts_ms(e.get("timestamp")),
                    "model": msg.get("model"),
                    "tool_calls": 0,
                    "input": 0,
                    "cache_read": 0,
                    "cache_creation": 0,
                    "output": 0,
                }
                by_id[mid] = ev
                events.append(ev)
            ev["tool_calls"] += n_tools
            ev["input"] = max(ev["input"], int(usage.get("input_tokens") or 0))
            ev["cache_read"] = max(
                ev["cache_read"], int(usage.get("cache_read_input_tokens") or 0)
            )
            ev["cache_creation"] = max(
                ev["cache_creation"], int(usage.get("cache_creation_input_tokens") or 0)
            )
            ev["output"] = max(ev["output"], int(usage.get("output_tokens") or 0))
        elif etype == "system" and e.get("subtype") == "compact_boundary":
            meta = e.get("compactMetadata") or {}
            pre = meta.get("preTokens", meta.get("pre_tokens"))
            events.append(
                {
                    "kind": "compaction",
                    "id": e.get("uuid"),
                    "ts": parse_ts_ms(e.get("timestamp")),
                    "trigger": meta.get("trigger"),
                    "pre_tokens": int(pre) if isinstance(pre, (int, float)) else None,
                }
            )
        elif etype == "user" and e.get("isCompactSummary"):
            # Summary written after a compaction; only counts if no boundary marker precedes it.
            prev = events[-1] if events else None
            if not (prev and prev["kind"] == "compaction"):
                events.append(
                    {
                        "kind": "compaction",
                        "id": e.get("uuid"),
                        "ts": parse_ts_ms(e.get("timestamp")),
                        "trigger": None,
                        "pre_tokens": None,
                    }
                )
    return events


def new_state() -> dict:
    return {
        "offset": 0,
        "step": 0,
        "seen_ids": [],
        "total_input_tokens": 0,
        "total_cache_read_tokens": 0,
        "total_cache_creation_tokens": 0,
        "total_output_tokens": 0,
        "tool_calls": 0,
        "max_context_pct": 0.0,
        "max_context_tokens": 0,
        "last_context_tokens": 0,
        "compactions": 0,
        "context_tokens_lost_total": 0,
        "pending_compaction": None,  # {"before": int, "source": "hook"|"marker", "marker_seen": bool}
        "triggers": [],
        "first_ts": None,
        "last_ts": None,
        "model": None,
    }


def apply_events(
    state: dict, events: list[dict], window: int
) -> list[tuple[str, float, int, int]]:
    """Update `state` with new events; return metrics as (key, value, ts_ms, step)."""
    out: list[tuple[str, float, int, int]] = []
    seen = set(state["seen_ids"])
    for ev in events:
        if ev.get("id") and ev["id"] in seen:
            continue
        if ev.get("id"):
            seen.add(ev["id"])
            state["seen_ids"].append(ev["id"])
        ts = ev.get("ts") or int(time.time() * 1000)
        state["first_ts"] = state["first_ts"] or ts
        state["last_ts"] = max(state["last_ts"] or ts, ts)

        if ev["kind"] == "compaction":
            pend = state["pending_compaction"]
            if pend and pend["source"] == "hook" and not pend.get("marker_seen"):
                pend["marker_seen"] = (
                    True  # same compaction the PreCompact hook already counted
                )
                if ev.get("trigger") and ev["trigger"] not in state["triggers"]:
                    state["triggers"].append(ev["trigger"])
                continue
            out += record_compaction(
                state, ev.get("pre_tokens"), ev.get("trigger"), "marker", ts
            )
            continue

        state["step"] += 1
        step = state["step"]
        state["model"] = state["model"] or ev.get("model")
        ctx = ev["input"] + ev["cache_read"] + ev["cache_creation"]
        pct = ctx / window
        state["total_input_tokens"] += ev["input"]
        state["total_cache_read_tokens"] += ev["cache_read"]
        state["total_cache_creation_tokens"] += ev["cache_creation"]
        state["total_output_tokens"] += ev["output"]
        state["tool_calls"] += ev["tool_calls"]
        state["max_context_pct"] = max(state["max_context_pct"], pct)
        state["max_context_tokens"] = max(state["max_context_tokens"], ctx)
        state["last_context_tokens"] = ctx
        total = (
            state["total_input_tokens"]
            + state["total_cache_read_tokens"]
            + state["total_cache_creation_tokens"]
            + state["total_output_tokens"]
        )
        out += [
            ("context_tokens", ctx, ts, step),
            ("context_pct", round(pct, 6), ts, step),
            ("output_tokens", ev["output"], ts, step),
            (
                "cache_hit_ratio",
                round(ev["cache_read"] / ctx, 6) if ctx else 0.0,
                ts,
                step,
            ),
            ("total_input_tokens", state["total_input_tokens"], ts, step),
            ("total_cache_read_tokens", state["total_cache_read_tokens"], ts, step),
            (
                "total_cache_creation_tokens",
                state["total_cache_creation_tokens"],
                ts,
                step,
            ),
            ("total_output_tokens", state["total_output_tokens"], ts, step),
            ("total_tokens", total, ts, step),
            ("tool_calls", state["tool_calls"], ts, step),
            ("max_context_pct", round(state["max_context_pct"], 6), ts, step),
        ]
        pend = state["pending_compaction"]
        if pend:
            lost = max(0, pend["before"] - ctx)
            state["context_tokens_lost_total"] += lost
            out += [
                ("context_tokens_after_compaction", ctx, ts, step),
                ("context_tokens_lost", lost, ts, step),
                (
                    "context_tokens_lost_total",
                    state["context_tokens_lost_total"],
                    ts,
                    step,
                ),
            ]
            state["pending_compaction"] = None
    state["seen_ids"] = state["seen_ids"][-2000:]
    return out


def record_compaction(
    state: dict, pre_tokens: int | None, trigger: str | None, source: str, ts: int
) -> list[tuple[str, float, int, int]]:
    before = pre_tokens if pre_tokens is not None else state["last_context_tokens"]
    state["compactions"] += 1
    state["pending_compaction"] = {
        "before": int(before),
        "source": source,
        "marker_seen": source == "marker",
    }
    if trigger and trigger not in state["triggers"]:
        state["triggers"].append(trigger)
    step = state["step"]
    return [
        ("compactions", state["compactions"], ts, step),
        ("context_tokens_before_compaction", int(before), ts, step),
    ]


def on_precompact(
    state: dict, trigger: str | None
) -> list[tuple[str, float, int, int]]:
    """PreCompact hook. If the transcript already showed this compaction (late worker), don't recount."""
    pend = state["pending_compaction"]
    if pend and pend["source"] == "marker":
        if trigger and trigger not in state["triggers"]:
            state["triggers"].append(trigger)
        return []
    return record_compaction(state, None, trigger, "hook", int(time.time() * 1000))


def summary_metrics(state: dict) -> list[tuple[str, float, int, int]]:
    ts = state["last_ts"] or int(time.time() * 1000)
    step = state["step"]
    dur = (
        ((state["last_ts"] or 0) - (state["first_ts"] or 0)) / 1000
        if state["first_ts"]
        else 0
    )
    return [
        ("assistant_messages", step, ts, step),
        ("max_context_tokens", state["max_context_tokens"], ts, step),
        ("duration_s", round(dur, 1), ts, step),
    ]


# ------------------------------------------------------------------- MLflow side


class Tracker:
    def __init__(self, uri: str):
        from mlflow import MlflowClient

        self.client = MlflowClient(tracking_uri=uri)
        exp = self.client.get_experiment_by_name(EXPERIMENT)
        self.exp_id = (
            exp.experiment_id if exp else self.client.create_experiment(EXPERIMENT)
        )

    def find_run(self, tags: dict[str, str]):
        flt = " and ".join(f"tags.{k} = '{v}'" for k, v in tags.items())
        runs = self.client.search_runs([self.exp_id], filter_string=flt, max_results=1)
        return runs[0] if runs else None

    def get_or_create(
        self, key_tags: dict, extra_tags: dict, run_name: str, start_ms: int | None
    ):
        run = self.find_run(key_tags)
        if run:
            missing = {
                k: v for k, v in extra_tags.items() if v and run.data.tags.get(k) != v
            }
            for k, v in missing.items():
                self.client.set_tag(run.info.run_id, k, v)
            return run.info.run_id
        tags = {**key_tags, **{k: v for k, v in extra_tags.items() if v}}
        run = self.client.create_run(
            self.exp_id, start_time=start_ms, tags=tags, run_name=run_name
        )
        return run.info.run_id

    def log(self, run_id: str, metrics: list[tuple[str, float, int, int]]) -> None:
        from mlflow.entities import Metric

        ms = [Metric(k, float(v), int(ts), int(step)) for k, v, ts, step in metrics]
        for i in range(0, len(ms), 900):
            self.client.log_batch(run_id, metrics=ms[i : i + 900])

    def finish(self, run_id: str, end_ms: int | None) -> None:
        self.client.set_terminated(run_id, "FINISHED", end_time=end_ms)


def load_state(path: Path) -> dict:
    try:
        st = json.loads(path.read_text())
        base = new_state()
        base.update(st)
        return base
    except Exception:
        return new_state()


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(path)


def leader_meta(entries: list[dict]) -> dict:
    """Map Agent tool_use_id -> subagent_type, agentId -> tool_use_id; git branch."""
    out = {"types": {}, "agent_tool": {}, "branch": None, "descriptions": {}}
    for e in entries:
        out["branch"] = out["branch"] or e.get("gitBranch")
        msg = e.get("message") or {}
        content = msg.get("content")
        if e.get("type") == "assistant" and isinstance(content, list):
            for b in content:
                if (
                    isinstance(b, dict)
                    and b.get("type") == "tool_use"
                    and b.get("name") in ("Agent", "Task")
                ):
                    inp = b.get("input") or {}
                    out["types"][b.get("id")] = inp.get("subagent_type")
        tur = e.get("toolUseResult")
        if (
            e.get("type") == "user"
            and isinstance(tur, dict)
            and tur.get("agentId")
            and isinstance(content, list)
        ):
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    out["agent_tool"][tur["agentId"]] = b.get("tool_use_id")
            if isinstance(tur.get("description"), str):
                out["descriptions"][tur["agentId"]] = tur["description"][:200]
    return out


def agent_roles() -> list[str]:
    d = REPO_ROOT / ".claude" / "agents"
    return sorted(p.stem for p in d.glob("*.md")) if d.is_dir() else []


def detect_role(path: Path, roles: list[str]) -> str | None:
    """Which project agent (.claude/agents/*.md) the subagent was briefed as.

    Scans the first user prompt for the role names; only the matched name is kept.
    """
    if not roles:
        return None
    try:
        with path.open(encoding="utf-8") as fh:
            for _ in range(20):
                line = fh.readline()
                if not line:
                    break
                e = json.loads(line)
                if e.get("type") != "user":
                    continue
                c = (e.get("message") or {}).get("content")
                text = (
                    c
                    if isinstance(c, str)
                    else " ".join(
                        b.get("text", "")
                        for b in c or []
                        if isinstance(b, dict) and b.get("type") == "text"
                    )
                )
                head = text[:3000]
                hits = [
                    (m.start(), role)
                    for role in roles
                    if (m := re.search(re.escape(role), head))
                ]
                return min(hits)[1] if hits else None
    except Exception:
        return None
    return None


def process_session(
    tracker: Tracker,
    session_id: str,
    transcript: Path,
    event: str | None = None,
    trigger: str | None = None,
    finish_agent_id: str | None = None,
    finish_leader: bool = False,
) -> dict:
    """Incrementally log the leader transcript and all its subagent transcripts."""
    window = context_window()
    sdir = STATE_DIR / safe_id(session_id)
    stats = {"runs": 0, "metrics": 0}

    # Leader (re-read from 0 for metadata only: Agent tool_use -> subagent_type).
    all_entries, _ = read_new_lines(transcript, 0)
    meta = leader_meta(all_entries)
    lpath = sdir / "leader.json"
    lstate = load_state(lpath)
    entries, new_off = read_new_lines(transcript, lstate["offset"])
    metrics = apply_events(lstate, extract_events(entries), window)
    if event == "PreCompact":
        metrics += on_precompact(lstate, trigger)
    lstate["offset"] = new_off
    leader_id = tracker.get_or_create(
        {"session_id": session_id, "agent": "leader"},
        {
            "model": lstate["model"] or "",
            "git_branch": meta["branch"] or "",
            "compaction_triggers": ",".join(lstate["triggers"]),
        },
        f"leader-{session_id[:8]}",
        lstate["first_ts"],
    )
    stats["runs"] += 1

    # Subagents
    team = {
        k: lstate[k]
        for k in (
            "total_input_tokens",
            "total_cache_read_tokens",
            "total_cache_creation_tokens",
            "total_output_tokens",
        )
    }
    subdir = transcript.with_suffix("") / "subagents"
    roles = agent_roles()
    for apath in sorted(subdir.glob("agent-*.jsonl")) if subdir.is_dir() else []:
        agent_id = apath.stem.removeprefix("agent-")
        am = {}
        mpath = apath.with_suffix(".meta.json")
        if mpath.exists():
            try:
                am = json.loads(mpath.read_text())
            except Exception:
                am = {}
        spath = sdir / f"agent-{safe_id(agent_id)}.json"
        st = load_state(spath)
        ents, off = read_new_lines(apath, st["offset"])
        sm = apply_events(st, extract_events(ents), window)
        st["offset"] = off
        if "role" not in st:
            st["role"] = detect_role(apath, roles)
        tool_id = am.get("toolUseId") or meta["agent_tool"].get(agent_id)
        sub_type = am.get("agentType") or meta["types"].get(tool_id) or ""
        desc = (am.get("description") or meta["descriptions"].get(agent_id) or "")[:200]
        label = st.get("role") or sub_type or "subagent"
        run_id = tracker.get_or_create(
            {"session_id": session_id, "agent_id": safe_id(agent_id)},
            {
                "agent": "subagent",
                "mlflow.parentRunId": leader_id,
                "subagent_type": sub_type,
                "agent_role": st.get("role") or "",
                "description": desc,
                "model": st["model"] or "",
            },
            f"{label}-{agent_id[:8]}",
            st["first_ts"],
        )
        if sm:
            tracker.log(run_id, sm + summary_metrics(st))
            stats["metrics"] += len(sm)
        if (am.get("finished") is True or finish_agent_id == agent_id) and not st.get(
            "finished"
        ):
            tracker.finish(run_id, st["last_ts"])
            st["finished"] = True
        stats["runs"] += 1
        for k in team:
            team[k] += st[k]
        save_state(spath, st)

    team_total = sum(team.values())
    if metrics or stats["metrics"]:
        ts, step = lstate["last_ts"] or int(time.time() * 1000), lstate["step"]
        metrics += summary_metrics(lstate) + [
            ("team_total_tokens", team_total, ts, step),
            ("team_total_output_tokens", team["total_output_tokens"], ts, step),
        ]
        tracker.log(leader_id, metrics)
        stats["metrics"] += len(metrics)
    if lstate["triggers"]:
        tracker.client.set_tag(
            leader_id, "compaction_triggers", ",".join(lstate["triggers"])
        )
    if finish_leader and not lstate.get("finished"):
        tracker.finish(leader_id, lstate["last_ts"])
        lstate["finished"] = True
    save_state(lpath, lstate)
    stats.update(leader_run_id=leader_id, leader=lstate, team_total_tokens=team_total)
    return stats


# --------------------------------------------------------------------- entrypoints


def resolve_transcript(payload: dict) -> tuple[str, Path] | None:
    sid = payload.get("session_id")
    tp = payload.get("transcript_path")
    if not sid:
        return None
    path = Path(tp).expanduser() if tp else projects_dir() / f"{sid}.jsonl"
    if "/subagents/" in str(path):  # always key off the leader transcript
        path = path.parent.parent.with_suffix(".jsonl")
    return (sid, path) if path.exists() else None


def builtin_trace(payload: dict) -> None:
    """Built-in MLflow conversation trace (full prompt/tool content) into TRACES_EXPERIMENT."""
    if os.environ.get("MLFLOW_CLAUDE_TRACING_ENABLED", "").lower() != "true":
        return
    if payload.get("hook_event_name") not in (None, "Stop"):
        return
    os.environ["MLFLOW_EXPERIMENT_NAME"] = TRACES_EXPERIMENT
    os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
    from mlflow.claude_code.tracing import process_transcript, setup_mlflow

    setup_mlflow()
    if (
        process_transcript(payload.get("transcript_path"), payload.get("session_id"))
        is None
    ):
        log(
            "trace: built-in tracer returned no trace (see .claude/mlflow/claude_tracing.log)"
        )


def run_worker(payload_file: Path, trace: bool) -> int:
    def _timeout(*_):
        raise TimeoutError(f"worker exceeded {WORKER_TIMEOUT_S}s")

    signal.signal(signal.SIGALRM, _timeout)
    signal.alarm(WORKER_TIMEOUT_S)
    try:
        payload = json.loads(payload_file.read_text())
    except Exception as exc:
        log(f"worker: bad payload: {type(exc).__name__}")
        return 0
    finally:
        try:
            payload_file.unlink()
        except OSError:
            pass
    event = payload.get("hook_event_name")
    try:
        uri = tracking_uri()
        if not mlflow_reachable(uri):
            return 0  # MLflow down: skip silently; state is untouched so the next hook catches up
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "10")
        os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "1")
        if trace:
            builtin_trace(payload)
            return 0
        resolved = resolve_transcript(payload)
        if not resolved:
            return 0
        sid, path = resolved
        import fcntl

        lockp = STATE_DIR / safe_id(sid) / ".lock"
        lockp.parent.mkdir(parents=True, exist_ok=True)
        with lockp.open("w") as lk:
            fcntl.flock(
                lk, fcntl.LOCK_EX
            )  # serialise workers of one session (alarm bounds the wait)
            process_session(
                Tracker(uri),
                sid,
                path,
                event=event,
                trigger=payload.get("trigger"),
                finish_agent_id=payload.get("agent_id")
                if event == "SubagentStop"
                else None,
                finish_leader=event in FINAL_EVENTS,
            )
    except Exception as exc:
        log(f"worker[{event}]: {type(exc).__name__}: {str(exc)[:300]}")
    finally:
        signal.alarm(0)
    return 0


def run_hook(trace: bool) -> int:
    """Read stdin, queue a whitelisted payload, spawn a detached worker, return immediately."""
    try:
        if os.environ.get("CLAUDE_MLFLOW_HOOK_DISABLED") == "1":
            return 0
        raw = sys.stdin.read(1_000_000)
        data = json.loads(raw) if raw.strip() else {}
        payload = {k: data[k] for k in PAYLOAD_KEYS if isinstance(data.get(k), str)}
        qdir = STATE_DIR / "queue"
        qdir.mkdir(parents=True, exist_ok=True)
        fd, qpath = tempfile.mkstemp(prefix="payload-", suffix=".json", dir=qdir)
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh)
        args = [sys.executable, str(Path(__file__).resolve()), "--worker", qpath]
        if trace:
            args.append("--trace")
        subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
            cwd=str(REPO_ROOT),
        )
    except Exception as exc:
        log(f"hook: {type(exc).__name__}: {str(exc)[:300]}")
    return 0


def run_backfill() -> int:
    uri = tracking_uri()
    if not mlflow_reachable(uri, timeout=3):
        print(f"MLflow not reachable at {uri} (start it: docker compose up -d mlflow)")
        return 1
    tracker = Tracker(uri)
    pdir = projects_dir()
    sessions = sorted(pdir.glob("*.jsonl"))
    total_runs = 0
    for path in sessions:
        sid = path.stem
        stale = time.time() - path.stat().st_mtime > STALE_SESSION_S
        st = process_session(tracker, sid, path, finish_leader=stale)
        total_runs += st["runs"]
        lead = st["leader"]
        print(
            f"session {sid[:8]}: runs={st['runs']} new_metrics={st['metrics']} "
            f"leader_total_tokens={lead['total_input_tokens'] + lead['total_cache_read_tokens'] + lead['total_cache_creation_tokens'] + lead['total_output_tokens']} "
            f"leader_max_context_pct={lead['max_context_pct']:.3f} compactions={lead['compactions']} "
            f"team_total_tokens={st['team_total_tokens']}"
        )
    print(
        f"sessions={len(sessions)} runs={total_runs} experiment={EXPERIMENT} uri={uri}"
    )
    return 0


def main(argv: list[str]) -> int:
    if "--backfill" in argv:
        return run_backfill()
    if "--worker" in argv:
        i = argv.index("--worker")
        return run_worker(Path(argv[i + 1]), trace="--trace" in argv)
    return run_hook(trace="--trace" in argv)


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except SystemExit:
        raise
    except Exception as exc:  # never fail a hook
        log(f"main: {type(exc).__name__}: {str(exc)[:300]}")
        sys.exit(0)
