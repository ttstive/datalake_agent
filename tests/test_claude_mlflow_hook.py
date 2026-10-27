"""Tests for observability/claude_mlflow_hook.py using small synthetic transcripts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "claude_mlflow_hook",
    Path(__file__).resolve().parent.parent / "observability" / "claude_mlflow_hook.py",
)
hook = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hook)

WINDOW = 1000


def assistant(mid, inp, cr, cc, out, blocks=("text",), ts="2026-10-27T10:00:00.000Z"):
    return {
        "type": "assistant",
        "uuid": f"u-{mid}-{out}-{len(blocks)}",
        "timestamp": ts,
        "message": {
            "id": mid,
            "model": "claude-test",
            "content": [{"type": b} for b in blocks],
            "usage": {
                "input_tokens": inp,
                "cache_read_input_tokens": cr,
                "cache_creation_input_tokens": cc,
                "output_tokens": out,
            },
        },
    }


def metrics_dict(out):
    d = {}
    for key, value, _ts, step in out:
        d.setdefault(key, []).append((step, value))
    return d


def write_jsonl(path: Path, entries):
    path.write_text("".join(json.dumps(e) + "\n" for e in entries))


def test_split_entries_are_merged_by_message_id():
    entries = [
        assistant("m1", 10, 100, 40, 5, ("thinking",)),
        assistant("m1", 10, 100, 40, 50, ("tool_use", "tool_use")),
        {"type": "user", "message": {"content": []}},
        assistant("m2", 2, 300, 0, 7),
    ]
    events = hook.extract_events(entries)
    assert [e["id"] for e in events] == ["m1", "m2"]
    assert events[0]["output"] == 50 and events[0]["tool_calls"] == 2


def test_metric_math():
    state = hook.new_state()
    events = hook.extract_events(
        [assistant("m1", 10, 100, 40, 50, ("tool_use",)), assistant("m2", 2, 300, 0, 7)]
    )
    m = metrics_dict(hook.apply_events(state, events, WINDOW))
    assert m["context_tokens"] == [(1, 150), (2, 302)]
    assert m["context_pct"] == [(1, 0.15), (2, 0.302)]
    assert m["cache_hit_ratio"][1] == (2, round(300 / 302, 6))
    assert m["total_input_tokens"][-1] == (2, 12)
    assert m["total_cache_read_tokens"][-1] == (2, 400)
    assert m["total_output_tokens"][-1] == (2, 57)
    assert m["total_tokens"][-1] == (2, 12 + 400 + 40 + 57)
    assert m["tool_calls"][-1] == (2, 1)
    assert m["max_context_pct"][-1] == (2, 0.302)


def test_idempotent_by_message_id():
    state = hook.new_state()
    events = hook.extract_events([assistant("m1", 1, 1, 1, 1)])
    assert hook.apply_events(state, events, WINDOW)
    assert hook.apply_events(state, events, WINDOW) == []
    assert state["step"] == 1


def test_incremental_read_skips_partial_line(tmp_path):
    p = tmp_path / "t.jsonl"
    write_jsonl(p, [assistant("m1", 1, 1, 1, 1)])
    with p.open("a") as fh:
        fh.write('{"type": "assist')  # line still being written
    entries, off = hook.read_new_lines(p, 0)
    assert len(entries) == 1
    with p.open("a") as fh:
        fh.write('ant"}\n')
    entries2, off2 = hook.read_new_lines(p, off)
    assert len(entries2) == 1 and off2 == p.stat().st_size


def test_compaction_from_transcript_marker():
    state = hook.new_state()
    entries = [
        assistant("m1", 0, 800, 0, 10),
        {
            "type": "system",
            "subtype": "compact_boundary",
            "uuid": "c1",
            "timestamp": "2026-10-27T10:01:00Z",
            "compactMetadata": {"trigger": "auto", "preTokens": 820},
        },
        {
            "type": "user",
            "isCompactSummary": True,
            "uuid": "s1",
            "message": {"content": "x"},
        },
        assistant("m2", 0, 0, 120, 10),
    ]
    m = metrics_dict(hook.apply_events(state, hook.extract_events(entries), WINDOW))
    assert m["compactions"] == [(1, 1)]
    assert m["context_tokens_before_compaction"] == [(1, 820)]
    assert m["context_tokens_after_compaction"] == [(2, 120)]
    assert m["context_tokens_lost"] == [(2, 700)]
    assert state["triggers"] == ["auto"]


def test_compact_summary_without_boundary_counts_once():
    entries = [
        assistant("m1", 0, 500, 0, 1),
        {
            "type": "user",
            "isCompactSummary": True,
            "uuid": "s1",
            "message": {"content": "x"},
        },
    ]
    kinds = [e["kind"] for e in hook.extract_events(entries)]
    assert kinds == ["message", "compaction"]


def test_precompact_hook_then_marker_not_double_counted():
    state = hook.new_state()
    hook.apply_events(
        state, hook.extract_events([assistant("m1", 0, 900, 0, 1)]), WINDOW
    )
    m1 = metrics_dict(hook.on_precompact(state, "manual"))
    assert (
        m1["compactions"][0][1] == 1
        and m1["context_tokens_before_compaction"][0][1] == 900
    )
    later = [
        {
            "type": "system",
            "subtype": "compact_boundary",
            "uuid": "c1",
            "compactMetadata": {"trigger": "manual"},
        },
        assistant("m2", 0, 100, 0, 1),
    ]
    m2 = metrics_dict(hook.apply_events(state, hook.extract_events(later), WINDOW))
    assert "compactions" not in m2
    assert m2["context_tokens_lost"] == [(2, 800)]
    assert state["compactions"] == 1 and state["triggers"] == ["manual"]


def test_marker_seen_before_late_precompact_hook():
    state = hook.new_state()
    entries = [
        assistant("m1", 0, 900, 0, 1),
        {
            "type": "system",
            "subtype": "compact_boundary",
            "uuid": "c1",
            "compactMetadata": {},
        },
    ]
    hook.apply_events(state, hook.extract_events(entries), WINDOW)
    assert hook.on_precompact(state, "auto") == []
    assert state["compactions"] == 1


def test_payload_whitelist_never_persists_content(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "STATE_DIR", tmp_path)
    spawned = {}
    monkeypatch.setattr(
        hook.subprocess, "Popen", lambda args, **kw: spawned.setdefault("args", args)
    )
    payload = {
        "session_id": "s",
        "hook_event_name": "Stop",
        "last_assistant_message": "SECRET",
    }
    monkeypatch.setattr(
        hook.sys, "stdin", __import__("io").StringIO(json.dumps(payload))
    )
    assert hook.run_hook(trace=False) == 0
    qfile = Path(spawned["args"][3])
    assert json.loads(qfile.read_text()) == {
        "session_id": "s",
        "hook_event_name": "Stop",
    }


def test_hook_never_raises_on_garbage(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(hook, "STATE_DIR", tmp_path)
    monkeypatch.setattr(hook, "LOG_FILE", tmp_path / "hook.log")
    monkeypatch.setattr(hook.sys, "stdin", __import__("io").StringIO("not json"))
    assert hook.run_hook(trace=False) == 0
    assert capsys.readouterr().out == ""


class FakeTracker:
    def __init__(self):
        self.runs, self.logged, self.finished = {}, {}, set()
        self.client = self

    def get_or_create(self, key_tags, extra_tags, run_name, start_ms):
        key = tuple(sorted(key_tags.items()))
        if key not in self.runs:
            self.runs[key] = (
                f"run{len(self.runs)}",
                run_name,
                {**key_tags, **extra_tags},
            )
        return self.runs[key][0]

    def log(self, run_id, metrics):
        self.logged.setdefault(run_id, []).extend(metrics)

    def finish(self, run_id, end_ms):
        self.finished.add(run_id)

    def set_tag(self, *a):
        pass


def test_process_session_leader_and_subagent(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "STATE_DIR", tmp_path / "state")
    sid = "abcdef12-0000"
    leader = tmp_path / f"{sid}.jsonl"
    write_jsonl(
        leader,
        [
            {"type": "user", "gitBranch": "main", "message": {"content": "hi"}},
            assistant("L1", 1, 100, 0, 5, ("tool_use",)),
        ],
    )
    sub = tmp_path / sid / "subagents"
    sub.mkdir(parents=True)
    write_jsonl(
        sub / "agent-a123456789.jsonl",
        [
            {
                "type": "user",
                "message": {"content": "You are acting as the devops-engineer"},
            },
            assistant("S1", 1, 50, 10, 3),
        ],
    )
    (sub / "agent-a123456789.meta.json").write_text(
        json.dumps(
            {
                "agentType": "general-purpose",
                "description": "Set up infra",
                "finished": True,
            }
        )
    )
    monkeypatch.setattr(
        hook,
        "agent_roles",
        lambda: ["cybersecurity", "data-engineer", "devops-engineer"],
    )

    t = FakeTracker()
    stats = hook.process_session(t, sid, leader)
    assert stats["runs"] == 2
    names = {v[1]: v[2] for v in t.runs.values()}
    assert "leader-abcdef12" in names and "devops-engineer-a1234567" in names
    sub_tags = names["devops-engineer-a1234567"]
    assert (
        sub_tags["mlflow.parentRunId"] == "run0"
        and sub_tags["description"] == "Set up infra"
    )
    assert "run1" in t.finished and "run0" not in t.finished
    assert stats["team_total_tokens"] == (1 + 100 + 5) + (1 + 50 + 10 + 3)

    # second call with nothing new logs nothing
    t.logged.clear()
    hook.process_session(t, sid, leader)
    assert t.logged == {}


@pytest.mark.parametrize(
    "ts,expected",
    [("2026-10-27T10:00:00.000Z", 1793095200000), (None, None), ("bad", None)],
)
def test_parse_ts(ts, expected):
    assert hook.parse_ts_ms(ts) == expected
