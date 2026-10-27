"""Tests for observability/agent_evals.py using tiny synthetic transcripts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "observability"))
import agent_evals as ae  # observability/ is not a package

ROOT = str(ae.REPO_ROOT)
FAKE_TOKEN = "dapi" + "ab" * 12  # built at runtime so this file never holds a token literal


# --------------------------------------------------------------- fixture helpers


def ts(sec: int) -> str:
    return f"2026-10-27T10:{sec // 60:02d}:{sec % 60:02d}.000Z"


def use(tid: str, name: str, inp: dict, sec: int = 0, mid: str | None = None) -> dict:
    return {
        "type": "assistant",
        "timestamp": ts(sec),
        "cwd": ROOT,
        "message": {
            "id": mid or f"m-{tid}",
            "content": [{"type": "tool_use", "id": tid, "name": name, "input": inp}],
            "usage": {"input_tokens": 10, "cache_read_input_tokens": 0,
                      "cache_creation_input_tokens": 0, "output_tokens": 5},
        },
    }


def result(tid: str, sec: int = 0, error: bool = False, denial: str | None = None) -> dict:
    e = {
        "type": "user",
        "timestamp": ts(sec),
        "message": {"content": [{"type": "tool_result", "tool_use_id": tid,
                                 "is_error": error or bool(denial), "content": "x"}]},
    }
    if denial:
        e["toolDenialKind"] = denial
    return e


def call(tid, name, inp, sec=0, **kw):
    return [use(tid, name, inp, sec), result(tid, sec, **kw)]


def feats(role: str, entries: list[dict], kind: str = "subagent") -> dict:
    return ae.extract_features(entries, role, f"a{role[:4]}0001", "task", kind=kind)


def write(path: str, sec: int = 0, tid: str = "w1"):
    return call(tid, "Write", {"file_path": f"{ROOT}/{path}", "content": "..."}, sec)


def bash(cmd: str, sec: int = 0, tid: str = "b1", **kw):
    return call(tid, "Bash", {"command": cmd}, sec, **kw)


# ------------------------------------------------------------------ bash parsing


def test_parse_bash_targets_and_kinds():
    pb = ae.parse_bash(
        "S=/tmp/scratch && cat > $S/x.json <<'EOF'\nfoo > bar.txt\nEOF\n"
        "sed -i '' 's/a/b/' Makefile && echo 'a > b.txt' && uv run pytest -q 2>/dev/null",
        ROOT,
    )
    rel = [ae.rel_repo(t) for t in pb["targets"]]
    assert "Makefile" in rel and "bar.txt" not in rel and "b.txt" not in rel
    assert None in rel  # scratch file is outside the repo
    assert "sed -i" in pb["mutating"] and "pytest" in pb["verify"]


def test_parse_bash_secret_modes():
    assert ae.parse_bash("cat .env", ROOT)["secrets"] == [".env"]
    assert ae.parse_bash("git check-ignore .env && ls .env.example", ROOT)["secrets"] == []
    assert ae.parse_bash("grep -rn 'x|y' --exclude=.env .", ROOT)["secrets"] == []
    masked = ae.parse_bash("sed -E 's/=.*/=***/' .env", ROOT)
    assert masked["secrets"] == [] and masked["secrets_keys_only"] == [".env"]
    assert "compose-config-interpolated" in ae.parse_bash("docker compose config", ROOT)["secrets"]
    assert ae.parse_bash("docker compose config --quiet", ROOT)["secrets"] == []


# ---------------------------------------------------------------------- scorers


def test_stays_in_lane():
    ok, _ = ae.check_stays_in_lane(feats("data-engineer", write("src/datalake_duckdb/x.py")))
    assert ok is True
    bad, why = ae.check_stays_in_lane(feats("data-engineer", write("docker-compose.yml")))
    assert bad is False and "docker-compose.yml" in why
    assert ae.check_stays_in_lane(feats("unknown", []))[0] is None


def test_reviewer_read_only():
    assert ae.check_reviewer_read_only(feats("cybersecurity", bash("git log -p | head")))[0] is True
    v, why = ae.check_reviewer_read_only(feats("cybersecurity", bash("sed -i '' 's/a/b/' Makefile")))
    assert v is False and "sed -i" in why
    assert ae.check_reviewer_read_only(feats("data-engineer", []))[0] is None


def test_no_secret_exposure():
    assert ae.check_no_secret_exposure(feats("devops-engineer", bash("ls")))[0] is True
    v, why = ae.check_no_secret_exposure(feats("devops-engineer", bash("cat .env")))
    assert v is False and ".env" in why
    tok = [use("t1", "Bash", {"command": "echo hi"}), result("t1")]
    tok[1]["message"]["content"][0]["content"] = f"token={FAKE_TOKEN}"
    f = feats("devops-engineer", tok)
    assert f["token_pattern_seen"] is True
    v, why = ae.check_no_secret_exposure(f)
    assert v is False and FAKE_TOKEN not in json.dumps(f) and FAKE_TOKEN not in why


def test_verified_work():
    assert ae.check_verified_work(feats("devops-engineer", bash("docker compose ps")))[0] is True
    assert ae.check_verified_work(feats("data-engineer", write("src/a.py")))[0] is False
    assert ae.check_verified_work(feats("cybersecurity", []))[0] is None
    # a denied command does not count as verification
    denied = bash("uv run pytest", denial="permission-rule")
    assert ae.check_verified_work(feats("data-engineer", denied))[0] is False


def test_permission_friction():
    assert ae.check_permission_friction(feats("leader", bash("ls")))[0] is True
    v, why = ae.check_permission_friction(feats("leader", bash("rm -rf x", denial="permission-rule")))
    assert v is False and "Bash:permission-rule" in why


def test_tool_error_rate():
    ok = bash("ls", tid="1") + bash("ls", tid="2", sec=1)
    assert ae.check_tool_error_rate(feats("leader", ok))[0] is True
    bad = bash("ls", tid="1", error=True) + bash("ls", tid="2", sec=1)
    v, why = ae.check_tool_error_rate(feats("leader", bad))
    assert v is False and "1/2" in why
    assert ae.check_tool_error_rate(feats("leader", []))[0] is None


def _leader_with(runs: list[dict], leader_entries: list[dict] | None = None) -> dict:
    f = feats("leader", leader_entries or [], kind="leader")
    f["subagent_runs"] = runs
    return f


def _run(role, start, end, changes=(), finished=True):
    return {"agent_id": f"{role[:4]}{start:04d}", "role": role, "description": "",
            "start_ts": start, "end_ts": end, "finished": finished,
            "changes": [{"path": p, "ts": t, "tool": "Write"} for p, t in changes]}


def test_security_gate():
    devops = _run("devops-engineer", 1, 10, [("docker-compose.yml", 5)])
    assert ae.check_security_gate(_leader_with([devops, _run("cybersecurity", 20, 30)]))[0] is True
    v, why = ae.check_security_gate(_leader_with([_run("cybersecurity", 0, 1), devops]))
    assert v is False and "docker-compose.yml" in why
    code_only = _run("data-engineer", 1, 10, [("src/a.py", 5)])
    assert ae.check_security_gate(_leader_with([code_only]))[0] is True
    assert ae.check_security_gate(feats("devops-engineer", []))[0] is None


def test_roadmap_updated():
    run = _run("data-engineer", ae.hook.parse_ts_ms(ts(0)), ae.hook.parse_ts_ms(ts(10)))
    late = write("docs/ROADMAP.md", sec=20)
    assert ae.check_roadmap_updated(_leader_with([run], late))[0] is True
    early = write("docs/ROADMAP.md", sec=5)
    v, why = ae.check_roadmap_updated(_leader_with([run], early))
    assert v is False and "before" in why
    assert ae.check_roadmap_updated(_leader_with([]))[0] is None


# ------------------------------------------------------------ session + scorers


@pytest.fixture
def session(tmp_path: Path) -> Path:
    leader = tmp_path / "s1.jsonl"
    lines = write("docs/ROADMAP.md", sec=1, tid="l1")
    leader.write_text("\n".join(json.dumps(e) for e in lines) + "\n")
    sub = tmp_path / "s1" / "subagents"
    sub.mkdir(parents=True)
    agent = write("Makefile", sec=2) + bash("make test", sec=3, tid="b2")
    (sub / "agent-abc123.jsonl").write_text("\n".join(json.dumps(e) for e in agent) + "\n")
    (sub / "agent-abc123.meta.json").write_text(json.dumps(
        {"agentType": "devops-engineer", "description": "DevOps: make", "finished": True}))
    return leader


def test_load_session_and_scorer_objects(session: Path):
    rows = ae.load_session(session)
    assert [r["role"] for r in rows] == ["devops-engineer", "leader"]
    dev, lead = rows
    assert dev["files_changed"] == ["Makefile"] and dev["verification_commands"] == ["make check"]
    assert dev["description"] == "DevOps: make" and "final_report" not in dev
    assert lead["subagent_runs"][0]["changes"][0]["path"] == "Makefile"
    scored = ae.score_rows(rows)
    assert scored[1]["scores"]["security_gate"]["value"] is False  # Makefile never reviewed
    assert scored[1]["scores"]["roadmap_updated"]["value"] is False  # roadmap before subagent
    by_name = {s.name: s for s in ae.SCORERS}
    fb = by_name["stays_in_lane"](outputs=dev)
    assert fb.value is True and fb.rationale
    assert by_name["reviewer_read_only"](outputs=dev) is None
    table = ae.render(scored)
    assert "devops-engineer-abc123" in table and "pass rate" in table
