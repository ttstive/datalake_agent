import time
from unittest.mock import patch

import pytest

from datalake_duckdb.cowork import CoworkMonitor


def event(**changes):
    return dict(
        event_id="event-1",
        session_id="session-1",
        provider="test",
        model="model-1",
        input_tokens=80,
        output_tokens=20,
        context_tokens=90,
        context_window=100,
        **changes,
    )


def test_replay_and_thresholds():
    monitor = CoworkMonitor(token_budget=100, tokens_per_minute=100)
    assert monitor.record_usage(event())
    assert not monitor.record_usage(event())
    with patch.object(monitor, "processes", return_value=([], None)):
        row = monitor.snapshot()["sessions"][0]
    assert row["total_tokens"] == 100
    assert row["tokens_last_minute"] == 100
    assert len(row["alerts"]) == 3


def test_old_events_do_not_trigger_burn_rate_or_replace_latest_context():
    monitor = CoworkMonitor()
    monitor.record_usage(event(timestamp=time.time()))
    older = event(timestamp=time.time() - 120)
    older.update(event_id="older", context_tokens=10)
    monitor.record_usage(older)
    with patch.object(monitor, "processes", return_value=([], None)):
        row = monitor.snapshot()["sessions"][0]
    assert row["total_tokens"] == 200
    assert row["tokens_last_minute"] == 100
    assert row["context_pct"] == 90


@pytest.mark.parametrize(
    "change",
    [
        {"input_tokens": -1},
        {"output_tokens": True},
        {"timestamp": float("nan")},
        {"prompt": "secret"},
        {"provider": "personal data"},
        {"context_window": 10},
    ],
)
def test_bad_events_rejected(change):
    monitor = CoworkMonitor()
    data = event()
    data.update(change)
    with pytest.raises(ValueError):
        monitor.record_usage(data)


def test_process_detection_uses_executable_identity():
    monitor = CoworkMonitor()
    output = "10 2.0 2048 /Applications/ChatGPT.app/Contents/MacOS/ChatGPT\n11 0.0 1024 /bin/bash\n"
    with patch("subprocess.run") as run:
        run.return_value.stdout = output
        processes, error = monitor.processes()
    assert error is None
    assert len(processes) == 1
    assert processes[0]["app"] == "chatgpt"
    assert processes[0]["memory_mb"] == 2


def test_usage_persists(tmp_path):
    database = str(tmp_path / "usage.sqlite")
    first = CoworkMonitor(database)
    first.record_usage(event())
    first.db.close()
    second = CoworkMonitor(database)
    assert not second.record_usage(event())
