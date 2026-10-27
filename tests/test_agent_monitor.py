from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from datalake_duckdb.agent_monitor import (
    AgentMonitor,
    AgentRunRecord,
    QualityComparison,
)


def make_record(**overrides):
    values = {
        "agent_id": "support-agent",
        "provider": "example-provider",
        "model": "example-model",
        "task_id": "answer-question",
        "task_case_id": "case-001",
        "duration_seconds": 1.25,
        "outcome": "success",
        "input_tokens": 120,
        "output_tokens": 30,
        "total_tokens": 150,
        "context_tokens": 60,
        "context_variant": "with_context",
        "context_source": "retrieval-v1",
        "quality_score": 0.9,
    }
    return AgentRunRecord(**(values | overrides))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("agent_id", ""),
        ("task_case_id", "customer@example.com"),
        ("context_source", "customer support notes"),
        ("duration_seconds", float("nan")),
        ("duration_seconds", -0.1),
        ("input_tokens", -1),
        ("input_tokens", True),
        ("total_tokens", 149),
        ("context_tokens", 121),
        ("quality_score", 1.01),
        ("quality_score", float("inf")),
        ("outcome", "unknown"),
        ("context_variant", "implicit"),
        ("context_source", None),
        ("context_tokens", 0),
    ],
)
def test_record_rejects_invalid_values(field, value):
    with pytest.raises(ValueError):
        make_record(**{field: value})


def test_without_context_record_requires_zero_context_metadata():
    record = make_record(
        input_tokens=60,
        output_tokens=30,
        total_tokens=90,
        context_tokens=0,
        context_variant="without_context",
        context_source=None,
    )
    assert record.context_variant == "without_context"
    with pytest.raises(ValueError, match="without_context"):
        make_record(context_variant="without_context")


class FakeMLflow:
    def __init__(self):
        self.tracking_uri = None
        self.experiment_name = None
        self.params = []
        self.metrics = []
        self.tags = []
        self.run_names = []
        self._counter = 0

    def set_tracking_uri(self, uri):
        self.tracking_uri = uri

    def set_experiment(self, name):
        self.experiment_name = name

    @contextmanager
    def start_run(self, run_name, tags):
        self._counter += 1
        self.run_names.append(run_name)
        self.tags.append(tags)
        yield SimpleNamespace(info=SimpleNamespace(run_id=f"run-{self._counter}"))

    def log_params(self, params):
        self.params.append(params)

    def log_metrics(self, metrics):
        self.metrics.append(metrics)


def install_fake_mlflow(monkeypatch, fake):
    monkeypatch.setattr(AgentMonitor, "_mlflow", staticmethod(lambda: fake))


def test_log_record_configures_tracking_and_logs_only_metadata(monkeypatch):
    fake = FakeMLflow()
    install_fake_mlflow(monkeypatch, fake)
    record = make_record()

    run_id = AgentMonitor(tracking_uri="databricks").log_record(record)

    assert run_id == "run-1"
    assert fake.tracking_uri == "databricks"
    assert fake.experiment_name == "agent-monitoring"
    assert fake.metrics[0] == {
        "duration_seconds": 1.25,
        "input_tokens": 120,
        "output_tokens": 30,
        "total_tokens": 150,
        "context_tokens": 60,
        "quality_score": 0.9,
    }
    assert fake.params[0]["provider"] == "example-provider"
    assert fake.tags[0]["context_variant"] == "with_context"
    assert not ({"prompt", "response", "context"} & fake.params[0].keys())


def test_tracking_uri_can_come_from_environment(monkeypatch):
    fake = FakeMLflow()
    install_fake_mlflow(monkeypatch, fake)
    monkeypatch.setenv("AGENT_MONITOR_TRACKING_URI", "https://workspace.example")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "should-not-win")
    monkeypatch.setenv("AGENT_MONITOR_EXPERIMENT", "test-agents")

    AgentMonitor().log_record(make_record())

    assert fake.tracking_uri == "https://workspace.example"
    assert fake.experiment_name == "test-agents"


def test_quality_comparison_logs_explicit_delta_and_pair_ids(monkeypatch):
    fake = FakeMLflow()
    install_fake_mlflow(monkeypatch, fake)
    comparison = QualityComparison(
        task_case_id="case-001",
        without_context_run_id="run-baseline",
        with_context_run_id="run-context",
        without_context_score=0.6,
        with_context_score=0.85,
    )

    run_id = AgentMonitor(tracking_uri="databricks").log_quality_comparison(comparison)

    assert run_id == "run-1"
    assert comparison.quality_delta == pytest.approx(0.25)
    assert fake.metrics[0]["quality_delta"] == pytest.approx(0.25)
    assert fake.params[0]["without_context_run_id"] == "run-baseline"
    assert fake.params[0]["with_context_run_id"] == "run-context"
    assert fake.tags[0]["record_type"] == "context_quality_comparison"


def test_comparison_requires_distinct_runs_and_valid_scores():
    with pytest.raises(ValueError, match="different run IDs"):
        QualityComparison("case", "same", "same", 0.4, 0.5)
    with pytest.raises(ValueError, match="0.0 to 1.0"):
        QualityComparison("case", "a", "b", 0.4, -0.1)
