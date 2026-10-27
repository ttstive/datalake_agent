"""Provider-agnostic, metadata-only agent evaluation logging for MLflow.

No prompt, response, or context content is accepted by this module. Quality
scores use a caller-defined, consistently applied 0.0–1.0 scale.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from typing import Any, Literal

ContextVariant = Literal["with_context", "without_context"]
Outcome = Literal["success", "failure", "partial"]

DEFAULT_EXPERIMENT = "agent-monitoring"
_OPAQUE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/-]{0,249}\Z")


def _identifier(name: str, value: str | None) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    if len(value) > 250:
        raise ValueError(f"{name} must be at most 250 characters")


def _opaque_identifier(name: str, value: str | None) -> None:
    _identifier(name, value)
    if not isinstance(value, str) or not _OPAQUE_IDENTIFIER.fullmatch(value):
        raise ValueError(
            f"{name} must be an opaque identifier using letters, numbers, "
            "periods, underscores, colons, slashes, or hyphens"
        )


def _count(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _score(name: str, value: float) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number from 0.0 to 1.0")
    if not math.isfinite(value) or not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be a finite number from 0.0 to 1.0")


@dataclass(frozen=True)
class AgentRunRecord:
    """Metadata for one evaluated agent task; deliberately contains no content."""

    agent_id: str
    provider: str
    model: str
    task_id: str
    task_case_id: str
    duration_seconds: float
    outcome: Outcome
    input_tokens: int
    output_tokens: int
    total_tokens: int
    context_tokens: int
    context_variant: ContextVariant
    context_source: str | None
    quality_score: float

    def __post_init__(self) -> None:
        for field in ("agent_id", "provider", "model", "task_id", "task_case_id"):
            _opaque_identifier(field, getattr(self, field))
        if self.outcome not in ("success", "failure", "partial"):
            raise ValueError("outcome must be 'success', 'failure', or 'partial'")
        if self.context_variant not in ("with_context", "without_context"):
            raise ValueError(
                "context_variant must be 'with_context' or 'without_context'"
            )
        if isinstance(self.duration_seconds, bool) or not isinstance(
            self.duration_seconds, (int, float)
        ):
            raise TypeError("duration_seconds must be a finite non-negative number")
        if not math.isfinite(self.duration_seconds) or self.duration_seconds < 0:
            raise ValueError("duration_seconds must be a finite non-negative number")
        for field in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "context_tokens",
        ):
            _count(field, getattr(self, field))
        if self.total_tokens != self.input_tokens + self.output_tokens:
            raise ValueError("total_tokens must equal input_tokens + output_tokens")
        if self.context_tokens > self.input_tokens:
            raise ValueError("context_tokens cannot exceed input_tokens")
        if self.context_variant == "with_context":
            _opaque_identifier("context_source", self.context_source)
            if self.context_tokens == 0:
                raise ValueError("with_context requires context_tokens > 0")
        elif self.context_tokens != 0 or self.context_source is not None:
            raise ValueError(
                "without_context requires zero context_tokens and no context_source"
            )
        _score("quality_score", self.quality_score)


@dataclass(frozen=True)
class QualityComparison:
    """Explicit paired comparison; positive delta means context scored higher."""

    task_case_id: str
    without_context_run_id: str
    with_context_run_id: str
    without_context_score: float
    with_context_score: float

    def __post_init__(self) -> None:
        for field in ("task_case_id", "without_context_run_id", "with_context_run_id"):
            _opaque_identifier(field, getattr(self, field))
        if self.without_context_run_id == self.with_context_run_id:
            raise ValueError("paired runs must have different run IDs")
        _score("without_context_score", self.without_context_score)
        _score("with_context_score", self.with_context_score)

    @property
    def quality_delta(self) -> float:
        """With-context score minus without-context score."""
        return self.with_context_score - self.without_context_score


class AgentMonitor:
    """Log run metrics and explicit paired quality comparisons to MLflow."""

    def __init__(
        self,
        experiment_name: str | None = None,
        tracking_uri: str | None = None,
    ) -> None:
        experiment_name = experiment_name or os.getenv(
            "AGENT_MONITOR_EXPERIMENT", DEFAULT_EXPERIMENT
        )
        _opaque_identifier("experiment_name", experiment_name)
        self.experiment_name = experiment_name
        self.tracking_uri = (
            tracking_uri
            or os.getenv("AGENT_MONITOR_TRACKING_URI")
            or os.getenv("MLFLOW_TRACKING_URI")
            or "databricks"
        )
        _identifier("tracking_uri", self.tracking_uri)

    @staticmethod
    def _mlflow() -> Any:
        try:
            import mlflow
        except ImportError as exc:
            raise RuntimeError(
                "MLflow is required for logging. Install project dependencies with `uv sync`."
            ) from exc
        return mlflow

    def _configure(self, mlflow: Any) -> None:
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)

    def log_record(self, record: AgentRunRecord) -> str:
        """Log one metadata-only run and return its MLflow run ID."""
        if not isinstance(record, AgentRunRecord):
            raise TypeError("record must be an AgentRunRecord")
        mlflow = self._mlflow()
        self._configure(mlflow)
        with mlflow.start_run(
            run_name=f"{record.agent_id}:{record.task_id}",
            tags={
                "agent_id": record.agent_id,
                "provider": record.provider,
                "model": record.model,
                "task_id": record.task_id,
                "task_case_id": record.task_case_id,
                "outcome": record.outcome,
                "context_variant": record.context_variant,
                "context_source": record.context_source or "none",
            },
        ) as run:
            mlflow.log_params(
                {
                    "agent_id": record.agent_id,
                    "provider": record.provider,
                    "model": record.model,
                    "task_id": record.task_id,
                    "task_case_id": record.task_case_id,
                    "outcome": record.outcome,
                    "context_variant": record.context_variant,
                    "context_source": record.context_source or "none",
                }
            )
            mlflow.log_metrics(
                {
                    "duration_seconds": float(record.duration_seconds),
                    "input_tokens": record.input_tokens,
                    "output_tokens": record.output_tokens,
                    "total_tokens": record.total_tokens,
                    "context_tokens": record.context_tokens,
                    "quality_score": float(record.quality_score),
                }
            )
            return run.info.run_id

    def log_quality_comparison(self, comparison: QualityComparison) -> str:
        """Log a caller-specified, task-case-paired context quality delta."""
        if not isinstance(comparison, QualityComparison):
            raise TypeError("comparison must be a QualityComparison")
        mlflow = self._mlflow()
        self._configure(mlflow)
        with mlflow.start_run(
            run_name=f"context-quality:{comparison.task_case_id}",
            tags={
                "record_type": "context_quality_comparison",
                "task_case_id": comparison.task_case_id,
                "without_context_run_id": comparison.without_context_run_id,
                "with_context_run_id": comparison.with_context_run_id,
            },
        ) as run:
            mlflow.log_params(
                {
                    "task_case_id": comparison.task_case_id,
                    "without_context_run_id": comparison.without_context_run_id,
                    "with_context_run_id": comparison.with_context_run_id,
                }
            )
            mlflow.log_metrics(
                {
                    "without_context_quality_score": comparison.without_context_score,
                    "with_context_quality_score": comparison.with_context_score,
                    "quality_delta": comparison.quality_delta,
                }
            )
            return run.info.run_id
