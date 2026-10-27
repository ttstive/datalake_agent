"""Unit tests for spark/ingest_orders.py pure helpers (no Kafka, Spark session or Databricks)."""

import importlib.util
import sys
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[1] / "spark" / "ingest_orders.py"
_spec = importlib.util.spec_from_file_location("ingest_orders", _PATH)
ing = importlib.util.module_from_spec(_spec)
sys.modules["ingest_orders"] = ing
_spec.loader.exec_module(ing)

QID = "3f2a9c1e-0b7d-4e5f-9a8b-123456789abc"
ENV = {"DATABRICKS_CATALOG": "workspace"}


# -- config ------------------------------------------------------------------
def test_config_defaults():
    cfg = ing.Config.from_env(ENV)
    assert cfg.bootstrap_servers == "kafka:29092"
    assert cfg.topic == "orders"
    assert cfg.trigger_interval == "5 minutes"
    assert cfg.checkpoint_dir == "/checkpoints/orders"
    assert (cfg.catalog, cfg.schema, cfg.volume) == ("workspace", "ecommerce", "landing")
    assert cfg.starting_offsets == "earliest"
    assert cfg.max_offsets_per_trigger == 100000


def test_config_requires_catalog():
    with pytest.raises(ValueError, match="DATABRICKS_CATALOG"):
        ing.Config.from_env({})


@pytest.mark.parametrize("bad", ["a/b", "..", "x y", "cat;drop"])
def test_config_rejects_unsafe_names(bad):
    with pytest.raises(ValueError):
        ing.Config.from_env({**ENV, "DATABRICKS_SCHEMA": bad})


def test_config_does_not_need_token():
    # Token is read by WorkspaceClient, never by our config.
    assert "token" not in {f.lower() for f in ing.Config.__dataclass_fields__}


# -- paths / naming ------------------------------------------------------------
def test_landing_dir():
    assert ing.landing_dir("workspace", "ecommerce", "landing") == "/Volumes/workspace/ecommerce/landing/orders"


def test_batch_file_name_is_deterministic_and_sortable():
    assert ing.batch_file_name(42, QID) == "batch_0000000042_3f2a9c1e.parquet"
    assert ing.batch_file_name(42, QID) == ing.batch_file_name(42, QID)
    names = [ing.batch_file_name(i, QID) for i in (9, 10, 100, 2)]
    assert sorted(names) == [ing.batch_file_name(i, QID) for i in (2, 9, 10, 100)]


def test_batch_file_name_differs_per_query():
    other = "aaaaaaaa-0000-0000-0000-000000000000"
    assert ing.batch_file_name(0, QID) != ing.batch_file_name(0, other)


@pytest.mark.parametrize("batch_id,qid", [(-1, QID), (0, ""), (0, "../../x")])
def test_batch_file_name_rejects_bad_input(batch_id, qid):
    with pytest.raises(ValueError):
        ing.batch_file_name(batch_id, qid)


def test_batch_target_path():
    assert (
        ing.batch_target_path("workspace", "ecommerce", "landing", 7, QID)
        == "/Volumes/workspace/ecommerce/landing/orders/batch_0000000007_3f2a9c1e.parquet"
    )


# -- local files ---------------------------------------------------------------
def test_find_single_part_file(tmp_path):
    (tmp_path / "_SUCCESS").write_text("")
    (tmp_path / ".part-00000-x.snappy.parquet.crc").write_text("")
    part = tmp_path / "part-00000-abc.snappy.parquet"
    part.write_bytes(b"PAR1")
    assert ing.find_single_part_file(tmp_path) == part


@pytest.mark.parametrize("n", [0, 2])
def test_find_single_part_file_requires_exactly_one(tmp_path, n):
    for i in range(n):
        (tmp_path / f"part-0000{i}-x.parquet").write_bytes(b"PAR1")
    with pytest.raises(RuntimeError, match="exactly 1"):
        ing.find_single_part_file(tmp_path)


def test_summarize_offsets():
    rows = [
        {"topic": "orders", "partition": 1, "min_offset": 5, "max_offset": 9, "rows": 5},
        {"topic": "orders", "partition": 0, "min_offset": 0, "max_offset": 2, "rows": 3},
    ]
    assert ing.summarize_offsets(rows) == "orders[0]=0..2 (3), orders[1]=5..9 (5)"
    assert ing.summarize_offsets([]) == "-"


# -- Databricks client interactions (fake client) ----------------------------
class _FakeFiles:
    def __init__(self, fail_times=0, fail_dir=False):
        self.fail_times = fail_times
        self.fail_dir = fail_dir
        self.uploads = []
        self.dirs = []

    def create_directory(self, path):
        if self.fail_dir:
            raise PermissionError("PERMISSION_DENIED")
        self.dirs.append(path)

    def get_directory_metadata(self, path):
        return None

    def upload(self, path, fh, overwrite=None):
        if self.fail_times:
            self.fail_times -= 1
            raise ConnectionError("boom")
        self.uploads.append((path, fh.read(), overwrite))


class _FakeClient:
    def __init__(self, **kw):
        self.files = _FakeFiles(**kw)


def test_verify_landing_creates_dir():
    w = _FakeClient()
    cfg = ing.Config.from_env(ENV)
    assert ing.verify_landing(w, cfg) == "/Volumes/workspace/ecommerce/landing/orders"
    assert w.files.dirs == ["/Volumes/workspace/ecommerce/landing/orders"]


def test_verify_landing_fails_fast_with_clear_message():
    with pytest.raises(SystemExit, match="cannot reach landing volume"):
        ing.verify_landing(_FakeClient(fail_dir=True), ing.Config.from_env(ENV))


def test_upload_retries_then_overwrites(tmp_path, monkeypatch):
    monkeypatch.setattr(ing, "UPLOAD_BACKOFF_S", 0)
    f = tmp_path / "part.parquet"
    f.write_bytes(b"data")
    w = _FakeClient(fail_times=2)
    ing.upload_file(w, f, "/Volumes/c/s/v/orders/x.parquet")
    assert w.files.uploads == [("/Volumes/c/s/v/orders/x.parquet", b"data", True)]


def test_upload_raises_after_max_attempts(tmp_path, monkeypatch):
    monkeypatch.setattr(ing, "UPLOAD_BACKOFF_S", 0)
    f = tmp_path / "part.parquet"
    f.write_bytes(b"data")
    with pytest.raises(RuntimeError, match="failed after"):
        ing.upload_file(_FakeClient(fail_times=99), f, "/Volumes/c/s/v/orders/x.parquet")


# -- metadata columns (needs a pyspark module, but no session) ---------------
def test_add_batch_metadata_columns():
    pytest.importorskip("pyspark")

    class FakeDF:
        def withColumns(self, cols):
            self.added = cols
            return self

        def select(self, *cols):
            self.selected = list(cols)
            return self

    df = ing.add_batch_metadata(FakeDF(), 3)
    assert set(df.added) == {"batch_id", "ingested_at"}
    assert df.selected == ing.LANDING_COLUMNS
    assert ing.LANDING_COLUMNS[:6] == ["kafka_key", "value", "topic", "partition", "offset", "kafka_timestamp"]
