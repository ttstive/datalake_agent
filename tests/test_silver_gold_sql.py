"""Unit tests for databricks/src/orders_silver.py and orders_gold.py (pure SQL builders, no Spark)."""

import importlib.util
import re
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "databricks" / "src"


def _load(name):
    spec = importlib.util.spec_from_file_location(name, SRC / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


silver = _load("orders_silver")
gold = _load("orders_gold")

SILVER_NB = (SRC / "02_silver.py").read_text()


def _ddl_columns(table_var: str) -> list[str]:
    """Column names from a CREATE TABLE IF NOT EXISTS {table_var} (...) block in 02_silver.py."""
    block = SILVER_NB.split(f"CREATE TABLE IF NOT EXISTS {{{table_var}}} (")[1].split(
        "\n    )\n"
    )[0]
    return [line.split()[0] for line in block.strip().splitlines()]


def _balanced(sql: str) -> bool:
    depth = 0
    for ch in re.sub(
        r"'(?:[^'\\]|\\.)*'", "''", sql
    ):  # ignore parens inside string literals
        depth += ch == "("
        depth -= ch == ")"
        if depth < 0:
            return False
    return depth == 0


# -- status ranking -------------------------------------------------------------------------------------
def test_status_rank_order_and_cancelled_terminal():
    order = ["created", "paid", "shipped", "delivered", "cancelled"]
    assert [silver.status_rank(s) for s in order] == [1, 2, 3, 4, 5]
    assert max(silver.STATUS_RANK, key=silver.STATUS_RANK.get) == "cancelled"


@pytest.mark.parametrize("value", [None, "", "PAID", "refunded"])
def test_status_rank_unknown_is_zero(value):
    assert silver.status_rank(value) == 0


def test_status_rank_sql_covers_all_statuses():
    sql = silver.status_rank_sql("status")
    for s, r in silver.STATUS_RANK.items():
        assert f"WHEN '{s}' THEN {r}" in sql
    assert sql.endswith("ELSE 0 END")


# -- schema + DQ rules ----------------------------------------------------------------------------------
def test_event_schema_is_explicit_and_matches_adr001():
    fields = [f.split()[0] for f in silver.EVENT_JSON_SCHEMA.split(", ")]
    assert fields == [
        "event_id", "order_id", "customer_id", "product", "category", "quantity",
        "unit_price", "status", "event_time", "schema_version", "_corrupt_record",
    ]  # fmt: skip
    assert "quantity INT" in silver.EVENT_JSON_SCHEMA
    assert "schema_version INT" in silver.EVENT_JSON_SCHEMA


def test_dq_rules_names_unique_and_ordered():
    names = [r for r, _ in silver.DQ_RULES]
    assert len(names) == len(set(names))
    assert names[0] == "invalid_json"  # parse failure must win over field rules
    assert names[1] == "type_mismatch"
    assert set(names) >= {
        "missing_event_id", "missing_order_id", "invalid_status", "invalid_quantity",
        "invalid_unit_price", "missing_event_time", "unsupported_schema_version",
    }  # fmt: skip


def test_dq_rule_conditions_use_contract_values():
    rules = dict(silver.DQ_RULES)
    for s in ("created", "paid", "shipped", "delivered", "cancelled"):
        assert f"'{s}'" in rules["invalid_status"]
    assert "NOT BETWEEN 1 AND 5" in rules["invalid_quantity"]
    assert "unit_price <= 0" in rules["invalid_unit_price"]
    assert "<> 1" in rules["unsupported_schema_version"]
    # NULLs must fail, not slip through three-valued logic
    for name in (
        "invalid_status",
        "invalid_quantity",
        "invalid_unit_price",
        "unsupported_schema_version",
    ):
        assert "IS NULL OR" in rules[name]


def test_dq_reason_sql_lists_rules_in_order():
    sql = silver.dq_reason_sql()
    positions = [sql.index(f"THEN '{r}'") for r, _ in silver.DQ_RULES]
    assert positions == sorted(positions)
    assert sql.startswith("CASE ") and sql.endswith("ELSE NULL END")
    assert _balanced(sql)


def test_quarantine_comment_documents_every_reason():
    for reason, _ in silver.DQ_RULES:
        assert reason in SILVER_NB


# -- silver SQL -----------------------------------------------------------------------------------------
def test_parsed_select_uses_from_json_with_explicit_schema():
    sql = silver.parsed_select_sql("bronze_view")
    assert f"from_json(value, '{silver.EVENT_JSON_SCHEMA}'" in sql
    assert "columnNameOfCorruptRecord" in sql
    assert "try_cast(_e.unit_price AS DECIMAL(10,2))" in sql
    assert "try_cast(_e.event_time AS TIMESTAMP)" in sql
    assert "FROM bronze_view AS _bronze" in sql
    assert _balanced(sql)


@pytest.mark.parametrize(
    ("var", "columns"),
    [
        ("silver_table", silver.SILVER_ORDERS_COLUMNS),
        ("quarantine_table", silver.QUARANTINE_COLUMNS),
        ("status_table", silver.ORDER_STATUS_COLUMNS),
    ],
)
def test_ddl_matches_merge_columns(var, columns):
    assert _ddl_columns(var) == columns


def test_silver_and_quarantine_merges_are_insert_only():
    for sql in (
        silver.silver_orders_merge_sql("t", "v"),
        silver.quarantine_merge_sql("t", "v"),
    ):
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "WHEN MATCHED" not in sql.replace("WHEN NOT MATCHED", "")
        assert _balanced(sql)
    assert "ON t.event_id = s.event_id" in silver.silver_orders_merge_sql("t", "v")


def test_valid_events_dedups_on_event_id_and_excludes_quarantine():
    sql = silver.valid_events_sql("v")
    assert "PARTITION BY event_id" in sql
    assert "WHERE dq_reason IS NULL" in sql
    assert "WHERE _rn = 1" in sql


def test_order_status_merge_keeps_earliest_status_timestamps():
    sql = silver.order_status_merge_sql("t", "(events)")
    for col in silver.STATUS_TIMESTAMP_COLUMNS:
        assert f"t.{col} = least(t.{col}, s.{col})" in sql
    assert "t.last_event_time = greatest(t.last_event_time, s.last_event_time)" in sql
    assert (
        "coalesce(t.cancelled_at, s.cancelled_at) IS NOT NULL THEN 'cancelled'" in sql
    )
    assert "ON t.order_id = s.order_id" in sql
    assert _balanced(sql)


# -- gold -----------------------------------------------------------------------------------------------
TABLES = gold.fq_tables("workspace", "ecommerce")


@pytest.mark.parametrize("table", gold.GOLD_TABLES, ids=lambda t: t.name)
def test_gold_tables_well_formed(table):
    names = [c[0] for c in table.columns]
    assert len(names) == len(set(names))
    assert all(comment for _, _, comment in table.columns)
    assert table.comment
    sql = gold.insert_overwrite_sql(table, TABLES[table.name], TABLES)
    assert "{" not in sql and "}" not in sql  # every placeholder filled
    assert sql.startswith(
        f"INSERT OVERWRITE workspace.ecommerce.{table.name} SELECT CAST("
    )
    assert _balanced(sql)
    create = gold.create_sql(table, TABLES[table.name])
    assert create.startswith(
        f"CREATE TABLE IF NOT EXISTS workspace.ecommerce.{table.name} ("
    )
    assert _balanced(create)


def test_gold_tables_only_read_upstream_tables():
    built = []
    for t in gold.GOLD_TABLES:
        refs = set(re.findall(r"\{(\w+)\}", t.select))
        gold_refs = {r for r in refs if r.startswith("gold_")}
        assert gold_refs <= set(built), (
            f"{t.name} reads gold tables not built yet: {gold_refs - set(built)}"
        )
        built.append(t.name)


def test_pipeline_health_has_required_metrics():
    cols = [c[0] for c in gold.PIPELINE_HEALTH.columns]
    for c in ("layer", "row_count", "last_loaded_at", "avg_latency_s", "p95_latency_s",
              "duplicates_removed", "quarantined_rows", "measured_at"):  # fmt: skip
        assert c in cols
    for layer in (
        "kafka",
        "landing",
        "bronze",
        "silver",
        "quarantine",
        "gold",
        "end_to_end",
    ):
        assert f"'{layer}'" in gold.PIPELINE_HEALTH.select


def test_history_append_uses_health_columns_by_name():
    sql = gold.history_append_sql("h", "p")
    cols = ", ".join(c[0] for c in gold.PIPELINE_HEALTH.columns)
    assert sql == f"INSERT INTO h ({cols}) SELECT {cols} FROM p"
    assert gold.HEALTH_HISTORY_COMMENT in gold.history_create_sql("h")


def test_create_sql_escapes_quotes_in_comments():
    t = gold.GoldTable("x", "it's", [("a", "INT", "o'clock")], "SELECT 1 AS a")
    sql = gold.create_sql(t, "c.s.x")
    assert "COMMENT 'o\\'clock'" in sql and "COMMENT 'it\\'s'" in sql


def test_secs_helper():
    assert gold.secs("a", "b") == "(unix_millis(b) - unix_millis(a)) / 1000.0"
