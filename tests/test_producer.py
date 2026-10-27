import json
import re
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest

from datalake_duckdb.producer import CATALOG, TRANSITIONS, OrderGenerator, parse_args

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
FIELDS = {
    "event_id": str,
    "order_id": str,
    "customer_id": str,
    "product": str,
    "category": str,
    "quantity": int,
    "unit_price": float,
    "status": str,
    "event_time": str,
    "schema_version": int,
}
START = datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC)


def generate(n=3000, seed=42, dup_rate=0.0, step_ms=200):
    gen = OrderGenerator(seed=seed, dup_rate=dup_rate)
    return [gen.next_event(START + timedelta(milliseconds=i * step_ms)) for i in range(n)]


def parse_ts(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def test_schema_fields_and_types():
    for e in generate(500):
        assert set(e) == set(FIELDS)
        for k, t in FIELDS.items():
            assert type(e[k]) is t, (k, e[k])
        uuid.UUID(e["event_id"])
        uuid.UUID(e["order_id"])
        assert re.fullmatch(r"cust_\d{4}", e["customer_id"])
        assert TS_RE.match(e["event_time"])
        assert e["schema_version"] == 1
        assert 1 <= e["quantity"] <= 5
        assert e["unit_price"] > 0 and round(e["unit_price"], 2) == e["unit_price"]
        assert (e["product"], e["category"], e["unit_price"]) in CATALOG
        json.dumps(e)  # serialisable


def test_status_transitions_and_monotonic_time():
    by_order = defaultdict(list)
    for e in generate(5000):
        by_order[e["order_id"]].append(e)

    seen_statuses = set()
    for events in by_order.values():
        prev_status, prev_ts = None, None
        static = {(e["customer_id"], e["product"], e["quantity"]) for e in events}
        assert len(static) == 1  # order attributes stay constant
        for e in events:
            assert e["status"] in TRANSITIONS[prev_status], (prev_status, e["status"])
            ts = parse_ts(e["event_time"])
            assert prev_ts is None or ts >= prev_ts
            prev_status, prev_ts = e["status"], ts
            seen_statuses.add(e["status"])
    assert seen_statuses == {"created", "paid", "shipped", "delivered", "cancelled"}


def test_no_duplicates_when_dup_rate_zero():
    ids = [e["event_id"] for e in generate(5000, dup_rate=0.0)]
    assert len(ids) == len(set(ids))


def test_duplicates_emitted_with_dup_rate():
    gen = OrderGenerator(seed=1, dup_rate=0.05)
    events = [gen.next_event(START + timedelta(milliseconds=i * 200)) for i in range(4000)]
    ids = [e["event_id"] for e in events]
    assert len(ids) - len(set(ids)) == gen.duplicates_emitted > 0


def test_seed_is_deterministic():
    assert generate(1000, seed=7, dup_rate=0.01) == generate(1000, seed=7, dup_rate=0.01)
    assert generate(100, seed=7) != generate(100, seed=8)


def test_cli_validation():
    args = parse_args([])
    assert (args.rate, args.duration, args.dup_rate, args.dry_run) == (5.0, 0, 0.01, False)
    with pytest.raises(SystemExit):
        parse_args(["--rate", "0"])
    with pytest.raises(SystemExit):
        parse_args(["--dup-rate", "2"])
