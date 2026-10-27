"""Fake e-commerce order event producer (ADR-001 event schema v1).

All data is synthetic. Event generation (`OrderGenerator`) is pure and has no
Kafka dependency, so it can be unit-tested; Kafka I/O lives in `run()`.

Usage:
    uv run python -m datalake_duckdb.producer --rate 5 --duration 60
    uv run python -m datalake_duckdb.producer --dry-run --rate 20 --duration 2
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

SCHEMA_VERSION = 1

# (product, category, unit_price) — fixed synthetic catalog.
CATALOG: list[tuple[str, str, float]] = [
    ("Wireless Mouse", "electronics", 24.90),
    ("Mechanical Keyboard", "electronics", 89.00),
    ("USB-C Hub", "electronics", 39.50),
    ("Noise Cancelling Headphones", "electronics", 199.99),
    ("Cotton T-Shirt", "apparel", 15.00),
    ("Denim Jeans", "apparel", 49.90),
    ("Running Shoes", "apparel", 120.00),
    ("Rain Jacket", "apparel", 79.95),
    ("Coffee Beans 1kg", "grocery", 22.40),
    ("Green Tea 100 bags", "grocery", 8.75),
    ("Olive Oil 500ml", "grocery", 11.20),
    ("Dark Chocolate Bar", "grocery", 3.50),
    ("Chef Knife", "home", 64.00),
    ("Cast Iron Pan", "home", 45.00),
    ("Desk Lamp", "home", 29.99),
    ("Throw Blanket", "home", 34.50),
    ("Yoga Mat", "sports", 27.00),
    ("Dumbbell Set 10kg", "sports", 59.90),
    ("Water Bottle", "sports", 12.99),
    ("Tennis Racket", "sports", 110.00),
]

STATUSES = ("created", "paid", "shipped", "delivered", "cancelled")
TERMINAL = {"delivered", "cancelled"}
# Valid transitions; used by the generator and by tests.
TRANSITIONS: dict[str | None, set[str]] = {
    None: {"created"},
    "created": {"paid", "cancelled"},
    "paid": {"shipped", "cancelled"},
    "shipped": {"delivered"},
}
# Seconds to wait (min, max) before an order may emit its next status.
STAGE_DELAY_S: dict[str, tuple[float, float]] = {
    "created": (2, 20),
    "paid": (5, 40),
    "shipped": (10, 60),
}
CANCEL_PROB = {"created": 0.08, "paid": 0.03}

log = logging.getLogger("datalake_duckdb.producer")


def format_ts(ts: datetime) -> str:
    """ISO-8601 UTC with milliseconds and trailing Z, e.g. 2026-10-27T12:00:00.123Z."""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@dataclass
class _Order:
    order_id: str
    customer_id: str
    product: str
    category: str
    quantity: int
    unit_price: float
    status: str
    last_time: datetime
    next_due: datetime


class OrderGenerator:
    """Pure generator of order lifecycle events.

    Keeps a pool of in-flight orders; each call to `next_event` either starts a
    new order (`created`) or advances a due in-flight order to its next status.
    Pass `now` explicitly for deterministic output (tests); otherwise wall-clock
    UTC is used.
    """

    def __init__(
        self,
        seed: int | None = None,
        dup_rate: float = 0.01,
        new_order_prob: float = 0.35,
        max_in_flight: int = 500,
        dup_history: int = 1000,
    ) -> None:
        if not 0 <= dup_rate <= 1:
            raise ValueError("dup_rate must be between 0 and 1")
        self.rng = random.Random(seed)
        self.dup_rate = dup_rate
        self.new_order_prob = new_order_prob
        self.max_in_flight = max_in_flight
        self.in_flight: dict[str, _Order] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=dup_history)
        self.duplicates_emitted = 0

    # -- helpers -----------------------------------------------------------
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _delay(self, status: str) -> timedelta:
        lo, hi = STAGE_DELAY_S[status]
        return timedelta(seconds=self.rng.uniform(lo, hi))

    def _emit(self, order: _Order, ts: datetime) -> dict[str, Any]:
        event = {
            "event_id": self._uuid(),
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "product": order.product,
            "category": order.category,
            "quantity": order.quantity,
            "unit_price": order.unit_price,
            "status": order.status,
            "event_time": format_ts(ts),
            "schema_version": SCHEMA_VERSION,
        }
        self._history.append(event)
        return event

    def _new_order(self, now: datetime) -> dict[str, Any]:
        product, category, price = self.rng.choice(CATALOG)
        order = _Order(
            order_id=self._uuid(),
            customer_id=f"cust_{self.rng.randint(1, 9999):04d}",
            product=product,
            category=category,
            quantity=self.rng.randint(1, 5),
            unit_price=price,
            status="created",
            last_time=now,
            next_due=now + self._delay("created"),
        )
        self.in_flight[order.order_id] = order
        return self._emit(order, now)

    def _advance(self, order: _Order, now: datetime) -> dict[str, Any]:
        current = order.status
        if current in CANCEL_PROB and self.rng.random() < CANCEL_PROB[current]:
            nxt = "cancelled"
        else:
            nxt = {"created": "paid", "paid": "shipped", "shipped": "delivered"}[current]
        # Never go backwards in time for the same order.
        ts = max(now, order.last_time + timedelta(milliseconds=1))
        order.status = nxt
        order.last_time = ts
        if nxt in TERMINAL:
            del self.in_flight[order.order_id]
        else:
            order.next_due = ts + self._delay(nxt)
        return self._emit(order, ts)

    # -- public API --------------------------------------------------------
    def next_event(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(UTC)

        if self._history and self.rng.random() < self.dup_rate:
            self.duplicates_emitted += 1
            return dict(self.rng.choice(self._history))

        if len(self.in_flight) >= self.max_in_flight:
            # Pool full: advance the order that has waited longest.
            oldest = min(self.in_flight.values(), key=lambda o: o.next_due)
            return self._advance(oldest, now)

        due = [o for o in self.in_flight.values() if o.next_due <= now]
        if not due or self.rng.random() < self.new_order_prob:
            return self._new_order(now)
        return self._advance(self.rng.choice(due), now)


# ---------------------------------------------------------------------------
# Kafka I/O
# ---------------------------------------------------------------------------
def kafka_config() -> tuple[dict[str, Any], str]:
    """Build confluent-kafka config from env vars. Never logs secret values."""
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:  # pragma: no cover
        pass

    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    conf: dict[str, Any] = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        "security.protocol": protocol,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "client.id": "datalake-duckdb-producer",
    }
    if protocol.upper() == "SASL_SSL":
        user = os.getenv("KAFKA_SASL_USERNAME")
        password = os.getenv("KAFKA_SASL_PASSWORD")
        if not user or not password:
            raise SystemExit("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be set for SASL_SSL")
        conf.update({"sasl.mechanisms": "PLAIN", "sasl.username": user, "sasl.password": password})
    return conf, os.getenv("KAFKA_TOPIC", "orders")


class _Stats:
    def __init__(self) -> None:
        self.sent = 0
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            log.warning("delivery failed: %s", err)
        else:
            self.delivered += 1


def run(rate: float, duration: float, dup_rate: float, seed: int | None, dry_run: bool) -> _Stats:
    gen = OrderGenerator(seed=seed, dup_rate=dup_rate)
    stats = _Stats()
    producer = None
    topic = "orders"

    if not dry_run:
        from confluent_kafka import Producer

        conf, topic = kafka_config()
        log.info(
            "producing to topic=%s bootstrap=%s protocol=%s",
            topic,
            conf["bootstrap.servers"],
            conf["security.protocol"],
        )
        producer = Producer(conf)

    interval = 1.0 / rate
    start = time.monotonic()
    next_send = start
    last_log = start

    try:
        while duration <= 0 or time.monotonic() - start < duration:
            event = gen.next_event()
            payload = json.dumps(event, separators=(",", ":"))
            if producer is None:
                sys.stdout.write(payload + "\n")
                sys.stdout.flush()
                stats.delivered += 1
            else:
                while True:
                    try:
                        producer.produce(
                            topic,
                            key=event["order_id"],
                            value=payload,
                            on_delivery=stats.on_delivery,
                        )
                        break
                    except BufferError:
                        producer.poll(0.5)
                producer.poll(0)
            stats.sent += 1

            now = time.monotonic()
            if now - last_log >= 10:
                log.info(
                    "sent=%d delivered=%d failed=%d in_flight_orders=%d dups=%d",
                    stats.sent,
                    stats.delivered,
                    stats.failed,
                    len(gen.in_flight),
                    gen.duplicates_emitted,
                )
                last_log = now

            next_send += interval
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_send = time.monotonic()  # fell behind; don't burst
    except KeyboardInterrupt:
        log.info("interrupted, flushing...")
    finally:
        if producer is not None:
            remaining = producer.flush(15)
            if remaining:
                log.warning("%d messages not delivered after flush", remaining)
        log.info(
            "done: sent=%d delivered=%d failed=%d in_flight_orders=%d dups=%d",
            stats.sent,
            stats.delivered,
            stats.failed,
            len(gen.in_flight),
            gen.duplicates_emitted,
        )
    return stats


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fake e-commerce order event producer (synthetic data).")
    p.add_argument("--rate", type=float, default=5.0, help="events per second (default 5)")
    p.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = forever (default)")
    p.add_argument("--dup-rate", type=float, default=0.01, help="share of duplicate events (default 0.01)")
    p.add_argument("--seed", type=int, default=None, help="random seed for reproducible ids/choices")
    p.add_argument("--dry-run", action="store_true", help="print JSON lines to stdout instead of Kafka")
    args = p.parse_args(argv)
    if args.rate <= 0:
        p.error("--rate must be > 0")
    if not 0 <= args.dup_rate <= 1:
        p.error("--dup-rate must be between 0 and 1")
    return args


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    args = parse_args(argv)
    stats = run(args.rate, args.duration, args.dup_rate, args.seed, args.dry_run)
    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
