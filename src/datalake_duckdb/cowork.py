"""Local, metadata-only AI process and usage monitoring."""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import RLock
from typing import ClassVar


class CoworkMonitor:
    """Combine process observations with explicitly reported per-response usage.

    Events are deltas, never cumulative counters. An event_id must be unique
    across providers; replays are ignored. No prompt or command arguments persist.
    """

    APPS: ClassVar[dict[str, str]] = {
        "chatgpt": "OpenAI",
        "codex": "OpenAI",
        "claude": "Anthropic",
        "ollama": "Local",
        "lm studio": "Local",
        "gemini": "Google",
    }
    FIELDS: ClassVar[set[str]] = {
        "event_id",
        "session_id",
        "provider",
        "model",
        "input_tokens",
        "output_tokens",
        "context_tokens",
        "context_window",
        "timestamp",
    }

    def __init__(
        self,
        database: str = ":memory:",
        *,
        token_budget: int = 100000,
        tokens_per_minute: int = 10000,
    ):
        for value in (token_budget, tokens_per_minute):
            if type(value) is not int or value <= 0:
                raise ValueError("thresholds must be positive integers")
        self.token_budget = token_budget
        self.tokens_per_minute = tokens_per_minute
        self.lock = RLock()
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""CREATE TABLE IF NOT EXISTS usage (
            event_id TEXT PRIMARY KEY, session_id TEXT, provider TEXT, model TEXT,
            input_tokens INTEGER, output_tokens INTEGER, context_tokens INTEGER,
            context_window INTEGER, timestamp REAL)""")
        self.db.commit()

    def record_usage(self, event: dict) -> bool:
        if not isinstance(event, dict) or set(event) - self.FIELDS:
            raise ValueError("unsupported event fields")
        values = dict(event)
        for key in ("event_id", "session_id", "provider", "model"):
            if not isinstance(values.get(key), str) or not re.fullmatch(
                r"[A-Za-z0-9_.:/-]{1,150}", values[key]
            ):
                raise ValueError(f"{key} must be an opaque identifier")
        for key in (
            "input_tokens",
            "output_tokens",
            "context_tokens",
            "context_window",
        ):
            values.setdefault(key, 0)
            if type(values[key]) is not int or not 0 <= values[key] <= 10**12:
                raise ValueError(f"{key} must be a nonnegative integer <= 10^12")
        if (
            values["context_window"]
            and values["context_tokens"] > values["context_window"]
        ):
            raise ValueError("context exceeds context window")
        values.setdefault("timestamp", time.time())
        stamp = values["timestamp"]
        if (
            type(stamp) not in (int, float)
            or not math.isfinite(stamp)
            or not 0 <= stamp <= time.time() + 5
        ):
            raise ValueError("invalid timestamp")
        with self.lock, self.db:
            result = self.db.execute(
                "INSERT OR IGNORE INTO usage VALUES (:event_id,:session_id,:provider,:model,"
                ":input_tokens,:output_tokens,:context_tokens,:context_window,:timestamp)",
                values,
            )
            return result.rowcount == 1

    def processes(self) -> tuple[list[dict], str | None]:
        try:
            result = subprocess.run(
                ["ps", "-axo", "pid=,pcpu=,rss=,comm="],
                capture_output=True,
                text=True,
                timeout=3,
                check=True,
            )
        except (OSError, subprocess.SubprocessError):
            return [], "Process inspection unavailable"
        found = []
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 3)
            if len(parts) != 4:
                continue
            executable = Path(parts[3]).name.lower()
            for app, provider in self.APPS.items():
                # Match executable identity, never user command arguments.
                if executable == app or executable.startswith(app + " "):
                    try:
                        found.append(
                            {
                                "pid": int(parts[0]),
                                "app": app,
                                "provider": provider,
                                "cpu_percent": float(parts[1]),
                                "memory_mb": round(int(parts[2]) / 1024, 1),
                                "status": "process present; generation unknown",
                            }
                        )
                    except ValueError:
                        pass
                    break
        return found, None

    def snapshot(self) -> dict:
        now = time.time()
        with self.lock:
            rows = [
                dict(row)
                for row in self.db.execute(
                    """SELECT session_id, provider, model, SUM(input_tokens) input_tokens,
                SUM(output_tokens) output_tokens, MAX(timestamp) last_seen,
                SUM(CASE WHEN timestamp > ? THEN input_tokens+output_tokens ELSE 0 END) tokens_last_minute
                FROM usage GROUP BY session_id, provider, model ORDER BY last_seen DESC""",
                    (now - 60,),
                )
            ]
            latest = {
                (r["session_id"], r["provider"], r["model"]): dict(r)
                for r in self.db.execute(
                    "SELECT * FROM usage ORDER BY timestamp, rowid"
                )
            }
        for row in rows:
            event = latest[(row["session_id"], row["provider"], row["model"])]
            row["total_tokens"] = row["input_tokens"] + row["output_tokens"]
            row["context_pct"] = (
                100 * event["context_tokens"] / event["context_window"]
                if event["context_window"]
                else None
            )
            row["alerts"] = []
            if row["total_tokens"] >= self.token_budget:
                row["alerts"].append("Session token budget exceeded")
            if row["tokens_last_minute"] >= self.tokens_per_minute:
                row["alerts"].append("High token burn rate")
            if row["context_pct"] is not None and row["context_pct"] >= 85:
                row["alerts"].append("Context pressure >= 85%")
            row["telemetry_age_seconds"] = max(0, round(now - row["last_seen"]))
        processes, error = self.processes()
        return {
            "timestamp": now,
            "processes": processes,
            "process_error": error,
            "sessions": rows,
            "token_budget": self.token_budget,
            "tokens_per_minute": self.tokens_per_minute,
        }


def serve(monitor: CoworkMonitor, port: int = 8799) -> None:
    page = Path(__file__).with_name("cowork.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            # Reject DNS rebinding and cross-origin browser requests.
            if self.headers.get("Host") not in (
                f"127.0.0.1:{port}",
                f"localhost:{port}",
            ):
                self.send_error(403)
                return
            if self.path != "/" and self.headers.get("Sec-Fetch-Site") == "cross-site":
                self.send_error(403)
                return
            if self.path not in ("/", "/api/snapshot"):
                self.send_error(404)
                return
            data = page if self.path == "/" else json.dumps(monitor.snapshot()).encode()
            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/html; charset=utf-8" if self.path == "/" else "application/json",
            )
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Cowork dashboard: http://127.0.0.1:{port} (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        monitor.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="observability/.state/cowork.sqlite")
    parser.add_argument("--port", type=int, default=8799)
    parser.add_argument("--token-budget", type=int, default=100000)
    parser.add_argument("--tokens-per-minute", type=int, default=10000)
    parser.add_argument(
        "--import-events",
        type=Path,
        help="Import metadata-only JSONL deltas before serving",
    )
    parser.add_argument(
        "--import-only",
        action="store_true",
        help="Import events and exit; an open dashboard sees updates",
    )
    args = parser.parse_args()
    Path(args.database).parent.mkdir(parents=True, exist_ok=True)
    monitor = CoworkMonitor(
        args.database,
        token_budget=args.token_budget,
        tokens_per_minute=args.tokens_per_minute,
    )
    if args.import_events:
        with args.import_events.open() as stream:
            for line in stream:
                if line.strip():
                    monitor.record_usage(json.loads(line))
    if args.import_only:
        monitor.db.close()
    else:
        serve(monitor, args.port)


if __name__ == "__main__":
    main()
