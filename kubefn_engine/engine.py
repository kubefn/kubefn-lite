"""
KubeFn Lite Engine — Multi-runtime function composition without Kubernetes.

Long-lived workers, shared HeapExchange, hot-swap, FnGraph pipelines.
Python eager, JVM/Node lazy. Zero config to start.
"""

import os
import uuid
import time
import json
import hashlib
import logging
from typing import Optional, Callable, Any

from kubefn_engine.db import KubeFnDB
from kubefn_engine.workers import WorkerManager
from kubefn_engine.exchange import HeapExchange
from kubefn_engine.graph import execute_graph, validate_graph
from kubefn_engine.models import FunctionDef, FunctionResult, GraphDef
from kubefn_engine.errors import (
    FunctionNotFoundError, FunctionExecutionError,
    RuntimeNotAvailableError, HotSwapError,
)

logger = logging.getLogger("kubefn")


class KubeFnEngine:
    """
    Lightweight multi-runtime function composition engine.

    Deploy functions, compose into pipelines, execute with
    shared memory — no Kubernetes required.
    """

    def __init__(self, config: Optional[dict] = None):
        self.config = config or {}
        self.db = KubeFnDB(self.config)
        self.workers = WorkerManager(self.config)
        self.exchange = HeapExchange(max_entries=self.config.get("max_exchange_entries", 10_000))

        # Load active functions into workers
        self._load_active_functions()

        logger.info(
            "KubeFnEngine initialized (runtimes=%s, functions=%d)",
            self.workers.available_runtimes(),
            self._count_active_functions(),
        )

    # ── Function Management ────────────────────────────────────────────

    def deploy(
        self,
        name: str,
        source_code: str,
        entry_point: str = "handler",
        runtime: str = "python",
        dependencies: Optional[list] = None,
        config: Optional[dict] = None,
    ) -> dict:
        """
        Deploy a function. Writes code to file, loads into worker.

        For agents: write code as a string, we handle the rest.
        """
        fn_id = uuid.uuid4().hex[:16]
        now = _now()

        # Write source code to functions directory
        fn_dir = os.path.join(self.config.get("functions_dir", "./kubefn_functions"), runtime)
        os.makedirs(fn_dir, exist_ok=True)

        ext = {"python": ".py", "jvm": ".java", "node": ".js"}.get(runtime, ".py")
        source_path = os.path.join(fn_dir, f"{name}{ext}")

        with open(source_path, "w") as f:
            f.write(source_code)

        # Check if function already exists (hot-swap)
        existing = self._get_function_by_name(name)
        version = 1
        if existing:
            version = existing["version"] + 1
            with self.db.cursor() as cur:
                cur.execute("UPDATE functions SET is_active = 0, updated_at = ? WHERE name = ?", (now, name))

        # Register in DB
        with self.db.cursor() as cur:
            cur.execute(
                """INSERT INTO functions
                   (id, name, runtime, version, source_path, entry_point,
                    dependencies, config, is_active, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
                (fn_id, name, runtime, version, source_path, entry_point,
                 json.dumps(dependencies or []), json.dumps(config or {}), now, now),
            )

        # Load into worker
        worker = self.workers.get_worker(runtime)
        loaded = worker.load_function(name, source_path, entry_point)

        if not loaded:
            return {"success": False, "error": f"Failed to load function into {runtime} worker"}

        # Log
        with self.db.cursor() as cur:
            cur.execute(
                "INSERT INTO execution_log (function_id, runtime, success, duration_ms, created_at) VALUES (?, ?, 1, 0, ?)",
                (fn_id, runtime, now),
            )

        logger.info("Deployed %s v%d (%s) -> %s", name, version, runtime, entry_point)
        return {
            "success": True,
            "function_id": fn_id,
            "name": name,
            "version": version,
            "runtime": runtime,
        }

    def invoke(
        self,
        name: str,
        input_data: Optional[dict] = None,
        timeout: int = 30,
    ) -> FunctionResult:
        """Invoke a deployed function by name."""
        fn = self._get_function_by_name(name)
        if not fn:
            raise FunctionNotFoundError(f"Function '{name}' not found")

        worker = self.workers.get_worker(fn["runtime"])
        start = time.monotonic()
        result = worker.invoke(name, input_data or {}, timeout=timeout)
        duration = int((time.monotonic() - start) * 1000)

        # Log execution
        now = _now()
        with self.db.cursor() as cur:
            cur.execute(
                "INSERT INTO execution_log (function_id, runtime, success, duration_ms, error, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (fn["id"], fn["runtime"], int(result.success), duration, result.error, now),
            )

        result.version = fn["version"]
        return result

    def hot_swap(self, name: str, source_code: str, entry_point: Optional[str] = None) -> dict:
        """Hot-swap a function — reload without restarting the worker."""
        fn = self._get_function_by_name(name)
        if not fn:
            raise FunctionNotFoundError(f"Function '{name}' not found for hot-swap")

        ep = entry_point or fn["entry_point"]
        return self.deploy(name, source_code, ep, fn["runtime"])

    def undeploy(self, name: str) -> bool:
        """Remove a function."""
        fn = self._get_function_by_name(name)
        if not fn:
            return False

        worker = self.workers.get_worker(fn["runtime"])
        worker.unload_function(name)

        now = _now()
        with self.db.cursor() as cur:
            cur.execute("UPDATE functions SET is_active = 0, updated_at = ? WHERE name = ?", (now, name))

        return True

    # ── Pipeline Execution ─────────────────────────────────────────────

    def run_graph(self, graph_def: dict, initial_input: Optional[dict] = None) -> dict:
        """Execute a function graph (pipeline)."""
        graph = GraphDef(
            id=graph_def.get("id", uuid.uuid4().hex[:16]),
            name=graph_def.get("name", "unnamed"),
            stages=graph_def.get("stages", []),
        )

        errors = validate_graph(graph)
        if errors:
            return {"success": False, "error": "; ".join(errors)}

        return execute_graph(
            graph=graph,
            invoke_fn=lambda fn_name, data: self.invoke(fn_name, data),
            exchange_put=lambda data: self.exchange.put(data),
            exchange_get=lambda handle: self.exchange.get(handle),
            initial_input=initial_input,
        )

    # ── Query ──────────────────────────────────────────────────────────

    def list_functions(self, runtime: Optional[str] = None) -> list[dict]:
        cur = self.db.read_cursor()
        if runtime:
            cur.execute("SELECT * FROM functions WHERE is_active = 1 AND runtime = ?", (runtime,))
        else:
            cur.execute("SELECT * FROM functions WHERE is_active = 1")
        return [dict(row) for row in cur.fetchall()]

    def get_function(self, name: str) -> Optional[dict]:
        return self._get_function_by_name(name)

    def _get_function_by_name(self, name: str) -> Optional[dict]:
        cur = self.db.read_cursor()
        cur.execute("SELECT * FROM functions WHERE name = ? AND is_active = 1", (name,))
        row = cur.fetchone()
        return dict(row) if row else None

    def _count_active_functions(self) -> int:
        cur = self.db.read_cursor()
        cur.execute("SELECT COUNT(*) as c FROM functions WHERE is_active = 1")
        return cur.fetchone()["c"]

    def _load_active_functions(self):
        """Load all active functions into their workers on startup."""
        cur = self.db.read_cursor()
        cur.execute("SELECT * FROM functions WHERE is_active = 1")
        for row in cur.fetchall():
            try:
                worker = self.workers.get_worker(row["runtime"])
                worker.load_function(row["name"], row["source_path"], row["entry_point"])
            except RuntimeNotAvailableError:
                logger.debug("Skipping %s function %s (runtime not available)", row["runtime"], row["name"])

    # ── Stats & Health ─────────────────────────────────────────────────

    def stats(self) -> dict:
        cur = self.db.read_cursor()
        cur.execute("SELECT COUNT(*) as c FROM functions WHERE is_active = 1")
        fn_count = cur.fetchone()["c"]

        cur.execute("SELECT runtime, COUNT(*) as c FROM functions WHERE is_active = 1 GROUP BY runtime")
        by_runtime = {row["runtime"]: row["c"] for row in cur.fetchall()}

        cur.execute("SELECT COUNT(*) as c FROM execution_log")
        total_invocations = cur.fetchone()["c"]

        cur.execute("SELECT AVG(duration_ms) as avg_ms FROM execution_log WHERE success = 1")
        avg_row = cur.fetchone()
        avg_latency = round(avg_row["avg_ms"] or 0, 2)

        return {
            "active_functions": fn_count,
            "functions_by_runtime": by_runtime,
            "available_runtimes": self.workers.available_runtimes(),
            "total_invocations": total_invocations,
            "avg_latency_ms": avg_latency,
            "exchange": self.exchange.stats(),
            "workers": [w.__dict__ for w in self.workers.get_all_info()],
        }

    def health_check(self) -> dict:
        stats = self.stats()
        return {
            "healthy": True,
            "engine": "kubefn-lite",
            "runtimes": stats["available_runtimes"],
            "functions": stats["active_functions"],
        }

    # ── Lifecycle ──────────────────────────────────────────────────────

    def close(self):
        self.workers.close()
        self.exchange.clear()
        self.db.close()
        logger.info("KubeFnEngine closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
