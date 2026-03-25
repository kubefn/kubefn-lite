"""Runtime worker management — long-lived Python/JVM/Node processes.

Python: in-process (importlib) for zero-latency
JVM: subprocess with stdin/stdout JSON protocol
Node: subprocess with stdin/stdout JSON protocol

Workers start lazily on first function call for that runtime.
"""

import os
import sys
import time
import json
import shutil
import signal
import logging
import importlib
import importlib.util
import subprocess
import threading
from typing import Optional, Callable, Any

from kubefn_engine.models import Runtime, WorkerStatus, WorkerInfo, FunctionResult
from kubefn_engine.errors import RuntimeNotAvailableError, FunctionExecutionError

logger = logging.getLogger("kubefn.workers")


class PythonWorker:
    """In-process Python function executor using importlib."""

    def __init__(self):
        self.status = WorkerStatus.READY
        self._modules: dict[str, Any] = {}
        self._functions: dict[str, Callable] = {}
        self._start_time = time.time()

    def load_function(self, name: str, source_path: str, entry_point: str) -> bool:
        """Load a Python function from file. entry_point = 'function_name' or 'module:function'."""
        try:
            # Read and exec the source directly — avoids importlib caching issues
            with open(source_path, "r") as f:
                source = f.read()

            module_dict: dict = {}
            exec(compile(source, source_path, "exec"), module_dict)

            # Resolve entry point
            fn_name = entry_point.split(":")[-1] if ":" in entry_point else entry_point

            fn = module_dict.get(fn_name)
            if fn is None or not callable(fn):
                logger.error("Function '%s' not found in %s", fn_name, source_path)
                return False

            self._functions[name] = fn
            self._modules[name] = module_dict
            logger.info("Python function loaded: %s -> %s", name, entry_point)
            return True
        except Exception as e:
            logger.error("Failed to load Python function %s: %s", name, e)
            return False

    def unload_function(self, name: str) -> bool:
        self._functions.pop(name, None)
        self._modules.pop(name, None)
        return True

    def invoke(self, name: str, input_data: dict, timeout: int = 30) -> FunctionResult:
        """Invoke a loaded Python function."""
        fn = self._functions.get(name)
        if not fn:
            return FunctionResult(success=False, error=f"Function '{name}' not loaded", runtime="python")

        start = time.monotonic()
        try:
            result = fn(input_data)
            duration = int((time.monotonic() - start) * 1000)
            return FunctionResult(
                success=True, output=result, runtime="python",
                duration_ms=duration, function_id=name,
            )
        except Exception as e:
            duration = int((time.monotonic() - start) * 1000)
            return FunctionResult(
                success=False, error=str(e), runtime="python",
                duration_ms=duration, function_id=name,
            )

    def reload_function(self, name: str, source_path: str, entry_point: str) -> bool:
        """Hot-swap: reload function without restarting."""
        self.unload_function(name)
        return self.load_function(name, source_path, entry_point)

    def get_info(self) -> WorkerInfo:
        return WorkerInfo(
            runtime="python",
            status=self.status.value,
            pid=os.getpid(),
            functions_loaded=len(self._functions),
            uptime_seconds=time.time() - self._start_time,
        )


class SubprocessWorker:
    """Generic subprocess worker for JVM or Node.js.

    Communicates via stdin/stdout newline-delimited JSON.
    The worker process must implement a simple protocol:
    - Input: {"action": "invoke|load|unload|health", ...}
    - Output: {"success": bool, "output": ..., "error": ...}
    """

    def __init__(self, runtime: str, command: list[str]):
        self.runtime = runtime
        self._command = command
        self._process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self.status = WorkerStatus.STOPPED
        self._start_time: Optional[float] = None
        self._functions_loaded = 0

    def start(self) -> bool:
        """Start the worker process."""
        try:
            self._process = subprocess.Popen(
                self._command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.status = WorkerStatus.READY
            self._start_time = time.time()
            logger.info("%s worker started (pid=%s)", self.runtime, self._process.pid)
            return True
        except Exception as e:
            self.status = WorkerStatus.ERROR
            logger.error("Failed to start %s worker: %s", self.runtime, e)
            return False

    def stop(self):
        if self._process:
            try:
                self._process.terminate()
                self._process.wait(timeout=5)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
        self.status = WorkerStatus.STOPPED

    def _send_command(self, command: dict, timeout: int = 30) -> dict:
        """Send a JSON command and get response."""
        if not self._process or self._process.poll() is not None:
            return {"success": False, "error": f"{self.runtime} worker not running"}

        with self._lock:
            try:
                self._process.stdin.write(json.dumps(command) + "\n")
                self._process.stdin.flush()

                # Read response with timeout
                import select
                ready, _, _ = select.select([self._process.stdout], [], [], timeout)
                if not ready:
                    return {"success": False, "error": "Timeout waiting for response"}

                line = self._process.stdout.readline()
                return json.loads(line)
            except Exception as e:
                return {"success": False, "error": str(e)}

    def load_function(self, name: str, source_path: str, entry_point: str) -> bool:
        result = self._send_command({
            "action": "load", "name": name,
            "source_path": source_path, "entry_point": entry_point,
        })
        if result.get("success"):
            self._functions_loaded += 1
        return result.get("success", False)

    def unload_function(self, name: str) -> bool:
        result = self._send_command({"action": "unload", "name": name})
        if result.get("success"):
            self._functions_loaded = max(0, self._functions_loaded - 1)
        return result.get("success", False)

    def invoke(self, name: str, input_data: dict, timeout: int = 30) -> FunctionResult:
        start = time.monotonic()
        result = self._send_command(
            {"action": "invoke", "name": name, "input": input_data},
            timeout=timeout,
        )
        duration = int((time.monotonic() - start) * 1000)
        return FunctionResult(
            success=result.get("success", False),
            output=result.get("output"),
            error=result.get("error"),
            runtime=self.runtime,
            duration_ms=duration,
            function_id=name,
        )

    def reload_function(self, name: str, source_path: str, entry_point: str) -> bool:
        self.unload_function(name)
        return self.load_function(name, source_path, entry_point)

    def get_info(self) -> WorkerInfo:
        return WorkerInfo(
            runtime=self.runtime,
            status=self.status.value,
            pid=self._process.pid if self._process else None,
            functions_loaded=self._functions_loaded,
            uptime_seconds=(time.time() - self._start_time) if self._start_time else 0,
        )

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None


class WorkerManager:
    """Manages all runtime workers. Lazy start for JVM/Node."""

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self._python = PythonWorker()
        self._jvm: Optional[SubprocessWorker] = None
        self._node: Optional[SubprocessWorker] = None

        # Check available runtimes
        self._java_available = shutil.which("java") is not None
        self._node_available = shutil.which("node") is not None

    def get_worker(self, runtime: str):
        """Get (and lazily start) a worker for the given runtime."""
        if runtime == "python":
            return self._python

        elif runtime == "jvm":
            if not self._java_available:
                raise RuntimeNotAvailableError("Java not found on PATH. Install JDK to use JVM functions.")
            if self._jvm is None or not self._jvm.is_running:
                # JVM worker would need a separate Java process — placeholder
                raise RuntimeNotAvailableError("JVM worker not yet implemented in Lite. Use Python or Node.")
            return self._jvm

        elif runtime == "node":
            if not self._node_available:
                raise RuntimeNotAvailableError("Node.js not found on PATH. Install Node to use JS functions.")
            if self._node is None or not self._node.is_running:
                raise RuntimeNotAvailableError("Node worker not yet implemented in Lite. Use Python.")
            return self._node

        raise RuntimeNotAvailableError(f"Unknown runtime: {runtime}")

    def get_all_info(self) -> list[WorkerInfo]:
        workers = [self._python.get_info()]
        if self._jvm:
            workers.append(self._jvm.get_info())
        if self._node:
            workers.append(self._node.get_info())
        return workers

    def available_runtimes(self) -> list[str]:
        runtimes = ["python"]
        if self._java_available:
            runtimes.append("jvm")
        if self._node_available:
            runtimes.append("node")
        return runtimes

    def close(self):
        if self._jvm:
            self._jvm.stop()
        if self._node:
            self._node.stop()
