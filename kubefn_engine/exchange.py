"""HeapExchange Lite — shared state between functions.

In-memory key-value store that functions share. Avoids
re-serializing data between pipeline stages.
"""

import time
import json
import hashlib
import threading
import logging
from typing import Optional, Any

logger = logging.getLogger("kubefn.exchange")


class HeapExchange:
    """Shared in-memory exchange for cross-function data passing.

    Functions put/get data by handle. Pipeline stages pass handles
    instead of copying payloads — zero-copy within same process.
    """

    def __init__(self, max_entries: int = 10_000):
        self._store: dict[str, dict] = {}  # handle -> {data, metadata}
        self._lock = threading.RLock()
        self._max_entries = max_entries

    def put(self, data: Any, ttl_seconds: int = 300, metadata: Optional[dict] = None) -> str:
        """Store data and return a handle."""
        handle = hashlib.sha256(
            f"{time.time()}:{id(data)}".encode()
        ).hexdigest()[:16]

        with self._lock:
            if len(self._store) >= self._max_entries:
                self._evict()
            self._store[handle] = {
                "data": data,
                "created_at": time.time(),
                "expires_at": time.time() + ttl_seconds,
                "access_count": 0,
                "metadata": metadata or {},
            }
        return handle

    def get(self, handle: str) -> Optional[Any]:
        """Get data by handle. Returns None if not found or expired."""
        with self._lock:
            entry = self._store.get(handle)
            if not entry:
                return None
            if entry["expires_at"] < time.time():
                del self._store[handle]
                return None
            entry["access_count"] += 1
            return entry["data"]

    def delete(self, handle: str) -> bool:
        with self._lock:
            return self._store.pop(handle, None) is not None

    def exists(self, handle: str) -> bool:
        with self._lock:
            entry = self._store.get(handle)
            if not entry:
                return False
            return entry["expires_at"] >= time.time()

    def _evict(self):
        """Evict expired and oldest entries."""
        now = time.time()
        # Remove expired
        expired = [h for h, e in self._store.items() if e["expires_at"] < now]
        for h in expired:
            del self._store[h]

        # If still over limit, remove oldest
        while len(self._store) >= self._max_entries:
            oldest = min(self._store, key=lambda h: self._store[h]["created_at"])
            del self._store[oldest]

    def clear(self):
        with self._lock:
            self._store.clear()

    @property
    def size(self) -> int:
        return len(self._store)

    def stats(self) -> dict:
        with self._lock:
            now = time.time()
            active = sum(1 for e in self._store.values() if e["expires_at"] >= now)
            return {
                "total_entries": len(self._store),
                "active_entries": active,
                "max_entries": self._max_entries,
            }
