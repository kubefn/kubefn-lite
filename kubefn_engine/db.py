"""KubeFn Lite database."""
import os, sqlite3, threading, logging
from contextlib import contextmanager
logger = logging.getLogger("kubefn.db")

SCHEMA = """
    CREATE TABLE IF NOT EXISTS functions (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        runtime TEXT NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        source_path TEXT NOT NULL,
        entry_point TEXT NOT NULL,
        dependencies TEXT,
        config TEXT,
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_fn_name ON functions(name, is_active);
    CREATE INDEX IF NOT EXISTS idx_fn_runtime ON functions(runtime, is_active);

    CREATE TABLE IF NOT EXISTS graphs (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        definition TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS execution_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        function_id TEXT,
        graph_id TEXT,
        runtime TEXT,
        success INTEGER,
        duration_ms INTEGER,
        error TEXT,
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_exec_fn ON execution_log(function_id, created_at DESC);
"""

class KubeFnDB:
    def __init__(self, config=None):
        config = config or {}
        self.db_path = config.get("db_path", os.environ.get("KUBEFN_DB_PATH", "./kubefn.db"))
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute("PRAGMA synchronous = NORMAL")
        self._conn.execute("PRAGMA busy_timeout = 5000")
        with self.cursor() as cur:
            cur.executescript(SCHEMA)

    @contextmanager
    def cursor(self):
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                cur = self._conn.cursor()
                yield cur
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def read_cursor(self):
        return self._conn.cursor()

    def close(self):
        try: self._conn.close()
        except: pass
