"""KubeFn Lite data models."""
from dataclasses import dataclass, field
from typing import Optional, Any
from enum import Enum

class Runtime(Enum):
    PYTHON = "python"
    JVM = "jvm"
    NODE = "node"

class WorkerStatus(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    READY = "ready"
    ERROR = "error"

@dataclass
class FunctionDef:
    id: str = ""
    name: str = ""
    runtime: str = "python"  # python, jvm, node
    version: int = 1
    source_path: str = ""    # path to function code
    entry_point: str = ""    # module:function for Python, class.method for JVM
    dependencies: list = field(default_factory=list)
    config: dict = field(default_factory=dict)
    is_active: bool = True
    created_at: str = ""

@dataclass
class FunctionResult:
    success: bool = False
    output: Any = None
    error: Optional[str] = None
    runtime: str = ""
    duration_ms: int = 0
    function_id: str = ""
    version: int = 1

@dataclass
class GraphDef:
    id: str = ""
    name: str = ""
    stages: list = field(default_factory=list)  # ordered list of {function_id, input_mapping}
    edges: list = field(default_factory=list)
    config: dict = field(default_factory=dict)

@dataclass
class WorkerInfo:
    runtime: str = ""
    status: str = "stopped"
    pid: Optional[int] = None
    functions_loaded: int = 0
    uptime_seconds: float = 0
    memory_mb: float = 0
