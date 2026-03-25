"""KubeFn Lite error hierarchy."""
from typing import Optional

class KubeFnError(Exception):
    code: str = "KUBEFN_ERROR"
    retryable: bool = False
    def __init__(self, message: str, details: Optional[dict] = None):
        super().__init__(message)
        self.details = details or {}

class RuntimeNotAvailableError(KubeFnError):
    code = "KUBEFN_RUNTIME_NOT_AVAILABLE"

class FunctionNotFoundError(KubeFnError):
    code = "KUBEFN_FUNCTION_NOT_FOUND"

class FunctionExecutionError(KubeFnError):
    code = "KUBEFN_EXECUTION_ERROR"
    retryable = True

class HotSwapError(KubeFnError):
    code = "KUBEFN_HOTSWAP_ERROR"

class GraphCycleError(KubeFnError):
    code = "KUBEFN_GRAPH_CYCLE"

class DependencyError(KubeFnError):
    code = "KUBEFN_DEPENDENCY_ERROR"
