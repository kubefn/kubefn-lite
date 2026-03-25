"""FnGraph — function pipeline composition.

Compose multiple functions into a DAG. Each stage passes
its output handle (not the data itself) to the next stage.
"""

import time
import json
import logging
from typing import Optional, Callable

from kubefn_engine.models import FunctionResult, GraphDef
from kubefn_engine.errors import GraphCycleError

logger = logging.getLogger("kubefn.graph")


def execute_graph(
    graph: GraphDef,
    invoke_fn: Callable,     # (function_name, input_data) -> FunctionResult
    exchange_put: Callable,  # (data) -> handle
    exchange_get: Callable,  # (handle) -> data
    initial_input: dict = None,
) -> dict:
    """
    Execute a function graph (pipeline).

    Stages run sequentially for now. Each stage's output is stored
    in the exchange and passed by handle to the next stage.
    """
    stages = graph.stages
    if not stages:
        return {"success": True, "output": initial_input, "stages_run": 0}

    current_input = initial_input or {}
    results = []
    total_start = time.monotonic()

    for i, stage in enumerate(stages):
        fn_name = stage.get("function")
        if not fn_name:
            continue

        # Resolve input mapping
        stage_input = dict(current_input)
        input_mapping = stage.get("input_mapping", {})
        for key, source in input_mapping.items():
            if source.startswith("handle:"):
                handle = source.replace("handle:", "")
                stage_input[key] = exchange_get(handle)
            elif source.startswith("stage:"):
                idx = int(source.replace("stage:", ""))
                if idx < len(results):
                    stage_input[key] = results[idx].output

        # Invoke function
        result = invoke_fn(fn_name, stage_input)
        results.append(result)

        if not result.success:
            return {
                "success": False,
                "error": f"Stage {i} ({fn_name}) failed: {result.error}",
                "failed_stage": i,
                "stages_run": i + 1,
                "results": [{"function": r.function_id, "success": r.success, "duration_ms": r.duration_ms} for r in results],
            }

        # Store output in exchange for reference passing
        if result.output is not None:
            handle = exchange_put(result.output)
            current_input = result.output if isinstance(result.output, dict) else {"_result": result.output}
            current_input["_handle"] = handle

    total_ms = int((time.monotonic() - total_start) * 1000)

    return {
        "success": True,
        "output": current_input,
        "stages_run": len(results),
        "total_duration_ms": total_ms,
        "results": [
            {"function": r.function_id, "success": r.success, "duration_ms": r.duration_ms}
            for r in results
        ],
    }


def validate_graph(graph: GraphDef) -> list[str]:
    """Validate a graph definition."""
    errors = []
    if not graph.stages:
        errors.append("Graph must have at least one stage")
    for i, stage in enumerate(graph.stages):
        if not stage.get("function"):
            errors.append(f"Stage {i} missing 'function' field")
    return errors
