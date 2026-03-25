#!/usr/bin/env python3
"""KubeFn Lite Bridge."""
import sys, json, signal
from kubefn_engine.engine import KubeFnEngine
_engine = None
def get_engine(config=None):
    global _engine
    if _engine is None: _engine = KubeFnEngine(config)
    return _engine
def handle_command(command, args, config):
    engine = get_engine(config)
    try:
        if command == "deploy":
            return engine.deploy(**{k:v for k,v in args.items()})
        elif command == "invoke":
            r = engine.invoke(args["name"], args.get("input", {}), args.get("timeout", 30))
            return {"success": r.success, "output": r.output, "error": r.error, "duration_ms": r.duration_ms, "runtime": r.runtime}
        elif command == "hot_swap":
            return engine.hot_swap(args["name"], args["source_code"], args.get("entry_point"))
        elif command == "undeploy":
            return {"success": engine.undeploy(args["name"])}
        elif command == "run_graph":
            return engine.run_graph(args.get("graph", {}), args.get("input"))
        elif command == "list_functions":
            return {"success": True, "functions": engine.list_functions(args.get("runtime"))}
        elif command == "stats":
            return {"success": True, **engine.stats()}
        elif command == "health_check":
            return engine.health_check()
        else:
            return {"success": False, "error": f"Unknown: {command}"}
    except Exception as e:
        return {"success": False, "error": str(e)}
def main():
    if "--persistent" in sys.argv:
        signal.signal(signal.SIGTERM, lambda s,f: (_engine and _engine.close(), sys.exit(0)))
        for line in sys.stdin:
            if not line.strip(): continue
            try:
                d = json.loads(line)
                r = handle_command(d.get("command",""), d.get("args",{}), d.get("config",{}))
                r["request_id"] = d.get("request_id")
                print(json.dumps(r), flush=True)
            except Exception as e:
                print(json.dumps({"success": False, "error": str(e)}), flush=True)
    else:
        try: d = json.loads(sys.stdin.read())
        except: d = {}
        print(json.dumps(handle_command(d.get("command",""), d.get("args",{}), d.get("config",{}))))
if __name__ == "__main__": main()
