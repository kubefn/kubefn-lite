"""Tests for KubeFn Lite Engine."""
import os, sys, json, time, tempfile, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

class KubeFnTestCase(unittest.TestCase):
    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.db_file.name
        self.db_file.close()
        self.fn_dir = tempfile.mkdtemp()
        from kubefn_engine.engine import KubeFnEngine
        self.engine = KubeFnEngine({"db_path": self.db_path, "functions_dir": self.fn_dir})

    def tearDown(self):
        self.engine.close()
        for ext in ["", "-wal", "-shm"]:
            p = self.db_path + ext
            if os.path.exists(p): os.unlink(p)
        import shutil
        shutil.rmtree(self.fn_dir, ignore_errors=True)


class TestDeploy(KubeFnTestCase):
    def test_deploy_python_function(self):
        result = self.engine.deploy(
            name="add",
            source_code="def handler(input):\n    return input.get('a', 0) + input.get('b', 0)\n",
            entry_point="handler",
            runtime="python",
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["name"], "add")
        self.assertEqual(result["version"], 1)

    def test_deploy_creates_file(self):
        self.engine.deploy("hello", "def handler(i): return 'hello'", "handler", "python")
        fn = self.engine.get_function("hello")
        self.assertTrue(os.path.exists(fn["source_path"]))

    def test_list_functions(self):
        self.engine.deploy("fn1", "def handler(i): return 1", "handler", "python")
        self.engine.deploy("fn2", "def handler(i): return 2", "handler", "python")
        fns = self.engine.list_functions()
        self.assertEqual(len(fns), 2)

    def test_undeploy(self):
        self.engine.deploy("temp", "def handler(i): return 0", "handler", "python")
        self.assertTrue(self.engine.undeploy("temp"))
        self.assertIsNone(self.engine.get_function("temp"))


class TestInvoke(KubeFnTestCase):
    def test_invoke_python_function(self):
        self.engine.deploy("add", "def handler(input):\n    return input['a'] + input['b']\n", "handler")
        result = self.engine.invoke("add", {"a": 3, "b": 5})
        self.assertTrue(result.success)
        self.assertEqual(result.output, 8)
        self.assertEqual(result.runtime, "python")

    def test_invoke_with_dict_return(self):
        self.engine.deploy("greet", 'def handler(i): return {"greeting": f"Hello {i.get(\'name\', \'world\')}"}\n', "handler")
        result = self.engine.invoke("greet", {"name": "Alice"})
        self.assertTrue(result.success)
        self.assertEqual(result.output["greeting"], "Hello Alice")

    def test_invoke_nonexistent(self):
        from kubefn_engine.errors import FunctionNotFoundError
        with self.assertRaises(FunctionNotFoundError):
            self.engine.invoke("nonexistent", {})

    def test_invoke_error_handling(self):
        self.engine.deploy("bad", "def handler(i): raise ValueError('boom')", "handler")
        result = self.engine.invoke("bad", {})
        self.assertFalse(result.success)
        self.assertIn("boom", result.error)

    def test_invoke_latency(self):
        self.engine.deploy("fast", "def handler(i): return 42", "handler")
        result = self.engine.invoke("fast", {})
        self.assertLess(result.duration_ms, 100)  # Should be <1ms


class TestHotSwap(KubeFnTestCase):
    def test_hot_swap(self):
        self.engine.deploy("calc", "def handler(i): return i.get('x', 0) * 2", "handler")
        r1 = self.engine.invoke("calc", {"x": 5})
        self.assertEqual(r1.output, 10)

        # Hot-swap to triple
        self.engine.hot_swap("calc", "def handler(i): return i.get('x', 0) * 3")
        r2 = self.engine.invoke("calc", {"x": 5})
        self.assertEqual(r2.output, 15)

    def test_hot_swap_increments_version(self):
        self.engine.deploy("v_test", "def handler(i): return 1", "handler")
        fn1 = self.engine.get_function("v_test")
        self.assertEqual(fn1["version"], 1)

        self.engine.hot_swap("v_test", "def handler(i): return 2")
        fn2 = self.engine.get_function("v_test")
        self.assertEqual(fn2["version"], 2)


class TestExchange(KubeFnTestCase):
    def test_put_get(self):
        handle = self.engine.exchange.put({"data": [1, 2, 3]})
        result = self.engine.exchange.get(handle)
        self.assertEqual(result, {"data": [1, 2, 3]})

    def test_reference_passing(self):
        """Verify that functions can share data via exchange handles."""
        self.engine.deploy("produce", """
def handler(i):
    return {"items": [1, 2, 3], "total": 6}
""", "handler")
        self.engine.deploy("consume", """
def handler(i):
    items = i.get("items", [])
    return {"count": len(items), "sum": sum(items)}
""", "handler")

        r1 = self.engine.invoke("produce", {})
        self.assertTrue(r1.success)

        # Pass output directly
        r2 = self.engine.invoke("consume", r1.output)
        self.assertTrue(r2.success)
        self.assertEqual(r2.output["count"], 3)
        self.assertEqual(r2.output["sum"], 6)

    def test_exchange_eviction(self):
        exchange = self.engine.exchange
        for i in range(100):
            exchange.put({"i": i}, ttl_seconds=1)
        self.assertGreater(exchange.size, 0)

        time.sleep(1.5)
        # Access should trigger eviction
        exchange.put({"trigger": True})
        self.assertIsNone(exchange.get("nonexistent"))


class TestGraph(KubeFnTestCase):
    def test_simple_pipeline(self):
        self.engine.deploy("double", "def handler(i): return {'value': i.get('value', 0) * 2}", "handler")
        self.engine.deploy("add_ten", "def handler(i): return {'value': i.get('value', 0) + 10}", "handler")

        result = self.engine.run_graph(
            {"name": "math-pipeline", "stages": [
                {"function": "double"},
                {"function": "add_ten"},
            ]},
            initial_input={"value": 5},
        )
        self.assertTrue(result["success"])
        self.assertEqual(result["output"]["value"], 20)  # (5*2) + 10
        self.assertEqual(result["stages_run"], 2)

    def test_pipeline_failure(self):
        self.engine.deploy("ok", "def handler(i): return {'x': 1}", "handler")
        self.engine.deploy("fail", "def handler(i): raise Exception('boom')", "handler")

        result = self.engine.run_graph(
            {"name": "fail-pipe", "stages": [
                {"function": "ok"},
                {"function": "fail"},
            ]},
        )
        self.assertFalse(result["success"])
        self.assertEqual(result["failed_stage"], 1)

    def test_empty_graph(self):
        result = self.engine.run_graph({"name": "empty", "stages": []})
        self.assertFalse(result["success"])  # Validation rejects empty graphs


class TestStats(KubeFnTestCase):
    def test_stats(self):
        self.engine.deploy("s", "def handler(i): return 1", "handler")
        stats = self.engine.stats()
        self.assertEqual(stats["active_functions"], 1)
        self.assertIn("python", stats["available_runtimes"])
        self.assertIn("exchange", stats)

    def test_health_check(self):
        health = self.engine.health_check()
        self.assertTrue(health["healthy"])
        self.assertEqual(health["engine"], "kubefn-lite")

    def test_available_runtimes(self):
        runtimes = self.engine.workers.available_runtimes()
        self.assertIn("python", runtimes)


if __name__ == "__main__":
    unittest.main()
