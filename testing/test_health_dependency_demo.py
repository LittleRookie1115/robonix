# SPDX-License-Identifier: MulanPSL-2.0

import http.client
import json
from pathlib import Path
import runpy
import threading
import unittest
from http.server import ThreadingHTTPServer


class StreamingHealthFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.fixture = runpy.run_path(str(Path(__file__).with_name("demo_health_dependencies.py")))
        except ModuleNotFoundError as error:
            raise unittest.SkipTest("Build Webots Python bindings before testing the manual fixture") from error

    def test_model_prefix_can_pause_before_the_unsafe_call(self):
        handler = self.fixture["DependencyModel"]
        handler.stage = "streaming_prefix"
        handler.partial_started = threading.Event()
        handler.partial_release = threading.Event()
        handler.partial_tail_sent = threading.Event()
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
        request = {"messages": [{"role": "user", "content":
                   "## Tasks (current status board)\n- T1: in_progress\n"}]}
        try:
            connection.request("POST", "/v1/chat/completions", json.dumps(request),
                               {"Content-Type": "application/json"})
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            prefix = json.loads(response.readline().decode().removeprefix("data: "))[
                "choices"][0]["delta"]["content"]
            response.readline()
            self.assertTrue(handler.partial_started.wait(timeout=1))
            self.assertIn("STREAM_PREFIX_OK", prefix)
            self.assertNotIn("explore.explore_explore", prefix)
            self.assertFalse(handler.partial_tail_sent.is_set())
            handler.partial_release.set()
            tail = json.loads(response.readline().decode().removeprefix("data: "))[
                "choices"][0]["delta"]["content"]
            complete = json.loads(prefix + tail)
            self.assertEqual(complete["rtdl_parent_task_id"], "T1")
            self.assertEqual(complete["rtdl"]["children"][0]["cap"], "executor.builtin_run_command")
            self.assertEqual(complete["rtdl"]["children"][1]["cap"], "explore.explore_explore")
        finally:
            handler.partial_release.set()
            connection.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_generated_executor_bindings_include_the_stream_input(self):
        message = self.fixture["executor_pb2"].ExecuteStreamInput(
            message_kind=0, plan_id="fixture-stream", session_id="fixture-session")
        restored = type(message).FromString(message.SerializeToString())
        self.assertEqual(restored.plan_id, "fixture-stream")
        self.assertEqual(restored.message_kind, 0)


if __name__ == "__main__":
    unittest.main()
