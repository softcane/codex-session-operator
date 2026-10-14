"""Regression tests for demo conversation requests and completion evidence."""

import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error

spec = importlib.util.spec_from_file_location("demo_client", Path(__file__).with_name("client.py"))
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)
runner_spec = importlib.util.spec_from_file_location("demo_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(runner_spec)
runner_spec.loader.exec_module(runner)


class DemoTransportRecoveryTests(unittest.TestCase):
    def test_lost_response_reuses_exact_key_path_and_body(self):
        body = {"route_id": "investigate", "text": "Evidence\r\nunchanged", "title": "Checkout"}
        receipt = {"conversation_id": "conv-example", "acceptance": "committed"}
        accepted, deliveries = {}, []

        def deliver(path, method, value, key):
            deliveries.append((path, method, dict(value), key))
            if key not in accepted:
                accepted[key] = receipt
                value["text"] = "mutated local copy"
                raise urllib.error.URLError(ConnectionResetError("response lost after commit"))
            return 202, accepted[key]

        adapter = Mock()
        adapter.request.side_effect = deliver
        with patch.object(runner.time, "sleep"):
            result = runner.ReplaySafeAPI(adapter).request("/conversations", "POST", body, "stable-create")
        self.assertEqual(result, (202, receipt))
        self.assertEqual(len(accepted), 1)
        self.assertEqual(deliveries, [("/conversations", "POST", body, "stable-create")] * 2)
        self.assertEqual(body["text"], "Evidence\r\nunchanged")

    def test_get_retries_without_a_key(self):
        adapter = Mock()
        adapter.request.side_effect = [OSError("tunnel unavailable"), (200, {"history": {"turns": []}})]
        with patch.object(runner.time, "sleep") as sleep:
            result = runner.ReplaySafeAPI(adapter).request("/conversation")
        self.assertEqual(result[0], 200)
        self.assertEqual(adapter.request.call_count, 2)
        self.assertEqual(adapter.request.call_args_list[0], adapter.request.call_args_list[1])
        sleep.assert_called_once_with(0.5)

    def test_unkeyed_post_and_other_mutations_are_never_retried(self):
        for method, key in (("POST", None), ("POST", ""), ("DELETE", "key"), ("PATCH", "key")):
            with self.subTest(method=method, key=key):
                adapter = Mock()
                adapter.request.side_effect = OSError("ambiguous request")
                with patch.object(runner.time, "sleep") as sleep, self.assertRaises(OSError):
                    runner.ReplaySafeAPI(adapter).request("/work-events", method, {"text": "Request"}, key)
                adapter.request.assert_called_once()
                sleep.assert_not_called()

    def test_persistent_transport_failure_has_bounded_attempts(self):
        adapter = Mock()
        adapter.request.side_effect = urllib.error.URLError("tunnel remains unavailable")
        with patch.object(runner.time, "sleep") as sleep, self.assertRaises(urllib.error.URLError):
            runner.ReplaySafeAPI(adapter).request("/conversations", "POST", {"text": "Request"}, "key")
        self.assertEqual(adapter.request.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertTrue(all(call == adapter.request.call_args_list[0] for call in adapter.request.call_args_list))

    def test_http_rejections_are_not_retried(self):
        adapter = Mock()
        error = urllib.error.HTTPError("https://localhost/conversations", 409, "Conflict", {}, None)
        self.addCleanup(error.close)
        adapter.request.side_effect = error
        with patch.object(runner.time, "sleep") as sleep, self.assertRaises(urllib.error.HTTPError):
            runner.ReplaySafeAPI(adapter).request("/conversations", "POST", {"text": "Changed"}, "key")
        adapter.request.assert_called_once()
        sleep.assert_not_called()

    def test_gateway_replacement_waits_for_old_pods_to_disappear(self):
        class Checks:
            def __init__(self):
                self.calls, self.observed = [], []
                self.pods = iter([["old-1", "old-2"], ["old-2", "new-1", "new-2"], ["new-1", "new-2"]])

            def kubectl(self, *args):
                self.calls.append(args)
                if args[:2] == ("get", "deployment"):
                    return json.dumps({"spec": {"selector": {"matchLabels": {"app": "gateway", "component": "api"}}}})
                if args[:2] == ("get", "pods"):
                    return json.dumps({"items": [{"metadata": {"name": name}} for name in next(self.pods)]})
                return ""

            def wait(self, _label, callback, timeout):
                self.timeout = timeout
                for _ in range(2):
                    result = callback()
                    self.observed.append(result)
                    if result:
                        return
                raise RuntimeError("old Pods still exist")

        checks = Checks()
        runner.replace_gateway_pods(checks)
        self.assertEqual(checks.observed, [False, True])
        self.assertEqual(checks.timeout, 90)
        self.assertEqual(checks.calls[1], ("get", "pods", "-n", runner.NS, "-l", "app=gateway,component=api", "-o", "json"))
        self.assertEqual(checks.calls[2][:2], ("rollout", "restart"))
        self.assertEqual(checks.calls[3][:2], ("rollout", "status"))


class DemoCompletionTests(unittest.TestCase):
    def test_acceptance_or_empty_history_is_not_completion(self):
        for value in ({"acceptance": "committed"}, {"history": {"turns": None}},
                      {"history": {"turns": []}}, {"history": {"turns": [{"status": "in_progress"}]}}):
            with self.subTest(value=value):
                self.assertFalse(client.completed_history(value, 1))

    def test_completed_root_turn_count_proves_new_input(self):
        value = {"history": {"turns": [{"status": "completed"},
                  {"status": "completed", "subagent_id": "child"}]}}
        self.assertTrue(client.completed_history(value, 1))
        self.assertFalse(client.completed_history(value, 2))
        value["history"]["turns"].append({"status": "completed"})
        self.assertTrue(client.completed_history(value, 2))

    def test_saved_failed_or_cancelled_turn_fails_wait(self):
        for status in ("failed", "cancelled"):
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                client.completed_history({"history": {"turns": [{"status": status}]}}, 1)

    def test_existing_work_history_remains_supported(self):
        value = {"conversation": {"history": {"turns": [{"status": "completed"}]}}}
        self.assertTrue(client.completed_history(value, 1))

    def test_closed_conversation_requires_verified_cleanup(self):
        for summary in ({"conversation_id": "conv-example", "state": "closed"},
                        {"conversation_id": "conv-example", "state": "closing", "cleaned": True},
                        {"conversation_id": "conv-example", "state": "closed", "cleaned": False}):
            with self.subTest(summary=summary):
                self.assertFalse(client.cleaned_up({"summary": summary}))
        self.assertTrue(client.cleaned_up({"summary": {
            "conversation_id": "conv-example", "state": "closed", "cleaned": True}}))

    def test_existing_work_wait_checks_all_attempts(self):
        value = {"summary": {"state": "closed"}, "work": {"attempts": [{"cleaned": True}, {"cleaned": False}]}}
        self.assertFalse(client.cleaned_up(value))
        value["work"]["attempts"][1]["cleaned"] = True
        self.assertTrue(client.cleaned_up(value))
        self.assertFalse(client.cleaned_up({"summary": {"state": "closed"}}))


class DemoConversationRequestTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="demo-client-test-")
        self.addCleanup(self.directory.cleanup)
        self.base = "/v1/namespaces/test-demo"
        self.path = self.base + "/conversations/conv-example"
        Path(self.directory.name, "curl.conf").write_text('header = "Authorization: Bearer test-placeholder"\n')

    def invoke(self, arguments, responses=None):
        captured = []
        responses = iter(responses or [(202, {"acceptance": "committed"})])

        def respond(request, **_kwargs):
            captured.append(request)
            status, value = next(responses)
            response = io.BytesIO(json.dumps(value).encode())
            response.status = status
            return response

        argv = ["demo-client.py", *arguments, "--demo-dir", self.directory.name]
        with patch.object(client.sys, "argv", argv), patch.dict(os.environ, {"DEMO_NAMESPACE": "test-demo"}), \
                patch.object(client.ssl, "create_default_context"), \
                patch.object(client.urllib.request, "urlopen", side_effect=respond), \
                patch.object(client.sys, "stdout", io.StringIO()), patch.object(client.sys, "stderr", io.StringIO()), \
                patch.object(client.time, "sleep"):
            result = client.main()
        return result, captured

    def test_create_uses_profile_and_retained_key(self):
        result, requests = self.invoke(["conversation", "investigate-code", "--text", "Investigate", "--title", "Checkout",
                                       "--key", "create-1"])
        self.assertEqual(result, 0)
        self.assertEqual(len(requests), 1)
        request = requests[0]
        self.assertEqual(request.full_url, "https://127.0.0.1:18082" + self.base + "/conversations")
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Idempotency-key"), "create-1")
        self.assertEqual(json.loads(request.data), {"route_id": "investigate-code", "text": "Investigate", "title": "Checkout"})

    def test_conversation_mutations_use_returned_identity(self):
        for action, body in (("message", {"text": "Evidence"}), ("close", {}), ("restart", {"text": "Evidence"})):
            with self.subTest(action=action):
                arguments = [action, self.path, "--key", "operation-1"]
                if action != "close":
                    arguments += ["--text", "Evidence"]
                result, requests = self.invoke(arguments)
                self.assertEqual(result, 0)
                self.assertEqual(requests[0].full_url, "https://127.0.0.1:18082" + self.path +
                                 ("/messages" if action == "message" else "/" + action))
                self.assertEqual(requests[0].get_header("Idempotency-key"), "operation-1")
                self.assertEqual(json.loads(requests[0].data), body)

    def test_restart_accepts_conversation_id(self):
        result, requests = self.invoke(["restart", "conv-example", "--text", "Fresh", "--key", "restart-1"])
        self.assertEqual(result, 0)
        self.assertTrue(requests[0].full_url.endswith(self.path + "/restart"))

    def test_message_accepts_returned_conversation_id(self):
        identifier = "conv-" + "a" * 64
        result, requests = self.invoke(["message", identifier, "--text", "Evidence", "--key", "message-1"])
        self.assertEqual(result, 0)
        self.assertTrue(requests[0].full_url.endswith(self.base + "/conversations/" + identifier + "/messages"))

    def test_mutation_rejects_cross_namespace_or_nested_url(self):
        for path in (self.path.replace("test-demo", "other"), self.path + "/messages", "../conv-example",
                     self.path + "?x=1", self.path + "%2fmessages"):
            with self.subTest(path=path), self.assertRaises(SystemExit) as raised:
                self.invoke(["close", path, "--key", "close-1"])
            self.assertEqual(raised.exception.code, 2)

    def test_changes_require_client_supplied_key(self):
        for action in ("conversation", "message", "close", "restart"):
            with self.subTest(action=action), self.assertRaises(SystemExit) as raised:
                self.invoke([action, "example", "--text", "Request"])
            self.assertEqual(raised.exception.code, 2)

    def test_wait_only_reads_and_never_resubmits_input(self):
        result, requests = self.invoke(["get", self.path, "--wait-for", "completed", "--turns", "2"], responses=[
            (200, {"history": {"turns": [{"status": "completed"}]}}),
            (200, {"history": {"turns": [{"status": "completed"}, {"status": "completed"}]}})])
        self.assertEqual(result, 0)
        self.assertEqual(len(requests), 2)
        self.assertTrue(all(request.method == "GET" and request.data is None for request in requests))

    def test_existing_session_message_path_is_preserved(self):
        result, requests = self.invoke(["message", "manual-session", "--text", "Request", "--key", "message-1"])
        self.assertEqual(result, 0)
        self.assertTrue(requests[0].full_url.endswith(self.base + "/sessions/manual-session/messages"))


if __name__ == "__main__":
    unittest.main()
