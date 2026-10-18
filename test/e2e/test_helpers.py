"""Regression tests for acceptance transport and completion evidence."""

import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import urllib.error

runner_spec = importlib.util.spec_from_file_location("demo_runner", Path(__file__).with_name("run.py"))
runner = importlib.util.module_from_spec(runner_spec)
runner_spec.loader.exec_module(runner)


class DemoTransportRecoveryTests(unittest.TestCase):
    def test_dead_forward_is_rebound_before_replaying_the_same_keyed_request(self):
        adapter, recover = Mock(), Mock()
        body = {"text": "Accepted task"}
        receipt = {"acceptance": "committed"}
        adapter.request.side_effect = [OSError("port-forward exited"), (202, receipt)]
        with patch.object(runner.time, "sleep"):
            result = runner.ReplaySafeAPI(adapter, recover_transport=recover).request("/conversations", "POST", body, "stable")
        self.assertEqual(result, (202, receipt))
        recover.assert_called_once_with()
        self.assertEqual(adapter.request.call_args_list[0], adapter.request.call_args_list[1])

    def test_rebinding_does_not_hide_gateway_failure(self):
        adapter, recover = Mock(), Mock(side_effect=RuntimeError("gateway never became healthy"))
        adapter.request.side_effect = OSError("port-forward exited")
        with self.assertRaisesRegex(RuntimeError, "gateway never became healthy"):
            runner.ReplaySafeAPI(adapter, recover_transport=recover).request("/conversation")
        adapter.request.assert_called_once()
        recover.assert_called_once_with()

    def test_application_rejection_and_unkeyed_mutation_do_not_rebind(self):
        recover, adapter = Mock(), Mock()
        adapter.request.return_value = (503, {"error": "application unavailable"})
        self.assertEqual(runner.ReplaySafeAPI(adapter, recover_transport=recover).request("/conversation")[0], 503)
        recover.assert_not_called()
        adapter.request.side_effect = OSError("ambiguous unkeyed input")
        with self.assertRaises(OSError):
            runner.ReplaySafeAPI(adapter, recover_transport=recover).request("/conversations", "POST", {"text": "task"})
        recover.assert_not_called()

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
                    runner.ReplaySafeAPI(adapter).request("/conversations", method, {"text": "Request"}, key)
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


class DemoGatewayForwardTargetTests(unittest.TestCase):
    def deployment(self):
        return {"metadata": {"generation": 2},
                "spec": {"replicas": 2, "selector": {"matchLabels": {"app": "gateway"}}},
                "status": {"observedGeneration": 2, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2}}

    def pod(self, name):
        return {"metadata": {"name": name, "uid": name + "-uid"}, "status": {"phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{"name": "gateway", "ready": True, "restartCount": 0, "state": {"running": {"startedAt": "now"}}}]}}

    def test_forward_selects_a_current_healthy_pod_and_rejects_terminating_targets(self):
        terminating, healthy = self.pod("a-old"), self.pod("c-healthy")
        terminating["metadata"]["deletionTimestamp"] = "now"
        checks = Mock()
        checks.kubectl.side_effect = [json.dumps(self.deployment()), json.dumps({"items": [terminating, healthy, self.pod("d-healthy")]})]
        self.assertEqual(runner.ready_gateway_pod(checks), "c-healthy")

    def test_incomplete_rollout_and_crashed_application_cannot_be_used_for_rebinding(self):
        checks = Mock()
        deployment = self.deployment()
        deployment["status"]["observedGeneration"] = 1
        checks.kubectl.return_value = json.dumps(deployment)
        self.assertIsNone(runner.ready_gateway_pod(checks))
        checks.kubectl.assert_called_once()
        crashed = self.pod("crashed")
        crashed["status"]["containerStatuses"][0]["ready"] = False
        crashed["status"]["containerStatuses"][0]["state"] = {"waiting": {"reason": "CrashLoopBackOff"}}
        checks.kubectl.side_effect = [json.dumps(self.deployment()), json.dumps({"items": [self.pod("a-healthy"), crashed]})]
        self.assertIsNone(runner.ready_gateway_pod(checks))

    def test_healthy_restart_baseline_allows_cold_start_recovery_but_rejects_later_crashes(self):
        checks, baseline = Mock(), {}
        first, second = self.pod("a-gateway"), self.pod("b-gateway")
        first["status"]["containerStatuses"][0]["restartCount"] = 2
        checks.kubectl.side_effect = [json.dumps(self.deployment()), json.dumps({"items": [first, second]})]
        self.assertEqual(runner.ready_gateway_pod(checks, baseline), "a-gateway")
        self.assertEqual(baseline[first["metadata"]["uid"]], {"gateway": 2})
        first["status"]["containerStatuses"][0]["restartCount"] = 3
        checks.kubectl.side_effect = [json.dumps(self.deployment()), json.dumps({"items": [first, second]})]
        with self.assertRaisesRegex(RuntimeError, "restarted after healthy observation"):
            runner.ready_gateway_pod(checks, baseline)

    def test_forward_binds_selected_pod_and_preserves_tunnel_diagnostics(self):
        checks, log = Mock(), io.StringIO()
        checks.cluster, checks.env = "codex-e2e-0123456789ab", {}
        with patch.object(runner.helpers.subprocess, "Popen") as launch:
            runner.helpers.start_forward(checks, 18082, pod="gateway-current", log=log)
        self.assertIn("pod/gateway-current", launch.call_args.args[0])
        self.assertNotIn("service/codex-session-operator-gateway", launch.call_args.args[0])
        self.assertIs(launch.call_args.kwargs["stdout"], log)
        self.assertEqual(launch.call_args.kwargs["stderr"], runner.helpers.subprocess.STDOUT)


class DemoCompletionTests(unittest.TestCase):
    def test_acceptance_or_empty_history_is_not_completion(self):
        for value in ({"acceptance": "committed"}, {"history": {"turns": None}},
                      {"history": {"turns": []}}, {"history": {"turns": [{"status": "in_progress"}]}}):
            with self.subTest(value=value):
                self.assertFalse(runner.helpers.completed_history(value, 1))

    def test_completed_root_turn_count_proves_new_input(self):
        value = {"history": {"turns": [{"status": "completed"},
                  {"status": "completed", "subagent_id": "child"}]}}
        self.assertTrue(runner.helpers.completed_history(value, 1))
        self.assertFalse(runner.helpers.completed_history(value, 2))
        value["history"]["turns"].append({"status": "completed"})
        self.assertTrue(runner.helpers.completed_history(value, 2))

    def test_saved_failed_or_cancelled_turn_fails_wait(self):
        for status in ("failed", "cancelled"):
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                runner.helpers.completed_history({"history": {"turns": [{"status": status}]}}, 1)

    def test_closed_conversation_requires_verified_cleanup(self):
        for summary in ({"conversation_id": "conv-example", "state": "closed"},
                        {"conversation_id": "conv-example", "state": "closing", "cleaned": True},
                        {"conversation_id": "conv-example", "state": "closed", "cleaned": False}):
            with self.subTest(summary=summary):
                self.assertFalse(runner.helpers.cleaned_up({"summary": summary}))
        self.assertTrue(runner.helpers.cleaned_up({"summary": {
            "conversation_id": "conv-example", "state": "closed", "cleaned": True}}))

if __name__ == "__main__":
    unittest.main()
