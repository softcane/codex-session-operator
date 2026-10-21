"""Retained live access and cleanup safety, without provider/cluster mutations."""

import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import urllib.error

import live
import retained


class RetainedAccessTests(unittest.TestCase):
    def config(self, directory):
        return {"run": "abc123", "cluster": "codex-sandbox-live-abc123", "namespace": "agents", "port": 12345,
                "origin": "https://127.0.0.1:12345", "catalogPath": "/v1/namespaces/agents/catalog",
                "conversationID": "conversation", "conversationPath": "/v1/namespaces/agents/conversations/conversation",
                "kubeconfig": str(directory / "kubeconfig"),
                "caFile": str(directory / "ca.crt"), "forwardState": str(directory / "forward.json"),
                "forwardLog": str(directory / "forward.log"), "scope": "owned/scope",
                "session": {"name": "session", "uid": "session-uid", "remoteID": "sess_owned"},
                "execution": {}, "resources": []}

    def test_private_files_are_owner_only_and_symlinks_are_not_followed(self):
        with tempfile.TemporaryDirectory() as name:
            directory = Path(name)
            original = os.umask(0)
            try:
                retained.private_write(directory / "token", "controlled-test-token")
            finally:
                os.umask(original)
            self.assertEqual(stat.S_IMODE((directory / "token").stat().st_mode), 0o600)
            self.assertEqual(retained.private_read(directory / "token"), "controlled-test-token")
            (directory / "link").symlink_to(directory / "token")
            with self.assertRaises(OSError):
                retained.private_read(directory / "link")
            (directory / "token").chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "owner-only"):
                retained.private_read(directory / "token")

    def test_forward_detaches_with_no_terminal_and_writes_owned_process_record(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            process = Mock(pid=4455)
            process.poll.return_value = None
            with patch.object(retained, "request", side_effect=[urllib.error.URLError("not listening"), (200, {})]), \
                    patch.object(retained.subprocess, "Popen", return_value=process) as spawn, \
                    patch.object(retained, "process_identity", return_value={"executable": "kubectl", "startTime": "fixed-start", "command": " ".join(retained.forward_command(config))}):
                state = retained.connect(config)
            self.assertEqual(state["pid"], 4455)
            arguments, options = spawn.call_args
            self.assertEqual(arguments[0], retained.forward_command(config))
            self.assertIn("127.0.0.1", arguments[0])
            self.assertTrue(options["start_new_session"])
            self.assertTrue(options["close_fds"])
            self.assertEqual(options["stdin"], subprocess.DEVNULL)
            self.assertEqual(options["stderr"], subprocess.STDOUT)
            self.assertEqual(json.loads(retained.private_read(config["forwardState"])), state)
            self.assertEqual(stat.S_IMODE(Path(config["forwardLog"]).stat().st_mode), 0o600)

    def test_reused_pid_is_never_signalled(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            retained.save_state(config["forwardState"], {"pid": 4455, "command": retained.forward_command(config)})
            with patch.object(retained.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout="unrelated-user-program")), \
                    patch.object(retained.os, "kill") as kill:
                retained.stop_forward(config)
            kill.assert_not_called()

    def test_substring_pid_false_positive_cannot_signal_unrelated_process(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            expected = {"executable": "kubectl", "startTime": "original", "command": " ".join(retained.forward_command(config))}
            retained.save_state(config["forwardState"], {"pid": 4455, "command": retained.forward_command(config), "processIdentity": expected})
            false_positive = {"executable": "python3", "startTime": "later", "command": "python3 unrelated.py kubectl port-forward kind-" + config["cluster"] + " 12345:8082"}
            with patch.object(retained, "process_identity", return_value=false_positive), patch.object(retained.os, "kill") as kill:
                retained.stop_forward(config)
            kill.assert_not_called()
            self.assertFalse(retained.matches_forward_command(config, {**expected, "command": expected["command"] + " extra-argument"}))

    def test_http_access_needs_no_certificate_or_openssl(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            checks = SimpleNamespace(cluster="codex-sandbox-live-abc123", env={"KUBECONFIG": str(root / "kubeconfig")})
            session = {"metadata": {"namespace": "agents"}, "status": {"podName": "pod", "sandboxName": "sandbox", "sandboxClaimName": "claim"}}
            journey = SimpleNamespace(report={"run": "abc123", "model": "gpt-6-luna"}, scope="scope", resources={}, attempts={"conv": {
                "name": "session", "uid": "uid", "remoteID": "remote", "path": "/v1/namespaces/agents/conversations/conv", "session": session}})
            with patch.object(retained.subprocess, "run") as run:
                configuration, config = retained.prepare_access(root, root / "reports", None,
                    checks, journey, 12345, {"claim": {"metadata": {"name": "actual-pvc"}}})
            run.assert_not_called()
            self.assertEqual(config["origin"], "http://127.0.0.1:12345")
            self.assertNotIn("caFile", config)
            self.assertNotIn("certificateExpires", config)
            self.assertNotIn("--ca-file", (configuration.parent / "gateway-client").read_text())

    def test_access_config_and_wrappers_need_no_gateway_credentials(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            certificate = root / "certificate.crt"
            certificate.write_text("controlled certificate")
            checks = SimpleNamespace(cluster="codex-sandbox-live-abc123", env={"KUBECONFIG": str(root / "kubeconfig")})
            session = {"metadata": {"namespace": "agents"}, "status": {"podName": "pod", "sandboxName": "sandbox", "sandboxClaimName": "claim"}}
            journey = SimpleNamespace(report={"run": "abc123", "model": "gpt-6-luna"}, scope="scope", resources={}, attempts={"conv": {
                "name": "session", "uid": "uid", "remoteID": "remote", "path": "/v1/namespaces/agents/conversations/conv", "session": session}})
            with patch.object(retained.subprocess, "run", return_value=SimpleNamespace(stdout="notAfter=expiry")):
                configuration, config = retained.prepare_access(root, root / "reports", certificate,
                                                                 checks, journey, 12345, {"claim": {"metadata": {"name": "actual-pvc"}}})
            self.assertNotIn("controlled-test-token", configuration.read_text())
            self.assertEqual(config["execution"]["pvc"], "actual-pvc")
            self.assertEqual(config["certificateExpires"], "expiry")
            self.assertEqual(stat.S_IMODE(configuration.parent.stat().st_mode), 0o700)
            self.assertFalse((configuration.parent / "gateway-token").exists())
            for path in configuration.parent.iterdir():
                self.assertNotIn("controlled-test-token", path.read_text())
            wrapper = (configuration.parent / "gateway-client").read_text()
            self.assertNotIn("--token-file", wrapper)
            self.assertNotIn("tokenFile", config)
            self.assertIn("--state-dir", wrapper)
            self.assertIn("continue conv", (configuration.parent / "send-message").read_text())

    def test_additional_conversations_prevent_close_or_cluster_deletion(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            with patch.object(retained, "connect"), \
                    patch.object(retained, "request", return_value=(200, {"conversations": [{"conversation_id": "another"}], "more": False})) as request, \
                    patch.object(retained, "kubectl") as kubectl, patch.object(retained.subprocess, "run") as run:
                with self.assertRaisesRegex(RuntimeError, "Additional conversations"):
                    retained.cleanup(config)
            self.assertEqual(request.call_count, 1)
            kubectl.assert_not_called()
            run.assert_not_called()

    def test_unconfirmed_remote_absence_never_deletes_dependencies_or_cluster(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            secret = {"data": {"OPENAI_API_KEY": "Y29udHJvbGxlZC1rZXk="}}
            remote = {"id": "sess_owned", "metadata": {"operator": "codex-session-operator", "kubernetes_uid": "session-uid", "owner_scope": "owned/scope"}}
            def local(_, kind, __):
                return secret if kind == "secret" else None
            def wait(label, observe, timeout=300):
                if not observe():
                    raise RuntimeError(label + " unconfirmed")
            with patch.object(retained, "connect"), patch.object(retained, "verify_conversations"), \
                    patch.object(retained, "local_resource", side_effect=local), patch.object(retained, "provider_get", return_value=remote), \
                    patch.object(retained, "request", return_value=(202, {})), patch.object(retained, "wait_for", side_effect=wait), \
                    patch.object(retained, "kubectl", return_value='{"items":[]}') as kubectl, patch.object(retained.subprocess, "run") as run:
                with self.assertRaisesRegex(RuntimeError, "Remote Session absence"):
                    retained.cleanup(config)
            self.assertTrue(all(call.args[1] == "get" for call in kubectl.call_args_list))
            run.assert_not_called()

    def test_changed_remote_owner_refuses_gateway_close(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            secret = {"data": {"OPENAI_API_KEY": "Y29udHJvbGxlZC1rZXk="}}
            with patch.object(retained, "connect"), patch.object(retained, "verify_conversations"), patch.object(retained, "verify_inventory"), \
                    patch.object(retained, "local_resource", side_effect=lambda _, kind, __: secret if kind == "secret" else None), \
                    patch.object(retained, "provider_get", return_value={"id": "sess_owned", "metadata": {}}), \
                    patch.object(retained, "request") as request:
                with self.assertRaisesRegex(RuntimeError, "ownership differs"):
                    retained.cleanup(config)
            request.assert_not_called()

    def test_cleanup_requires_empty_inventory_before_kind_delete(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            secret = {"data": {"OPENAI_API_KEY": "Y29udHJvbGxlZC1rZXk="}}
            with patch.object(retained, "connect"), patch.object(retained, "verify_conversations"), \
                    patch.object(retained, "local_resource", side_effect=lambda _, kind, __: secret if kind == "secret" else None), \
                    patch.object(retained, "provider_get", return_value=None), patch.object(retained, "request", return_value=(202, {})), \
                    patch.object(retained, "kubectl", return_value='{"items":[{"kind":"Session"}]}'), \
                    patch.object(retained.subprocess, "run") as run:
                with self.assertRaisesRegex(RuntimeError, "Additional API resources"):
                    retained.cleanup(config)
            run.assert_not_called()

    def test_direct_extra_session_blocks_before_close_or_shared_dependency_delete(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            config["resources"] = [{"kind": "Agent", "name": "shared", "uid": "agent-uid"}]
            inventory = {"items": [{"kind": "Agent", "metadata": {"namespace": "agents", "name": "shared", "uid": "agent-uid"}},
                                   {"kind": "Session", "metadata": {"namespace": "agents", "name": "another", "uid": "another-uid"}}]}
            with patch.object(retained, "connect"), patch.object(retained, "verify_conversations"), \
                    patch.object(retained, "kubectl", return_value=json.dumps(inventory)) as kubectl, \
                    patch.object(retained, "request") as request, patch.object(retained, "local_resource") as local:
                with self.assertRaisesRegex(RuntimeError, "Additional API resources"):
                    retained.cleanup(config)
            request.assert_not_called()
            local.assert_not_called()
            self.assertTrue(all(call.args[1] == "get" for call in kubectl.call_args_list))

    def test_confirmed_cleanup_deletes_only_owned_cluster_after_absence_checks(self):
        with tempfile.TemporaryDirectory() as name:
            config = self.config(Path(name))
            secret = {"data": {"OPENAI_API_KEY": "Y29udHJvbGxlZC1rZXk="}}
            observed = []
            def local(_, kind, resource):
                observed.append((kind, resource))
                return secret if kind == "secret" else None
            with patch.object(retained, "connect"), patch.object(retained, "verify_conversations") as discover, \
                    patch.object(retained, "local_resource", side_effect=local), patch.object(retained, "provider_get", return_value=None) as provider, \
                    patch.object(retained, "request", return_value=(202, {})) as request, \
                    patch.object(retained, "kubectl", return_value='{"items":[]}'), patch.object(retained, "stop_forward") as stop, \
                    patch.object(retained.subprocess, "run", return_value=SimpleNamespace(returncode=0)) as run:
                retained.cleanup(config)
            self.assertIn(("session", "session"), observed)
            self.assertEqual(provider.call_count, 2)
            self.assertEqual(discover.call_count, 2)
            self.assertEqual(request.call_args.args[2:4], ("POST", {}))
            self.assertEqual(run.call_args.args[0], ["kind", "delete", "cluster", "--name", config["cluster"]])
            stop.assert_called_once_with(config)
            self.assertTrue(json.loads(retained.private_read(Path(name) / "cleanup.json"))["remoteAbsenceConfirmed"])

    def test_keep_is_restricted_to_single_session_before_any_resources(self):
        with patch.object(sys, "argv", ["live.py", "--keep"]), patch.object(live, "report_path") as path:
            with self.assertRaisesRegex(RuntimeError, "requires --single-session"):
                live.main()
            path.assert_not_called()

    def test_single_session_keeps_conversation_open_only_when_explicitly_requested(self):
        for keep in (False, True):
            with self.subTest(keep=keep):
                report = {"run": "run", "scope": "scope", "model": "gpt-6-luna", "scenarios": []}
                checks = Mock()
                checks.get.return_value = None
                provider = Mock()
                provider.get.return_value = {"id": "remote", "metadata": {"operator": "codex-session-operator", "kubernetes_uid": "uid", "owner_scope": "scope"}, "agent": {"model": "gpt-6-luna"}}
                journey = live.Journey(checks, Mock(), provider, Mock(), report, "image", "arm64", Mock(), without_session_budget=True)
                journey.stage = Mock()
                journey.publish = Mock()
                journey.operation = Mock(return_value={"conversation_url": "/conversation"})
                journey.ready = Mock(return_value=({}, {"metadata": {"uid": "uid"}, "status": {"openaiSessionID": "remote"}}, {"metadata": {"name": "pod", "uid": "pod-uid"}}, {"metadata": {"name": "pvc", "uid": "pvc-uid"}}))
                journey.command_history = Mock(return_value=({"history": {"turns": [{"id": "root", "status": "completed"}], "items": [{"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "done"}]}]}}, ["command"]))
                journey.read_exec = Mock(return_value=live.REPOSITORY_COMMIT)
                journey.exec = Mock(side_effect=["ok github.com/google/uuid", ""])
                journey.close = Mock()
                with patch.object(live.sandbox_resources, "resources", return_value=[]):
                    journey.exercise_single(keep=keep)
                self.assertEqual(journey.close.call_count, 0 if keep else 1)
                self.assertEqual(checks.get.call_count, 0 if keep else 1)


if __name__ == "__main__":
    unittest.main()
