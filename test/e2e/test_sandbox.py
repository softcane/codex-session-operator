"""Safety/evidence regressions for maintained sandbox runners; no clusters or provider calls."""

import io
import copy
from contextlib import redirect_stderr
import json
import os
import sys
import subprocess
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error

import live
import cli
import faults
import reporting
import support


class SandboxEvidenceTests(unittest.TestCase):
    def test_packet_probe_exits_after_outcome_and_never_calls_timeout_a_denial(self):
        captured=[]
        def invoke(*args,**kwargs):
            captured.append((args,kwargs))
            program=args[-2]
            for event,code,output in (("fixtureSocket.emit('error',{code:'ECONNREFUSED'})",0,"denied"),
                                      ("fixtureSocket.emit('connect')",0,"reachable"),
                                      ("fixtureSocket.emit('error',{code:'EBADFD'})",2,"probe-error")):
                stub="const Module=require('node:module'),EventEmitter=require('node:events');const fixtureSocket=new EventEmitter();fixtureSocket.destroy=()=>{};fixtureSocket.setTimeout=()=>{};const original=Module._load;Module._load=(name,...args)=>name==='node:net'?{createConnection:()=>{queueMicrotask(()=>{"+event+"});return fixtureSocket;}}:original(name,...args);setInterval(()=>{},1000);"
                result=subprocess.run(["node","-e",stub+program,"10.0.0.7"],capture_output=True,text=True,timeout=3,check=False)
                self.assertEqual((result.returncode,result.stdout.strip()),(code,"probe-started\n"+output))
            return "probe-started\ndenied"
        checks=SimpleNamespace(kubectl=invoke)
        self.assertEqual(faults.packet_probe(checks,"pod","10.0.0.7"),"denied")
        self.assertEqual(captured[0][1]["timeout"],15)
        checks.kubectl=Mock(side_effect=subprocess.TimeoutExpired(["kubectl"],15))
        with self.assertRaises(subprocess.TimeoutExpired):
            faults.packet_probe(checks,"pod","10.0.0.7")

    def test_interruption_waits_for_saved_matching_running_root_despite_missing_history(self):
        marker={"job":"job","pid":12,"stage":"running"}
        counts={"jobs":[{"id":"job","session_id":"session","turn_id":"root","started":True,"instance":"process"}]}
        for value in (None,{}, {"history":None}, {"history":{"turns":None}}, {"history":{"turns":[]}},
                      {"history":{"turns":[{"id":"old","status":"completed"}]}},
                      {"history":{"turns":[{"id":"root","status":"completed"}]}}):
            with self.subTest(value=value):
                self.assertIsNone(faults.running_root(value,marker,counts,"session"))
        current={"history":{"turns":[{"id":"root","status":"in_progress"}]}}
        self.assertIs(faults.running_root(current,marker,counts,"session"),current)
        self.assertIsNone(faults.running_root(current,marker,{"jobs":[]},"session"))

    def test_current_executor_requires_gateway_eligibility_and_exact_running_instance(self):
        session = {"metadata": {"uid": "session", "namespace": "team", "generation": 1}, "status": {
            "conditions": [{"type": "Ready", "status": "True", "observedGeneration": 1}], "workspaceClaimUID": "claim",
            "workspacePreparation": {"podUID": "pod", "containerID": "runtime", "restartCount": 0, "startedAt": "now",
                                     "configDigest": "configuration", "claimUID": "claim"}}}
        pod = {"metadata": {"uid": "pod", "namespace": "team", "ownerReferences": [{"uid": "session", "controller": True}],
                "annotations": {"agents.softcane.io/executor-config": "configuration"}},
            "spec": {"initContainers": [{"name": "repository-prepare"}]}, "status": {"phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "initContainerStatuses": [{"name": "repository-prepare", "state": {"terminated": {"exitCode": 0}}}],
                "containerStatuses": [{"ready": True, "containerID": "runtime", "restartCount": 0,
                                       "state": {"running": {"startedAt": "now"}}}]}}
        detail = {"summary": {"session_uid": "session"}, "lifecycle": {"execution_ready": True}}
        self.assertTrue(support.prepared_executor(detail, session, pod))
        for field, value in (("containerID", "replacement"), ("restartCount", 1), ("state", {"terminated": {"exitCode": 1}})):
            changed = copy.deepcopy(pod)
            changed["status"]["containerStatuses"][0][field] = value
            with self.subTest(field=field):
                self.assertFalse(support.prepared_executor(detail, session, changed))
        changed = copy.deepcopy(pod)
        changed["metadata"]["uid"] = "replacement"
        self.assertFalse(support.prepared_executor(detail, session, changed))
        self.assertFalse(support.prepared_executor({**detail, "lifecycle": {"execution_ready": False}}, session, pod))

    def test_read_only_transition_retries_never_repeat_program_effects_or_missing_files(self):
        checks = SimpleNamespace(deadline=__import__("time").monotonic()+30, kubectl=Mock(side_effect=[
            support.CommandError("kubectl", 1, 'error: unable to upgrade connection: container not found ("executor")'), "nonce"]))
        with patch.object(support.time, "sleep"):
            self.assertEqual(support.read_only_exec(checks, "pod", "cat", "/workspace/proof.txt"), "nonce")
        self.assertEqual(checks.kubectl.call_count, 2)
        checks.kubectl.reset_mock()
        with self.assertRaisesRegex(RuntimeError, "effectful"):
            support.read_only_exec(checks, "pod", "go", "test", "./...")
        checks.kubectl.assert_not_called()
        checks.kubectl.side_effect = support.CommandError("kubectl", 1, "cat: proof.txt: No such file or directory")
        with self.assertRaises(support.CommandError):
            support.read_only_exec(checks, "pod", "cat", "/workspace/proof.txt")
        self.assertEqual(checks.kubectl.call_count, 1)

    def test_cli_owned_paths_are_canonical_even_below_a_symlinked_temp_ancestor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            actual = root / "actual"
            actual.mkdir(mode=0o700)
            alias = root / "alias"
            alias.symlink_to(actual, target_is_directory=True)
            certificate = actual / "gateway.crt"
            certificate.write_text("fixture certificate")
            private, token, text, state, ca = cli.private_paths(alias, "fixture-token", "fixture input", alias / "gateway.crt")
            self.assertEqual(private, actual / "cli")
            self.assertEqual((token, text, state, ca), (actual / "cli/gateway-token", actual / "cli/task.txt",
                                                       actual / "cli/operations", certificate))
            self.assertEqual(token.stat().st_mode & 0o777, 0o600)
            self.assertEqual(token.read_text(), "fixture-token\n")

    def test_imported_manifest_identity_is_not_a_docker_configuration_hash(self):
        digest = "sha256:" + "a" * 64
        listing = "REF TYPE DIGEST SIZE\ndocker.io/library/executor:test manifest " + digest + " 1MB\n"
        self.assertEqual(support.immutable_reference("executor:test", listing), "docker.io/library/executor@" + digest)
        for changed in (listing + listing.splitlines()[1] + "\n", listing.replace(digest, "configuration-id")):
            with self.subTest(value=changed), self.assertRaises(RuntimeError):
                support.immutable_reference("executor:test", changed)

    def test_public_evidence_excludes_payloads_credentials_and_recovery_journals(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            report = {"result": "failed", "checks": {"cleanup": False}, "error": "private provider body",
                      "recoveryKubeconfig": "/private/kubeconfig", "operations": [{"key": "private-retry", "text": "private task"}],
                      "scenarios": [{"id": "proof", "status": "failed", "finding": "private task"}]}
            reporting.public_report(path, report)
            value = json.loads((path / "public/report.json").read_text())
            self.assertEqual(value, {"result": "failed", "checks": {"cleanup": False}, "scenarios": [{"id": "proof", "status": "failed"}]})
            self.assertEqual((path / "public/report.json").stat().st_mode & 0o777, 0o600)

    def test_public_evidence_preserves_explicit_unbudgeted_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            policy = {"spendingMode": "unlimited", "aggregateLimitCents": None, "allocationCents": None, "spendingLimitQualified": False}
            reporting.public_report(Path(directory), {**policy, "error": "private provider body"})
            self.assertEqual(json.loads((Path(directory) / "public/report.json").read_text()), {**policy, "scenarios": []})


class LiveSandboxSafetyTests(unittest.TestCase):
    def test_spending_policy_is_limited_by_default_and_unbudgeted_only_when_explicit(self):
        journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        self.assertEqual(journey.spending_policy(200), {"mode": "limited", "limitCents": 200})
        journey.budgets["existing"] = 400
        with self.assertRaisesRegex(RuntimeError, "budget exhausted"):
            journey.spending_policy(200)
        explicit = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock(),
                                without_session_budget=True)
        self.assertEqual(explicit.spending_policy(200), {"mode": "unlimited"})
        self.assertEqual(explicit.spending_policy(50), {"mode": "unlimited"})

    def test_spending_readback_requires_exact_cap_or_explicit_absent_null_policy(self):
        self.assertTrue(live.verify_spending({"spend_control": {"limit": 200, "consumed": 0}}, 200))
        for value in ({}, {"spend_control": None}):
            self.assertFalse(live.verify_spending(value, None))
            with self.assertRaisesRegex(RuntimeError, "limited spending cap"):
                live.verify_spending(value, 200)
        for spending in ({"limit": 500, "consumed": 0}, {"limit": 200, "consumed": 200}, {"limit": 200, "consumed": -1},
                         {"limit": 200, "consumed": None}):
            with self.subTest(spending=spending), self.assertRaisesRegex(RuntimeError, "limited spending cap"):
                live.verify_spending({"spend_control": spending}, 200)
            with self.subTest(unbudgeted=spending), self.assertRaisesRegex(RuntimeError, "unexpected spending policy"):
                live.verify_spending({"spend_control": spending}, None)

    def test_changed_limit_or_conflicting_explicit_spending_arguments_are_rejected(self):
        with patch.object(sys, "argv", ["live.py", "--spend-limit-cents", "501"]), patch.object(live, "report_path") as path:
            with self.assertRaisesRegex(RuntimeError, "500 cents"):
                live.main()
            path.assert_not_called()
        with patch.object(sys, "argv", ["live.py", "--without-session-budget", "--spend-limit-cents", "500"]), \
                patch.object(live, "report_path") as path, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as raised:
                live.main()
            self.assertEqual(raised.exception.code, 2)
            path.assert_not_called()

    def test_explicit_unbudgeted_preflight_reports_no_monetary_guarantee(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "report"
            with patch.object(sys, "argv", ["live.py", "--without-session-budget"]), \
                    patch.object(live, "report_path", return_value=destination), \
                    patch.object(live.shutil, "which", return_value="/fixture/tool"), \
                    patch.object(live.local_setup, "configuration_key", side_effect=ValueError("Required private credential is missing")), \
                    patch.object(live.local_setup, "global_configs", return_value={}), \
                    patch.object(live.support.Checks, "run") as run:
                self.assertEqual(live.main(), 1)
            run.assert_not_called()
            report = json.loads((destination / "report.json").read_text())
            self.assertEqual(report["stage"], "preflight-incomplete")
            self.assertEqual(report["spendingMode"], "unlimited")
            self.assertIsNone(report["aggregateLimitCents"])
            self.assertIsNone(report["allocationCents"])
            self.assertFalse(report["spendingLimitQualified"])

    def test_login_shell_toolchain_failure_stops_before_cluster_and_paid_resources(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "report"
            with patch.object(sys, "argv", ["live.py"]), patch.object(live, "report_path", return_value=destination), \
                    patch.object(live.shutil, "which", return_value="/fixture/tool"), \
                    patch.object(live.local_setup, "configuration_key", side_effect=["fixture-application", "fixture-executor"]), \
                    patch.object(live.local_setup, "global_configs", return_value={}), patch.object(live.support, "Checks") as factory:
                checks = factory.return_value
                checks.keys = {"application": "fixture-application", "executor": "fixture-executor"}
                commands = []
                def run(args, **kwargs):
                    commands.append((args, kwargs))
                    if args[:2] == ["docker", "run"]:
                        raise RuntimeError("Login shell Go toolchain unavailable")
                    return ""
                checks.run.side_effect = run
                self.assertEqual(live.main(), 1)
                checks.apply.assert_not_called()
            smoke = [(args, options) for args, options in commands if args[:2] == ["docker", "run"]]
            self.assertEqual(len(smoke), 1)
            args, options = smoke[0]
            self.assertEqual(args[:8], ["docker", "run", "--rm", "--network", "none", "--entrypoint", "/bin/bash", args[7]])
            self.assertTrue(args[7].startswith("my-go-project-executor:live-"))
            self.assertEqual(args[8:], ["-lc", "command -v go && go version && command -v gofmt"])
            self.assertEqual(options, {"timeout": 30})
            self.assertFalse(any(args[:3] == ["kind", "create", "cluster"] for args, _ in commands))
            report = json.loads((destination / "report.json").read_text())
            self.assertEqual(report["result"], "failed")
            self.assertEqual(report["stage"], "production executor login-shell toolchain preflight")
            self.assertNotIn("productionExecutorLoginShellToolchain", report["checks"])

    def project_fixture(self, root):
        environment = {"PATH": os.environ["PATH"], "HOME": str(root), "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1"}
        def run(*args):
            result = subprocess.run(args, cwd=root, env=environment, text=True, capture_output=True, timeout=10, check=False)
            if result.returncode:
                raise RuntimeError("Independent project probe failed")
            return result.stdout.strip()
        (root / "uuid.go").write_text("package uuid\n")
        (root / ".gitignore").write_text("ignored/\n.cache/\n")
        run("git", "init", "-q")
        run("git", "add", ".")
        run("git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-qm", "baseline")
        journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        journey.exec = lambda _pod, *args, **_kwargs: run(*args)
        journey.read_exec = lambda _pod, *args, **_kwargs: run(*args)
        return journey, run

    def test_live_project_oracle_rejects_staged_ignored_and_untracked_source_additions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            journey, run = self.project_fixture(root)
            baseline = journey.project_manifest("executor")
            with patch.object(live, "REPOSITORY_COMMIT", run("git", "rev-parse", "HEAD")):
                (root / "proof.txt").write_text("independent-proof")
                (root / "sandbox-findings.json").write_text("{}")
                (root / ".sandbox-original-uuid.go").write_text("explicit harness backup")
                cache = root / ".cache/go-build"
                cache.mkdir(parents=True)
                (cache / "runtime.go").write_text("runtime cache")
                journey.verify_project("executor", baseline)
                for name in ("extra_test.go", "ignored/extra_test.go"):
                    with self.subTest(name=name):
                        added = root / name
                        added.parent.mkdir(exist_ok=True)
                        added.write_text("package uuid\n")
                        with self.assertRaisesRegex(RuntimeError, "project filesystem"):
                            journey.verify_project("executor", baseline)
                        added.unlink()
                        if name.startswith("ignored/"):
                            added.parent.rmdir()
                (root / "uuid.go").write_text("package uuid\n// staged modification\n")
                run("git", "add", "uuid.go")
                self.assertEqual(run("git", "diff", "--"), "")
                with self.assertRaisesRegex(RuntimeError, "tracked project"):
                    journey.verify_project("executor", baseline)
                run("git", "restore", "--source=HEAD", "--staged", "--worktree", "uuid.go")
                (root / ".gitignore").write_text("ignored/\n.cache/\n# non-Go modification\n")
                with self.assertRaisesRegex(RuntimeError, "tracked project"):
                    journey.verify_project("executor", baseline)
                run("git", "restore", "--", ".gitignore")
                run("git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-qm", "changed HEAD")
                with self.assertRaisesRegex(RuntimeError, "HEAD differs"):
                    journey.verify_project("executor", baseline)

    def test_live_project_manifest_rejects_symlink_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            project = root / "project"
            project.mkdir()
            journey, _ = self.project_fixture(project)
            outside = root / "outside"
            outside.mkdir()
            (outside / "private.go").write_text("must not be read")
            (project / "escape").symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(RuntimeError, "Independent project probe failed"):
                journey.project_manifest("executor")

    def test_live_completion_waits_through_nullable_history_for_a_new_root(self):
        journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        snapshots = [{}, {"history": None}, {"history": {"turns": None}},
                     {"history": {"turns": [{"id": "old", "status": "completed"}]}},
                     {"history": {"turns": [{"id": "child", "subagent_id": "child", "status": "completed"}]}},
                     {"history": {"turns": [{"id": "new", "status": "in_progress"}]}},
                     {"history": {"turns": [{"id": "new", "status": "completed"}]}}]
        journey.detail = Mock(side_effect=snapshots)
        def wait(_label, observe, _timeout):
            for _ in snapshots:
                result = observe()
                if result is not None:
                    return result
            self.fail("New completed root was not observed")
        journey.checks.wait.side_effect = wait
        self.assertIs(journey.completed("/conversation", {"old"}), snapshots[-1])
        self.assertEqual(journey.detail.call_count, len(snapshots))

    def test_default_live_preflight_report_uses_durable_ignored_private_storage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with patch.object(sys, "argv", ["live.py"]), patch.object(live, "ROOT", root), \
                    patch.dict(os.environ, {"CODEX_TEST_REPORT_DIR": ""}), \
                    patch.object(live.shutil, "which", return_value="/fixture/tool"), \
                    patch.object(live.local_setup, "global_configs", return_value={}), \
                    patch.object(live.support.Checks, "run") as run:
                self.assertEqual(live.main(), 1)
            run.assert_not_called()
            reports = list((root / ".private/reports").glob("codex-sandbox-live-*/report.json"))
            self.assertEqual(len(reports), 1)
            report = json.loads(reports[0].read_text())
            self.assertEqual(report["stage"], "preflight-incomplete")
            self.assertEqual(report["spendingMode"], "limited")
            self.assertEqual(report["aggregateLimitCents"], 500)
            self.assertEqual(sum(report["allocationCents"].values()), 500)
            self.assertFalse(report["spendingLimitQualified"])
            self.assertEqual(reports[0].stat().st_mode & 0o777, 0o600)

    def test_live_command_history_waits_for_completed_root_and_fresh_saved_items(self):
        journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        command = {"id": "command", "turn_id": "new", "type": "command_execution", "cwd": "/workspace",
                   "command": "go test ./... && cat proof.txt", "status": "completed", "exit_code": 0,
                   "output": "ok  github.com/google/uuid 0.01s\nindependent-proof"}
        old = {"id": "old", "status": "completed"}
        root = {"id": "new", "status": "completed"}
        snapshots = [{"history": None}, {"history": {"turns": [old], "items": [command]}},
                     {"history": {"turns": [old, {**root, "status": "in_progress"}], "items": [command]}},
                     {"history": {"turns": [old, root], "items": None}},
                     {"history": {"turns": [old, root], "items": [{**command, "status": "in_progress", "exit_code": None, "output": None}]}},
                     {"history": {"turns": [old, root], "items": [command]}}]
        journey.detail = Mock(side_effect=snapshots)
        def wait(_label, observe, timeout):
            self.assertEqual(timeout, 300)
            for _ in snapshots:
                result = observe()
                if result is not None:
                    return result
            self.fail("Complete saved command evidence was not observed")
        journey.checks.wait.side_effect = wait
        self.assertEqual(journey.command_history("/conversation", {"history": {"turns": [old], "items": None}},
                                                 ["go test", "independent-proof"]), (snapshots[-1], ["command"]))
        self.assertEqual(journey.detail.call_count, len(snapshots))
        journey.checks.wait.assert_called_once()

    def test_live_command_history_cannot_qualify_failed_roots_or_expired_missing_items(self):
        for status in ("failed", "cancelled"):
            with self.subTest(status=status):
                journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
                journey.detail = Mock(return_value={"history": {"turns": [{"id": "new", "status": status}], "items": None}})
                journey.checks.wait.side_effect = lambda _label, observe, _timeout: observe()
                with self.assertRaisesRegex(RuntimeError, "failed or was cancelled"):
                    journey.command_history("/conversation", {"history": None}, ["go test"])
                journey.detail.assert_called_once()
        journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        journey.detail = Mock(return_value={"history": {"turns": [{"id": "new", "status": "completed"}], "items": None}})
        def expired(_label, observe, timeout):
            self.assertEqual(timeout, 300)
            self.assertIsNone(observe())
            raise RuntimeError("Timed out: command evidence")
        journey.checks.wait.side_effect = expired
        with self.assertRaisesRegex(RuntimeError, "Timed out"):
            journey.command_history("/conversation", {"history": None}, ["go test"])

    def test_failed_project_test_plus_successful_nonce_read_is_not_project_success(self):
        journey = live.Journey(Mock(), Mock(), Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        before = {"history": {"items": []}}
        commands = [
            {"id": "test", "type": "command_execution", "cwd": "/workspace", "command": "go test ./...", "exit_code": 1,
             "status": "failed", "output": "FAIL github.com/google/uuid"},
            {"id": "nonce", "type": "command_execution", "cwd": "/workspace", "command": "cat proof.txt", "exit_code": 0,
             "status": "completed", "output": "runtime-only-proof"},
        ]
        with self.assertRaisesRegex(RuntimeError, "project test"):
            journey.prove_commands({"history": {"items": commands}}, before, ["go test", "runtime-only-proof"])
        commands[0].update(status="completed", exit_code=0, output="ok  github.com/google/uuid 0.01s")
        self.assertEqual(journey.prove_commands({"history": {"items": commands}}, before, ["go test", "runtime-only-proof"]), ["test", "nonce"])
    def test_remote_mutation_identity_requires_operator_uid_scope_and_exact_id(self):
        value = {"id": "session", "metadata": {"operator": "codex-session-operator", "kubernetes_uid": "uid", "owner_scope": "scope"}}
        live.verify_owner(value, "uid", "scope", "session")
        for field in ("operator", "kubernetes_uid", "owner_scope"):
            changed = {"id": "session", "metadata": {**value["metadata"], field: "foreign"}}
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                live.verify_owner(changed, "uid", "scope", "session")

    def test_provider_absence_is_distinct_from_unavailable_observation(self):
        for code in (404, 503):
            body = io.BytesIO(b"private provider response")
            error = urllib.error.HTTPError("https://example.invalid", code, "failure", {}, body)
            with self.subTest(code=code), patch.object(live.urllib.request, "urlopen", side_effect=error):
                if code == 404:
                    self.assertIsNone(live.Provider("placeholder").get("session", "session"))
                else:
                    with self.assertRaises(RuntimeError) as raised:
                        live.Provider("placeholder").get("session", "session")
                    self.assertNotIn("private provider response", str(raised.exception))
            self.assertTrue(body.closed)

    def test_missing_required_credentials_fails_before_docker_or_provider_and_records_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "report"
            with patch.object(sys, "argv", ["live.py"]), \
                    patch.object(live, "report_path", return_value=destination), \
                    patch.object(live.shutil, "which", return_value="/fixture/tool"), \
                    patch.object(live.local_setup, "configuration_key", side_effect=ValueError("Required private credential is missing")), \
                    patch.object(live.local_setup, "global_configs", return_value={}), \
                    patch.object(live.support.Checks, "run") as run:
                self.assertEqual(live.main(), 1)
            run.assert_not_called()
            report = json.loads((destination / "report.json").read_text())
            self.assertEqual(report["stage"], "preflight-incomplete")
            self.assertEqual(report["result"], "failed")

    def test_unknown_accepted_source_prevents_cleanup_and_cluster_teardown(self):
        api = Mock()
        api.request.return_value = (200, {"conversations": [{"conversation_id": "accepted"}]})
        checks = SimpleNamespace(get=Mock(return_value=None), kubectl=Mock())
        journey = live.Journey(checks, api, Mock(), Mock(), {"scope": "scope", "run": "run"}, "image", "arm64", Mock())
        journey.detail = Mock(return_value={"summary": {"conversation_id": "accepted"}})
        with self.assertRaisesRegex(RuntimeError, "no auditable Session"):
            journey.cleanup()
        checks.kubectl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
