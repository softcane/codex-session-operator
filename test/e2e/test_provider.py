"""Public HTTP lifecycle contracts for the controlled Agents API fixture.

These tests start isolated Node processes. They use no cluster, OpenAI account,
or real credentials, and never import the fixture's implementation as an oracle.
"""

import json
import concurrent.futures
import http.client
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
AGENTS = "/v1/agents"
SESSIONS = AGENTS + "/sessions"
VAULTS = "/v1/vaults"
TEMPLATES = AGENTS + "/environments/templates"


class LifecycleFixtureContractTests(unittest.TestCase):
    def test_started_command_is_not_replayed_after_executor_instance_loss(self):
        session=self.post(SESSIONS,{"environment":{"type":"self_hosted","workspace_directory":"/workspace"}})
        environment=session["environment"]["id"]
        self.post("/executor/connect",{"environment_id":environment,"instance":"original"})
        self.assertEqual(self.request("POST",SESSIONS+"/"+session["id"]+"/events",{"events":[
            {"type":"agent.session.input.message","input":[{"content":[{"type":"input_text","text":"SANDBOX_LONG_COMMAND"}]}]}]})[0],204)
        job=self.read("/executor/commands?environment_id="+environment)[0]
        self.post("/executor/started",{"environment_id":environment,"id":job["id"],"instance":"original"})
        self.post("/executor/connect",{"environment_id":environment,"instance":"original"})
        self.assertEqual(self.read(SESSIONS+"/"+session["id"]+"/turns")["data"][0]["status"],"in_progress")
        self.post("/executor/connect",{"environment_id":environment,"instance":"replacement"})
        turns=self.read(SESSIONS+"/"+session["id"]+"/turns")["data"]
        self.assertEqual(turns,[{"id":"turn_1","status":"failed"}])
        self.assertEqual(self.read("/executor/commands?environment_id="+environment),[])
        command=next(value for value in self.read(SESSIONS+"/"+session["id"]+"/items")["data"] if value["type"]=="command_execution")
        self.assertEqual(command["status"],"failed")
        self.assertIn("outcome unknown",command["output"])
        self.assertNotIn("exit_code",command)
        self.assertEqual(self.read("/counts")["executorStarts"],1)
        self.assertEqual(self.request("POST","/executor/result",{"environment_id":environment,"id":job["id"],"exit_code":0})[0],404)
    def test_self_hosted_input_waits_for_actual_executor_result_and_scope_is_checked(self):
        session = self.post(SESSIONS, {"environment": {"type": "self_hosted", "workspace_directory": "/workspace"},
                                      "spend_control": {"limit": 500}})
        environment = session["environment"]["id"]
        self.assertEqual(session["spend_control"], {"limit": 500, "consumed": 0})
        self.post("/executor/connect", {"environment_id": environment, "hostname": "actual-test-pod"})
        self.assertEqual(self.read(AGENTS+"/environments/"+environment)["status"], "connected")
        self.assertEqual(self.request("POST", SESSIONS+"/"+session["id"]+"/events", {"events": [
            {"type": "agent.session.input.message", "input": [{"content": [{"type": "input_text", "text": "Inspect fixture"}]}]}]})[0], 204)
        self.assertEqual(self.read(SESSIONS+"/"+session["id"]+"/turns")["data"][0]["status"], "in_progress")
        jobs = self.read("/executor/commands?environment_id="+environment)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(self.request("POST", "/executor/result", {"environment_id": "foreign", "id": jobs[0]["id"]})[0], 404)
        self.post("/executor/result", {"environment_id": environment, "id": jobs[0]["id"],
                    "exit_code": 0, "output": "actual fixture command output", "cwd": "/workspace"})
        self.assertEqual(self.read(SESSIONS+"/"+session["id"]+"/turns")["data"][0]["status"], "completed")
        command = next(value for value in self.read(SESSIONS+"/"+session["id"]+"/items")["data"] if value["type"] == "command_execution")
        self.assertEqual(command["output"], "actual fixture command output")
    def test_workflow_actions_keep_parent_provenance_and_actual_command_evidence(self):
        coordinator = self.post(SESSIONS, {"environment": {"type": "self_hosted"}})
        path = SESSIONS + "/" + coordinator["id"]
        self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.message",
            "input": [{"content": [{"text": "WORKFLOW_FIRST_DELEGATION"}]}]}]})
        action = self.read(path)["required_actions"][0]
        self.assertEqual(action["name"], "workflow_delegate")
        turn = self.read(path + "/turns")["data"][0]
        self.assertEqual(turn["session_id"], coordinator["id"])
        self.assertIsNone(turn["subagent_id"])
        member = self.post(SESSIONS, {"environment": {"type": "self_hosted"}})
        member_path = SESSIONS + "/" + member["id"]
        self.request("POST", member_path + "/events", {"events": [{"type": "agent.session.input.message",
            "input": [{"content": [{"text": action["arguments"]["text"]}]}]}]})
        self.assertEqual(self.read(member_path)["required_actions"], [])
        environment = member["environment"]["id"]
        job = self.read("/executor/commands?environment_id=" + environment)[0]
        self.post("/executor/result", {"environment_id": environment, "id": job["id"],
            "exit_code": 0, "output": "actual checkout test failed as expected", "cwd": "/workspace"})
        result = self.read(member_path)["required_actions"][0]
        self.assertEqual(result["name"], "workflow_result")
        output = result["arguments"]["outputs"][0]
        check = output["checks"][0]
        command = next(item for item in self.read(member_path + "/items")["data"] if item["id"] == check["source_item_id"])
        self.assertEqual(check["output"], command["output"])
        self.assertEqual(check["command"], command["command"])
        self.assertEqual(check["exit_code"], command["exit_code"])
        event = {"type": "agent.session.input.tool_result", "turn_id": result["turn_id"],
                 "call_id": result["call_id"], "success": True, "output": '{"id":"result-saved"}'}
        self.request("POST", member_path + "/events", {"events": [event]})
        self.assertEqual(self.read(member_path)["required_actions"], [])
        self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.tool_result",
            "turn_id": action["turn_id"], "call_id": action["call_id"], "success": True, "output": '{}'}]})
        self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.message", "input": [{"content": [
            {"text": "Saved workflow result result-saved from assignment member-saved (diagnose) has outcome success."}]}]}]})
        read = self.read(path)["required_actions"][0]
        self.assertEqual(read["name"], "workflow_read_result")
        self.assertEqual(read["arguments"], {"result_id": "result-saved"})

    def test_research_parallel_actions_review_reads_and_synthesis_exact_references(self):
        def create():
            session = self.post(SESSIONS, {"environment": {"type": "self_hosted"}})
            return session, SESSIONS + "/" + session["id"]

        def message(path, text):
            self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.message",
                "input": [{"content": [{"text": text}]}]}]})

        def answer(path, action, output):
            self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.tool_result",
                "turn_id": action["turn_id"], "call_id": action["call_id"], "success": True, "output": output}]})

        def compute(session, path):
            job = self.read("/executor/commands?environment_id=" + session["environment"]["id"])[0]
            completed = subprocess.run(job["argv"], cwd=self.directory.name, capture_output=True, text=True, timeout=10)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            actual = json.loads((Path(self.directory.name) / "research-report.json").read_text())
            self.assertEqual(json.loads(completed.stdout), actual)
            self.post("/executor/result", {"environment_id": session["environment"]["id"], "id": job["id"],
                "exit_code": completed.returncode, "output": completed.stdout, "cwd": self.directory.name})
            action = self.read(path)["required_actions"][0]
            self.assertEqual(action["name"], "workflow_result")
            self.assertTrue(json.loads(action["arguments"]["outputs"][0]["content"])["verified"])
            return action

        coordinator, path = create()
        message(path, "RESEARCH_PILOT")
        delegates = self.read(path)["required_actions"]
        self.assertEqual([item["arguments"]["role"] for item in delegates], ["argument", "experiment"])
        self.assertNotEqual(delegates[0]["call_id"], delegates[1]["call_id"])
        self.assertEqual(delegates[0]["turn_id"], delegates[1]["turn_id"])
        answer(path, delegates[0], '{}')
        self.assertEqual(self.read(path)["required_actions"], [delegates[1]])
        answer(path, delegates[1], '{}')
        workers = []
        for delegate in delegates:
            worker, worker_path = create()
            message(worker_path, delegate["arguments"]["text"])
            workers.append((worker, worker_path))
        self.assertEqual(len(self.read("/counts")["jobs"]), 2)
        results = {}
        review_delegate = None
        for index, (worker, worker_path) in enumerate(workers):
            role = delegates[index]["arguments"]["role"]
            action = compute(worker, worker_path)
            identifier = "result-" + role
            raw = json.dumps(action["arguments"], separators=(",", ":"))
            results[identifier] = raw
            answer(worker_path, action, json.dumps({"result_id": identifier}))
            message(path, f"Saved workflow result {identifier} from assignment member-{role} ({role}) has outcome success.")
            read = self.read(path)["required_actions"][0]
            self.assertEqual(read["arguments"], {"result_id": identifier})
            answer(path, read, raw)
            if index == 0:
                self.assertEqual(self.read(path)["required_actions"], [])
            else:
                review_delegate = self.read(path)["required_actions"][0]
        self.assertEqual(review_delegate["arguments"]["dependencies"], ["result-argument", "result-experiment"])
        answer(path, review_delegate, '{}')
        reviewer, reviewer_path = create()
        reviewer_input = review_delegate["arguments"]["text"] + ("\n\nExact prerequisite saved result IDs: " +
            json.dumps(["result-argument", "result-experiment"]) + ". Read these with workflow_read_result before using their evidence.")
        message(reviewer_path, reviewer_input)
        reads = self.read(reviewer_path)["required_actions"]
        self.assertEqual({item["arguments"]["result_id"] for item in reads}, set(results))
        self.assertNotEqual(reads[0]["call_id"], reads[1]["call_id"])
        answer(reviewer_path, reads[0], results[reads[0]["arguments"]["result_id"]])
        self.assertEqual(self.read(reviewer_path)["required_actions"], [reads[1]])
        answer(reviewer_path, reads[1], results[reads[1]["arguments"]["result_id"]])
        review = compute(reviewer, reviewer_path)
        self.assertEqual(review["arguments"]["references"], ["result-argument", "result-experiment"])
        answer(reviewer_path, review, '{"result_id":"result-review"}')
        message(path, "Saved workflow result result-review from assignment member-review (review) has outcome success.")
        read = self.read(path)["required_actions"][0]
        answer(path, read, json.dumps(review["arguments"]))
        synthesis = self.read(path)["required_actions"][0]
        self.assertEqual(synthesis["name"], "workflow_result")
        self.assertEqual(synthesis["arguments"]["references"], ["result-argument", "result-experiment", "result-review"])
        self.assertEqual(synthesis["arguments"]["outputs"][0]["name"], "synthesis")
        # Execute the same reviewer with corrupt referenced evidence. This must
        # fail even when the worker claimed success and the current computation
        # itself still matches the supplied bounded mathematical claim.
        for corruption in ("verified", "sample"):
            bad, bad_path = create()
            message(bad_path, reviewer_input)
            bad_reads = self.read(bad_path)["required_actions"]
            changed = json.loads(results["result-experiment"])
            report = json.loads(changed["outputs"][0]["content"])
            if corruption == "verified":
                report["verified"] = False
            else:
                report["samples"][-1]["value"] = "999999"
            changed["outputs"][0]["content"] = json.dumps(report)
            for read_action in bad_reads:
                result_id = read_action["arguments"]["result_id"]
                answer(bad_path, read_action, json.dumps(changed) if result_id == "result-experiment" else results[result_id])
            job = self.read("/executor/commands?environment_id=" + bad["environment"]["id"])[0]
            failed = subprocess.run(job["argv"], cwd=self.directory.name, capture_output=True, text=True, timeout=10)
            self.assertEqual(failed.returncode, 1, failed.stderr)
            self.assertFalse(json.loads(failed.stdout)["verified"])


    def test_workflow_tool_ack_barrier_deduplicates_commits_and_releases_lost_response(self):
        session = self.post(SESSIONS, {"environment": {"type": "self_hosted"}, "input": "WORKFLOW_FAULT_ACK"})
        path = SESSIONS + "/" + session["id"]
        action = self.read(path)["required_actions"][0]
        self.post("/control", {"holdToolResultName": "workflow_delegate"})
        event = {"events": [{"type": "agent.session.input.tool_result", "turn_id": action["turn_id"],
                            "call_id": action["call_id"], "success": True, "output": '{}'}]}
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(self.request, "POST", path + "/events", event, "same-result-key")
            try:
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    snapshot = self.read("/counts")
                    if snapshot["heldToolResponses"]:
                        break
                    time.sleep(0.01)
                self.assertEqual(len(snapshot["heldToolResponses"]), 1)
                self.assertEqual(snapshot["toolResultCommits"][0]["accepted"], 1)
                self.assertFalse(future.done())
                retry = pool.submit(self.request, "POST", path + "/events", event, "same-result-key")
                # The held key applies to retries too; no duplicate output event
                # is committed while the gateway still lacks an ACK.
                time.sleep(0.05)
                self.assertFalse(retry.done())
                self.assertEqual(self.read("/counts")["toolResultCommits"][0]["accepted"], 1)
                self.assertEqual(self.read(path)["required_actions"], [])
            finally:
                self.post("/control", {"releaseToolResponses": True})
            for response in (future, retry):
                with self.assertRaises((OSError, http.client.HTTPException)):
                    response.result(timeout=3)
        self.assertEqual(self.request("POST", path + "/events", event, "same-result-key")[0], 204)
        self.assertEqual(self.read("/counts")["toolResultCommits"][0]["accepted"], 1)
        self.assertEqual(self.read("/counts")["heldToolResponses"], [])

    def test_workflow_fault_pending_long_and_failed_actions_remain_explicit(self):
        pending = self.post(SESSIONS, {"environment": {"type": "self_hosted"}, "input": "WORKFLOW_FAULT_PENDING"})
        self.assertEqual(self.request("GET", SESSIONS + "/" + pending["id"])[0], 503)
        self.assertEqual(self.read("/counts")["pendingWorkflowActions"][0]["name"], "workflow_delegate")
        self.post("/control", {"reset": True})
        for mode, expected_timeout in (("LONG", 120000), ("FAIL", 15000)):
            coordinator = self.post(SESSIONS, {"environment": {"type": "self_hosted"}, "input": "WORKFLOW_FAULT_" + mode})
            path = SESSIONS + "/" + coordinator["id"]
            delegate = self.read(path)["required_actions"][0]
            worker = self.post(SESSIONS, {"environment": {"type": "self_hosted"}, "input": delegate["arguments"]["text"]})
            worker_path = SESSIONS + "/" + worker["id"]
            job = self.read("/executor/commands?environment_id=" + worker["environment"]["id"])[0]
            self.assertEqual(job["timeout_ms"], expected_timeout)
            if mode == "LONG":
                self.assertIn("interrupted-started.json", job["argv"][2])
                continue
            completed = subprocess.run(job["argv"], cwd=self.directory.name, capture_output=True, text=True, timeout=10)
            self.assertEqual(completed.returncode, 1)
            self.post("/executor/result", {"environment_id": worker["environment"]["id"], "id": job["id"],
                "exit_code": completed.returncode, "output": completed.stdout})
            result = self.read(worker_path)["required_actions"][0]
            self.assertEqual(result["arguments"]["outcome"], "failed")
            self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.tool_result",
                "turn_id": delegate["turn_id"], "call_id": delegate["call_id"], "success": True, "output": '{}'}]})
            self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.message", "input": [{"content": [
                {"text": "Saved workflow result result-failed from assignment member-failed (diagnose) has outcome failed."}]}]}]})
            read = self.read(path)["required_actions"][0]
            self.request("POST", path + "/events", {"events": [{"type": "agent.session.input.tool_result",
                "turn_id": read["turn_id"], "call_id": read["call_id"], "success": True, "output": json.dumps(result["arguments"])}]})
            review = self.read(path)["required_actions"][0]
            self.assertEqual(review["arguments"]["dependencies"], ["result-failed"])

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="codex-lifecycle-fixture-")
        self.addCleanup(self.directory.cleanup)
        self.stderr = open(Path(self.directory.name) / "node.stderr", "w+")
        self.addCleanup(self.stderr.close)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.process = subprocess.Popen(
            ["node", str(ROOT / "test/e2e/provider.js")],
            cwd=self.directory.name,
            env={"PATH": os.environ.get("PATH", ""), "PORT": str(self.port)},
            stdout=subprocess.DEVNULL, stderr=self.stderr,
        )
        self.addCleanup(self.stop)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.fail("Fixture Node process exited before readiness")
            try:
                self.read("/counts")
                return
            except urllib.error.URLError:
                time.sleep(0.02)
        self.fail("Fixture listener did not start")

    def stop(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)

    def request(self, method, path, body=None, key=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}" + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method, headers={"Content-Type": "application/json", "Idempotency-Key": key or str(time.monotonic_ns())},
        )
        try:
            response = urllib.request.urlopen(request, timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else None

    def read(self, path):
        status, body = self.request("GET", path)
        self.assertEqual(status, 200, path)
        return body

    def post(self, path, body):
        status, value = self.request("POST", path, body)
        self.assertEqual(status, 200, path)
        return value

    def delete_and_confirm_absence(self, path, identifier):
        self.assertEqual(self.request("DELETE", path), (200, {"id": identifier, "deleted": True}))
        self.assertEqual(self.request("GET", path)[0], 404)
        self.assertEqual(self.request("DELETE", path)[0], 404)

    def assert_redacted(self, value, secrets):
        encoded = json.dumps(value)
        for secret in secrets:
            self.assertNotIn(secret, encoded)
        if isinstance(value, dict):
            for key, nested in value.items():
                self.assertNotIn(key, {"token", "access_token", "refresh_token", "client_secret", "secret_value"})
                self.assert_redacted(nested, secrets)
        elif isinstance(value, list):
            for nested in value:
                self.assert_redacted(nested, secrets)

    def fresh_id_after_delete(self, collection, body):
        first = self.post(collection, body)
        self.delete_and_confirm_absence(collection + "/" + first["id"], first["id"])
        self.assertEqual(self.read(collection)["data"], [])
        second = self.post(collection, body)
        self.assertNotEqual(first["id"], second["id"])
        self.assertGreater(int(second["id"].rsplit("_", 1)[1]), int(first["id"].rsplit("_", 1)[1]))
        self.assertEqual(self.read(collection)["data"], [second])
        self.assertEqual(self.request("GET", collection + "/" + first["id"])[0], 404)
        return first, second

    def test_named_agent_snapshot_retains_name_instructions_and_nested_settings(self):
        agent = self.post(AGENTS, {"name": "First Agent", "model": "gpt-6-luna",
            "instructions": "Original instructions", "text": {"verbosity": "low"},
            "metadata": {"kubernetes_uid": "agent-owner"}})
        original = self.post(SESSIONS, {"agent_id": agent["id"], "environment": {"type": "none"}, "input": "first task"})
        self.assertEqual(original["agent"]["id"], agent["id"])
        self.assertEqual(original["agent"]["name"], "First Agent")
        self.assertEqual(original["agent"]["instructions"], "Original instructions")
        changed = self.post(AGENTS + "/" + agent["id"], {"name": "Renamed Agent",
            "instructions": "New instructions", "text": {"verbosity": "high"}})
        self.assertEqual(self.read(SESSIONS + "/" + original["id"])["agent"], original["agent"])
        future = self.post(SESSIONS, {"agent_id": agent["id"], "environment": {"type": "none"}, "input": "future task"})
        self.assertEqual(future["agent"], changed)
        self.post(SESSIONS + "/" + future["id"], {"agent": {"name": "Local snapshot", "instructions": "Local override"}})
        self.assertEqual(self.read(AGENTS + "/" + agent["id"]), changed)
        self.assertEqual(self.read(SESSIONS + "/" + original["id"])["agent"], original["agent"])
        counts = self.read("/counts")
        self.assertEqual((counts["agentCreates"], counts["creates"], counts["messageCommits"]), (1, 2, 2))

    def test_agent_delete_and_recreate_uses_monotonic_id(self):
        self.fresh_id_after_delete(AGENTS, {"name": "Repeated Agent", "model": "gpt-6-luna"})
        self.assertEqual(self.read("/counts")["agentCreates"], 2)

    def test_vault_delete_and_recreate_uses_monotonic_id(self):
        self.fresh_id_after_delete(VAULTS, {"name": "Repeated Vault", "metadata": {"kubernetes_uid": "vault-owner"}})
        self.assertEqual(self.read("/counts")["vaultCreates"], 2)

    def test_template_delete_and_recreate_uses_monotonic_id(self):
        self.fresh_id_after_delete(TEMPLATES, {"name": "Repeated Template", "network": {"access": "disabled"}})
        self.assertEqual(self.read("/counts")["templateCreates"], 2)

    def test_session_delete_and_recreate_uses_monotonic_id(self):
        first, second = self.fresh_id_after_delete(SESSIONS, {"environment": {"type": "none"}, "input": "new task"})
        for suffix in ("items", "turns", "events"):
            self.assertEqual(self.request("GET", SESSIONS + "/" + first["id"] + "/" + suffix)[0], 404)
        self.assertEqual(len(self.read(SESSIONS + "/" + second["id"] + "/turns")["data"]), 1)
        counts = self.read("/counts")
        self.assertEqual((counts["creates"], counts["deletes"], counts["messageCommits"]), (2, 1, 2))

    def test_credential_delete_and_recreate_uses_monotonic_id(self):
        vault = self.post(VAULTS, {"name": "Credential Vault"})
        self.fresh_id_after_delete(VAULTS + "/" + vault["id"] + "/credentials",
            {"name": "Repeated Credential", "auth": {"type": "static_bearer", "mcp_server_url": "https://example.com/mcp", "token": "fixture-placeholder"}})
        counts = self.read("/counts")
        self.assertEqual((counts["credentialCreates"], counts["credentialWrites"]), (2, 2))

    def test_credential_create_and_rotation_keep_public_auth_only(self):
        vault = self.post(VAULTS, {"name": "Auth Vault"})
        collection = VAULTS + "/" + vault["id"] + "/credentials"
        tests = [
            {"auth": {"type": "static_bearer", "mcp_server_url": "https://example.com/mcp", "token": "fixture-bearer-v1"},
             "rotate": {"type": "static_bearer", "token": "fixture-bearer-v2"},
             "public": {"type": "static_bearer", "mcp_server_url": "https://example.com/mcp"}},
            {"auth": {"type": "environment_variable", "secret_name": "LIFECYCLE_TOKEN",
                      "networking": {"type": "limited", "allowed_hosts": ["api.example.com"]}, "secret_value": "fixture-env-v1"},
             "rotate": {"type": "environment_variable", "secret_value": "fixture-env-v2"},
             "public": {"type": "environment_variable", "secret_name": "LIFECYCLE_TOKEN",
                        "networking": {"type": "limited", "allowed_hosts": ["api.example.com"]}}},
            {"auth": {"type": "mcp_oauth", "mcp_server_url": "https://example.com/mcp", "access_token": "fixture-access-v1"},
             "rotate": {"type": "mcp_oauth", "access_token": "fixture-access-v2"},
             "public": {"type": "mcp_oauth", "mcp_server_url": "https://example.com/mcp"}},
        ]
        secrets = ["fixture-bearer-v1", "fixture-bearer-v2", "fixture-env-v1", "fixture-env-v2", "fixture-access-v1", "fixture-access-v2"]
        for index, case in enumerate(tests):
            with self.subTest(auth=case["auth"]["type"]):
                credential = self.post(collection, {"name": "Public credential", "auth": case["auth"], "metadata": {"generation": "1"}})
                self.assertEqual(credential["auth"], case["public"])
                path = collection + "/" + credential["id"]
                rotated = self.post(path, {"auth": case["rotate"], "metadata": {"generation": "2"}})
                self.assertEqual(rotated["id"], credential["id"])
                self.assertEqual(rotated["auth"], case["public"])
                self.assertEqual(rotated["metadata"], {"generation": "2"})
                self.assertEqual(self.read(path), rotated)
                self.assert_redacted([credential, rotated, self.read(collection), self.read("/counts")], secrets)
                counts = self.read("/counts")
                self.assertEqual((counts["credentialCreates"], counts["credentialWrites"]), (index + 1, (index + 1) * 2))

    def test_oauth_rotation_preserves_refresh_identity_and_redacts_nested_secrets(self):
        vault = self.post(VAULTS, {"name": "OAuth Vault"})
        collection = VAULTS + "/" + vault["id"] + "/credentials"
        auth = {"type": "mcp_oauth", "mcp_server_url": "https://example.com/mcp", "access_token": "fixture-access-secret",
            "expires_at": "2030-01-01T00:00:00Z", "refresh": {"token_endpoint": "https://auth.example.com/token",
            "client_id": "public-client", "resource": "https://example.com/mcp", "scope": "read", "refresh_token": "fixture-refresh-secret",
            "token_endpoint_auth": {"type": "client_secret_basic", "client_secret": "fixture-client-secret"}}}
        credential = self.post(collection, {"auth": auth})
        path = collection + "/" + credential["id"]
        rotated = self.post(path, {"auth": {"type": "mcp_oauth", "access_token": "fixture-new-access",
            "expires_at": None, "refresh": {"refresh_token": "fixture-new-refresh", "scope": None,
            "token_endpoint_auth": {"type": "client_secret_basic", "client_secret": "fixture-new-client"}}}})
        expected = {"type": "mcp_oauth", "mcp_server_url": "https://example.com/mcp", "expires_at": None,
            "refresh": {"token_endpoint": "https://auth.example.com/token", "client_id": "public-client",
            "resource": "https://example.com/mcp", "scope": None, "token_endpoint_auth": {"type": "client_secret_basic"}}}
        self.assertEqual(rotated["auth"], expected)
        self.assertEqual(self.read(path)["auth"], expected)
        self.assert_redacted([credential, rotated, self.read(collection), self.read("/counts")],
            ["fixture-access-secret", "fixture-refresh-secret", "fixture-client-secret", "fixture-new-access", "fixture-new-refresh", "fixture-new-client"])
        self.assertEqual(self.read("/counts")["credentialWrites"], 2)

    def test_credential_metadata_and_name_updates_do_not_count_as_auth_rotation(self):
        vault = self.post(VAULTS, {"name": "Metadata Vault"})
        collection = VAULTS + "/" + vault["id"] + "/credentials"
        credential = self.post(collection, {"name": "Original credential", "auth": {
            "type": "static_bearer", "mcp_server_url": "https://example.com/mcp", "token": "fixture-placeholder"}})
        updated = self.post(collection + "/" + credential["id"], {"name": "Renamed credential", "metadata": {"revision": "2"}})
        self.assertEqual(updated["name"], "Renamed credential")
        self.assertEqual(updated["metadata"], {"revision": "2"})
        self.assertEqual(updated["auth"], credential["auth"])
        counts = self.read("/counts")
        self.assertEqual((counts["credentialCreates"], counts["credentialWrites"]), (1, 1))

    def test_credential_routes_are_scoped_to_own_vault(self):
        first = self.post(VAULTS, {"name": "First Vault"})
        other = self.post(VAULTS, {"name": "Other Vault"})
        collection = VAULTS + "/" + first["id"] + "/credentials"
        credential = self.post(collection, {"auth": {"type": "static_bearer", "token": "fixture-placeholder"}})
        foreign = VAULTS + "/" + other["id"] + "/credentials"
        self.assertEqual(self.read(foreign)["data"], [])
        for method, body in (("GET", None), ("DELETE", None), ("POST", {"auth": {"type": "static_bearer", "token": "wrong-vault"}})):
            with self.subTest(method=method):
                self.assertEqual(self.request(method, foreign + "/" + credential["id"], body)[0], 404)
        self.assertEqual(self.read(collection + "/" + credential["id"]), credential)
        self.assertEqual(self.read("/counts")["credentialWrites"], 1)

    def test_template_update_and_hosted_sessions_have_distinct_connected_runtimes(self):
        template = self.post(TEMPLATES, {"name": "Hosted Template", "container_size": "small", "network": {"access": "disabled"}})
        updated = self.post(TEMPLATES + "/" + template["id"], {"name": "Updated Template", "container_size": "medium"})
        self.assertEqual(updated["id"], template["id"])
        self.assertEqual(self.read(TEMPLATES + "/" + template["id"]), updated)
        self.assertEqual(updated["network"], {"access": "disabled"})
        self.assertEqual(self.read("/counts")["templateCreates"], 1)
        identifiers = []
        for _ in range(2):
            session = self.post(SESSIONS, {"environment": {"type": "openai_hosted", "template_id": template["id"]}})
            environment = session["environment"]
            identifiers.append(environment["id"])
            self.assertEqual(environment["template_id"], template["id"])
            runtime = self.read(AGENTS + "/environments/" + environment["id"])
            self.assertEqual(runtime, {"id": environment["id"], "status": "connected", "object": "agent.environment"})
        self.assertNotEqual(identifiers[0], identifiers[1])
        self.assertEqual(self.read("/counts")["creates"], 2)

    def test_targeted_session_observation_fault_and_reset_preserve_identity(self):
        affected = self.post(SESSIONS, {"environment": {"type": "none"}})
        healthy = self.post(SESSIONS, {"environment": {"type": "none"}})
        before = self.read("/counts")
        self.post("/control", {"sessionGetStatus": 503, "sessionFaultID": affected["id"]})
        self.assertEqual(self.request("GET", SESSIONS + "/" + affected["id"])[0], 503)
        self.assertEqual(self.read(SESSIONS + "/" + healthy["id"]), healthy)
        self.assertEqual(len(self.read(SESSIONS)["data"]), 2)
        self.assertEqual(self.read("/counts"), before)
        self.post("/control", {"reset": True})
        self.assertEqual(self.read(SESSIONS + "/" + affected["id"]), affected)
        self.assertEqual(self.read("/counts"), before)

    def test_control_reset_clears_create_delete_discovery_and_observation_faults(self):
        session = self.post(SESSIONS, {"environment": {"type": "none"}})
        path = SESSIONS + "/" + session["id"]
        self.post("/control", {"failCreate": True, "failDelete": True, "hideDiscovery": True, "sessionGetStatus": 503})
        self.assertEqual(self.request("POST", SESSIONS, {"environment": {"type": "none"}})[0], 503)
        self.assertEqual(self.request("DELETE", path)[0], 503)
        self.assertEqual(self.request("GET", path)[0], 503)
        self.assertEqual(self.read(SESSIONS)["data"], [])
        before = self.read("/counts")
        self.assertEqual((before["creates"], before["deletes"]), (2, 0))
        self.post("/control", {"reset": True})
        self.assertEqual(self.read(path), session)
        self.assertEqual(self.read(SESSIONS)["data"], [session])
        self.assertEqual(self.read("/counts"), before)
        self.delete_and_confirm_absence(path, session["id"])
        fresh = self.post(SESSIONS, {"environment": {"type": "none"}})
        self.assertGreater(int(fresh["id"].rsplit("_", 1)[1]), int(session["id"].rsplit("_", 1)[1]))


if __name__ == "__main__":
    unittest.main()
