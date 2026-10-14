"""Public HTTP lifecycle contracts for the controlled Agents API fixture.

These tests start isolated Node processes. They use no cluster, OpenAI account,
or real credentials, and never import the fixture's implementation as an oracle.
"""

import json
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

    def request(self, method, path, body=None):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}" + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method, headers={"Content-Type": "application/json"},
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
