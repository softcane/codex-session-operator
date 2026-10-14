#!/usr/bin/env python3
"""Run the public conversation and five-CRD lifecycle suite in isolated Kind.

No real API credentials or inference. --keep retains a successful demo for CLI
and browser inspection; failures and ordinary runs remove only their own cluster.
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import secrets
import shlex
import shutil
import socket
import ssl
import subprocess
import time
import traceback
import urllib.error
import uuid

import gateway_tls
from reporting import report_path

import support as helpers
import client as demo_client
import lifecycle

ROOT = helpers.ROOT
ensure = helpers.ensure
NS = helpers.NS
GATEWAY = "codex-session-operator-gateway"
TAG = "codex-session-manager:demo"
FIXTURE_TAG = "codex-provider-fixture:demo"


class ReplaySafeAPI:
    """Recover tunnel failures only when a request can keep its replay identity."""
    def __init__(self, api, attempts=3, retry_delay=0.5):
        self.api, self.attempts, self.retry_delay = api, attempts, retry_delay

    def request(self, path, method="GET", body=None, key=None):
        frozen = None if body is None else json.dumps(body)
        attempts = self.attempts if method == "GET" or method == "POST" and key else 1
        for attempt in range(attempts):
            try:
                return self.api.request(path, method, None if frozen is None else json.loads(frozen), key)
            except urllib.error.HTTPError:
                raise  # An HTTP rejection is a result, not a tunnel failure.
            except (urllib.error.URLError, OSError):
                if attempt + 1 == attempts:
                    raise
                time.sleep(self.retry_delay)


def replace_gateway_pods(checks):
    """Wait out old Pods before a service port-forward chooses its backend."""
    deployment = json.loads(checks.kubectl("get", "deployment", GATEWAY, "-n", NS, "-o", "json"))
    labels = deployment["spec"]["selector"]["matchLabels"]
    ensure(labels, "Gateway Deployment has no Pod selector")
    selector = ",".join(key + "=" + value for key, value in sorted(labels.items()))

    def selected_names():
        pods = json.loads(checks.kubectl("get", "pods", "-n", NS, "-l", selector, "-o", "json"))
        return {pod["metadata"]["name"] for pod in pods["items"]}

    old_names = selected_names()
    ensure(old_names, "Gateway replacement has no old Pods to observe")
    checks.kubectl("rollout", "restart", "deployment/" + GATEWAY, "-n", NS)
    checks.kubectl("rollout", "status", "deployment/" + GATEWAY, "-n", NS, "--timeout=180s")
    checks.wait("previous gateway Pods are absent", lambda: not old_names.intersection(selected_names()), 90)


def stop_forward(process):
    if process is None:
        return
    process.terminate()
    try:
        process.wait(10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(10)


def private_write(path, text):
    path.touch(mode=0o600, exist_ok=True)
    path.chmod(0o600)
    path.write_text(text)


def prepare_manual(directory, kubeconfig, token, port):
    private_write(directory / "curl.conf", 'header = "Authorization: Bearer ' + token + '"\n')
    private_write(directory / "env.sh", "export DEMO_DIR=" + shlex.quote(str(directory)) +
                  "\nexport KUBECONFIG=" + shlex.quote(str(kubeconfig)) + "\nexport DEMO_NAMESPACE=" + NS +
                  "\nexport DEMO_PORT=" + str(port) + "\nexport DEMO_URL=https://127.0.0.1:" + str(port) + "\n")


def test_conversations(checks, api, base, run, evidence, report, replace_gateway, fixture_counts):
    """Exercise public conversation identities against real deployed storage and CRs."""
    api = ReplaySafeAPI(api)
    vault_name, route_id = "demo-conversation-vault", "demo-conversation"
    checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "Vault",
                  "metadata": {"name": vault_name, "namespace": NS},
                  "spec": {"deletionPolicy": "Delete"}})
    vault = checks.wait("conversation Vault Ready", lambda: value if
                       helpers.current_ready(value := checks.get("vault", vault_name)) else None)
    config = json.loads(checks.kubectl("get", "configmap", "gateway-work-routes", "-n", NS, "-o", "json"))
    routes = json.loads(config["data"]["routes.json"])
    routes.append({"id": route_id, "namespace": NS, "source": "manual-conversation",
                   "agent": "demo-investigator-v1", "environment": "demo-text-v1",
                   "vaults": [vault_name], "spend_control": {"mode": "unlimited"}})
    checks.apply({"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": "gateway-work-routes", "namespace": NS},
                  "data": {"routes.json": json.dumps(routes)}})
    replace_gateway()
    status, catalog = api.request(base + "/catalog")
    ensure(status == 200 and any(route["id"] == route_id and route["ready"]
                                for route in catalog["work_routes"]), "Configured conversation profile is not ready")
    counts_before = fixture_counts()
    collection = base + "/conversations"
    text = "CONVERSATION_INITIAL_" + run + ": explain the supplied checkout failure."
    body = {"route_id": route_id, "text": text, "title": "Checkout investigation " + run}
    key = "conversation-create-" + run

    def create(value=body):
        return api.request(collection, "POST", value, key)

    ensure(api.request(collection, "POST", body)[0] == 400, "Conversation creation accepted a missing replay key")
    status, receipt = create()
    ensure(status == 202 and receipt["acceptance"] == "committed" and receipt["attempt"] == 1,
           "Conversation was not durably accepted")
    path, conversation_id = receipt["conversation_url"], receipt["conversation_id"]
    ensure(path == collection + "/" + conversation_id, "Conversation receipt URL does not select its identity")
    evidence.write("conversation-create-receipt.json", receipt)
    ensure(create() == (202, receipt), "Conversation create replay changed its receipt")
    for changes in ({"text": "different input"}, {"title": "different title"}, {"route_id": "demo-investigation"}):
        ensure(create({**body, **changes})[0] == 409, "Changed create under the same key did not conflict")
    for suffix, method, value in (("", "GET", None), ("/messages", "POST", {"text": "forbidden"}),
                                  ("/close", "POST", {}), ("/restart", "POST", {"text": "forbidden"})):
        forbidden = path.replace("/namespaces/" + NS + "/", "/namespaces/kube-system/") + suffix
        ensure(api.request(forbidden, method, value, "denied-" + run)[0] == 404,
               "Conversation route accepted a caller outside its authorized namespace")
    ensure(api.request(collection.replace(NS, "kube-system"), "POST", body, key)[0] == 404,
           "Cross-namespace conversation creation was accepted")

    def detail(selected=path):
        code, value = api.request(selected)
        return value if code == 200 else None

    def completed(turns, selected=path):
        value = detail(selected)
        return value if value and demo_client.completed_history(value, turns) else None

    initial = checks.wait("conversation initial input produced saved completed history", lambda: completed(1))
    summary = initial["summary"]
    name, uid = summary["session_name"], summary["session_uid"]
    session = checks.get("session", name)
    sid = session["status"]["openaiSessionID"]
    ensure(session["metadata"]["namespace"] == NS and session["metadata"]["uid"] == uid and
           session["spec"]["agentRef"]["name"] == "demo-investigator-v1" and
           session["spec"]["environmentRef"]["name"] == "demo-text-v1" and
           session["spec"]["vaultRefs"] == [{"name": vault_name}] and
           session["spec"]["spendControl"] == {"mode": "unlimited"},
           "Conversation generated a Session with a different profile policy")
    ensure(summary["agent"] == "demo-investigator-v1" and summary["environment"] == "demo-text-v1" and
           summary["vaults"] == [vault_name] and summary["spend_control"] == {"mode": "unlimited"},
           "Conversation detail omitted or changed its frozen profile policy")
    for field, resource, expected_name in (("agentSnapshot", "agent", "demo-investigator-v1"),
                                           ("environmentSnapshot", "environment", "demo-text-v1")):
        expected = checks.get(resource, expected_name)
        ensure(session["status"][field]["uid"] == expected["metadata"]["uid"] and
               session["status"][field]["generation"] == expected["metadata"]["generation"],
               "Conversation did not freeze the selected resource identity")
    ensure(session["status"]["vaultSnapshots"][0]["uid"] == vault["metadata"]["uid"] and
           session["status"]["vaultSnapshots"][0]["openaiVaultID"] == vault["status"]["openaiVaultID"],
           "Conversation did not freeze the selected Vault identity")
    ensure(not session["status"].get("podName") and text not in json.dumps(session["spec"]) and
           text not in json.dumps(session["status"]), "Text-only conversation provisioned an executor or stored prompt in the CR")
    counts = fixture_counts()
    remote = next(value for value in counts["sessions"] if value["id"] == sid)
    ensure(remote["input"] == text and counts["creates"] == counts_before["creates"] + 1 and
           counts["messageCommits"] == counts_before["messageCommits"] + 1,
           "Conversation initial input was missing or replay created extra remote work")
    status, listing = api.request(collection)
    ensure(status == 200 and any(item["conversation_id"] == conversation_id for item in listing["conversations"]),
           "Conversation collection omitted the accepted conversation")
    report["checks"]["conversationProfileMappingInitialInputReplayAndAuthorization"] = True

    followup = {"text": "CONVERSATION_FOLLOWUP_" + run + ": include the second observation."}
    message_key = "conversation-message-" + run
    status, message_receipt = api.request(path + "/messages", "POST", followup, message_key)
    ensure(status == 202 and message_receipt["conversation_id"] == conversation_id,
           "Conversation follow-up was not accepted into the existing identity")
    ensure(api.request(path + "/messages", "POST", followup, message_key) == (202, message_receipt),
           "Conversation message replay changed its receipt")
    ensure(api.request(path + "/messages", "POST", {"text": "changed follow-up"}, message_key)[0] == 409,
           "Conversation message replay accepted changed content")
    second = checks.wait("conversation second saved completed root turn", lambda: completed(2))
    ensure(second["summary"]["session_uid"] == uid, "Follow-up replaced the conversation Session")
    # A route revision is chosen for a new attempt; existing conversations keep
    # their frozen Agent/Environment/Vault/spend policy through a gateway restart.
    next_environment = "demo-conversation-text-v2"
    checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "Environment",
                  "metadata": {"name": next_environment, "namespace": NS},
                  "spec": {"mode": "none", "deletionPolicy": "Delete"}})
    checks.wait("revised conversation Environment Ready", lambda:
                helpers.current_ready(checks.get("environment", next_environment)))
    routes[-1] = {**routes[-1], "environment": next_environment, "vaults": []}
    checks.apply({"apiVersion": "v1", "kind": "ConfigMap",
                  "metadata": {"name": "gateway-work-routes", "namespace": NS},
                  "data": {"routes.json": json.dumps(routes)}})
    replace_gateway()
    restored = checks.wait("conversation recovered after gateway replacement", lambda: completed(2))
    ensure(restored["summary"]["session_uid"] == uid and
           checks.get("session", name)["status"]["openaiSessionID"] == sid,
           "Gateway replacement changed conversation identity")
    ensure(restored["summary"]["environment"] == "demo-text-v1" and restored["summary"]["vaults"] == [vault_name],
           "A route revision rewrote the existing conversation policy")
    ensure(create() == (202, receipt) and
           api.request(path + "/messages", "POST", followup, message_key) == (202, message_receipt),
           "Gateway replacement lost original conversation acceptance receipts")
    counts = fixture_counts()
    ensure(counts["creates"] == counts_before["creates"] + 1 and
           counts["messageCommits"] == counts_before["messageCommits"] + 2,
           "Conversation retries after process replacement duplicated remote work")
    evidence.write("conversation-after-gateway-replacement.json", restored)
    report["checks"]["conversationFollowupAndGatewayReplacementRecovery"] = True

    close_key = "conversation-close-" + run
    status, close_receipt = api.request(path + "/close", "POST", {}, close_key)
    ensure(status == 202 and close_receipt["conversation_id"] == conversation_id, "Conversation close was not accepted")
    ensure(api.request(path + "/close", "POST", {}, close_key) == (202, close_receipt), "Close receipt was not stable")
    retained = checks.wait("conversation cleanup retained readable history", lambda: value
                          if (value := detail()) and demo_client.cleaned_up(value) else None)
    ensure(checks.get("session", name) is None and not any(value["id"] == sid for value in fixture_counts()["sessions"]),
           "Closed conversation left local or remote Session running")
    ensure(demo_client.completed_history(retained, 2), "Conversation close discarded its saved history")
    ensure(api.request(path + "/messages", "POST", {"text": "late input"}, "late-" + run)[0] == 410,
           "Closed conversation accepted fresh input")
    restart_body, restart_key = {"text": "CONVERSATION_RESTART_" + run + ": begin a fresh investigation."}, "conversation-restart-" + run
    status, restarted_receipt = api.request(path + "/restart", "POST", restart_body, restart_key)
    ensure(status == 202 and restarted_receipt["attempt"] == 2 and
           restarted_receipt["conversation_id"] != conversation_id, "Restart did not allocate a fresh conversation identity")
    new_path = restarted_receipt["conversation_url"]
    ensure(api.request(path + "/restart", "POST", restart_body, restart_key) == (202, restarted_receipt),
           "Restart replay changed its new conversation receipt")
    ensure(api.request(path + "/restart", "POST", {"text": "changed restart"}, restart_key)[0] == 409,
           "Restart replay accepted changed content")
    new = checks.wait("restarted conversation has its own completed history", lambda: completed(1, new_path))
    ensure(new["summary"]["session_uid"] != uid and len(new["history"]["turns"]) == 1,
           "Restart reused a Session or mixed histories")
    new_session = checks.get("session", new["summary"]["session_name"])
    ensure(new["summary"]["environment"] == next_environment and not new["summary"].get("vaults") and
           new_session["spec"]["environmentRef"]["name"] == next_environment and not new_session["spec"].get("vaultRefs"),
           "Restart failed to select the current approved profile")
    old = detail()
    ensure(old["summary"]["active"] is False and old["summary"]["state"] == "closed" and
           old["summary"]["session_uid"] == uid and len(old["history"]["turns"]) == 2,
           "Restart made the old conversation select the fresh attempt")
    ensure(api.request(path + "/messages", "POST", {"text": "old attempt input"}, "old-" + run)[0] == 410 and
           api.request(path + "/restart", "POST", restart_body, "old-restart-" + run)[0] == 410,
           "Old conversation identity remained writable after restart")
    ensure(create() == (202, receipt), "Restart changed the original create receipt")
    status, listing = api.request(collection)
    listed = {item["conversation_id"] for item in listing["conversations"]}
    ensure(status == 200 and {conversation_id, restarted_receipt["conversation_id"]} <= listed,
           "Conversation list omitted a retained attempt")
    ensure(api.request(new_path + "/close", "POST", {}, "conversation-close-new-" + run)[0] == 202,
           "Fresh conversation could not close")
    checks.wait("fresh conversation cleaned", lambda: value if (value := detail(new_path)) and
                demo_client.cleaned_up(value) else None)
    ensure(fixture_counts()["sessions"] == [], "Conversation cleanup left remote work running")
    evidence.write("conversation-old-retained.json", old)
    evidence.write("conversation-restart-receipt.json", restarted_receipt)
    report["checks"]["conversationClosureFreshRestartAndOldIdentityReadOnly"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true", help="Keep a successful isolated cluster for manual testing")
    args = parser.parse_args()
    for executable in ("docker", "kind", "kubectl", "openssl", "make"):
        ensure(shutil.which(executable), executable + " is required")
    ensure((ROOT / "bin/kustomize").exists(), "Run make kustomize first")

    run = uuid.uuid4().hex[:12]
    cluster = "codex-e2e-" + run
    directory = report_path(cluster)
    directory.mkdir(mode=0o700)
    kubeconfig = directory / "kubeconfig"
    token = secrets.token_hex(32)
    checks = helpers.Checks(kubeconfig, cluster, {"demo-token": token})
    evidence = helpers.Evidence(directory, [token])
    config = Path.home() / ".kube/config"
    before = config.read_bytes() if config.exists() else None
    existing = checks.run(["kind", "get", "clusters"]).splitlines()
    ensure(cluster not in existing, "Generated cluster already exists; refusing to reuse it")
    report = {"cluster": cluster, "storage": "postgres", "checks": {}, "scenarios": [],
              "sourceHashes": helpers.source_hashes(),
              "provider": "simulated Agents API; no real OpenAI inference", "result": "failed"}
    created, forward = False, None
    crd_before = None
    try:
        checks.run(["make", "docker-build", "IMG=" + TAG], timeout=1200)
        checks.run(["docker", "build", "--target", "test-provider", "-t", FIXTURE_TAG, "."])
        created = True
        checks.run(["kind", "create", "cluster", "--name", cluster, "--image", helpers.NODE_IMAGE,
                    "--kubeconfig", str(kubeconfig), "--wait", "120s"])
        kubeconfig.chmod(0o600)
        checks.run(["kind", "load", "docker-image", TAG, FIXTURE_TAG, "--name", cluster])
        checks.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS,
                      "labels": {"pod-security.kubernetes.io/enforce": "restricted"}}})
        checks.secret("openai-application", {"OPENAI_API_KEY": "fixture-only-no-real-key"})
        checks.secret("gateway-auth", {"principals.json": json.dumps([
            {"token": token, "subject": "demo-user", "namespace": NS}])})
        certificate = gateway_tls.provision(checks, directory)
        (directory / "gateway.key").chmod(0o600)
        evidence.protect([(directory / "gateway.key").read_text()])
        internal = json.loads(checks.kubectl("get", "secret/gateway-internal", "-n", NS, "-o", "json"))
        evidence.protect([base64.b64decode(internal["data"]["token"]).decode()])
        checks.apply({"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "provider-fixture", "namespace": NS},
                      "data": {"server.js": (ROOT / "test/e2e/provider.js").read_text()}})
        checks.secret("gateway-database", {"url": "postgres://postgres@gateway-postgres:5432/postgres?sslmode=disable"})
        manifest = checks.run([str(ROOT / "bin/kustomize"), "build", "config/demo"])
        evidence.write("install.yaml", manifest)
        report["images"] = {tag: checks.run(["docker", "image", "inspect", tag, "--format", "{{.Id}}"])
                            for tag in (TAG, FIXTURE_TAG)}
        ensure(helpers.source_hashes() == report["sourceHashes"], "Sources changed during build; rerun acceptance")
        report["manifestSHA256"] = hashlib.sha256(manifest.encode()).hexdigest()
        checks.kubectl("apply", "-f", "-", data=manifest)
        checks.kubectl("wait", "--for=condition=Established", "crd/agents.agents.softcane.io",
                       "crd/environments.agents.softcane.io", "crd/sessions.agents.softcane.io",
                       "crd/vaults.agents.softcane.io", "crd/vaultcredentials.agents.softcane.io", "--timeout=60s")
        crd_before = lifecycle.crd_hashes(checks)
        report["crdSchemaHashes"] = crd_before
        checks.kubectl("wait", "pod/provider-fixture", "-n", NS, "--for=condition=Ready", "--timeout=90s")
        deployments = ["gateway-postgres", helpers.DEPLOYMENT, GATEWAY]
        for deployment in deployments:
            checks.kubectl("rollout", "status", "deployment/" + deployment, "-n", NS, "--timeout=180s")
        for filename, resource, name in (("agent.yaml", "agent", "demo-investigator-v1"),
                                          ("environment.yaml", "environment", "demo-text-v1")):
            checks.kubectl("apply", "-f", "config/demo/" + filename)
            checks.wait(resource + " Ready", lambda resource=resource, name=name:
                        helpers.current_ready(checks.get(resource, name)))
        checks.kubectl("apply", "--dry-run=server", "-f", "config/demo/session.yaml")
        report["checks"]["yamlAndDirectSessionAdmission"] = True

        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        context = ssl.create_default_context(cafile=str(certificate))
        api = helpers.API(port, token, context)
        base = "/v1/namespaces/" + NS

        def start_forward():
            return helpers.start_forward(checks, port)

        def available_gateway():
            try:
                return api.request(base + "/catalog")[0] == 200
            except (OSError, urllib.error.URLError):
                return False

        def replace_gateway():
            nonlocal forward
            stop_forward(forward)
            forward = None
            replace_gateway_pods(checks)
            forward = start_forward()
            checks.wait("gateway listening after process replacement", available_gateway)

        def fixture_counts():
            return json.loads(checks.kubectl("exec", "pod/provider-fixture", "-n", NS, "--", "node", "-e",
                "fetch('http://localhost:8080/counts').then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))"))

        def event(filename, **changes):
            body = json.loads((ROOT / "config/demo" / filename).read_text())
            body["work_id"] = "smoke-" + run
            body.update(changes)
            # Delivery IDs are route-scoped, including across different work IDs.
            # Leave the checked-in sample identities unused for manual testing.
            body["event_id"] = "smoke-" + run + "-" + body["event_id"]
            return api.request(base + "/work-events", "POST", body)

        forward = start_forward()
        checks.wait("authenticated gateway listening", available_gateway)
        unauthorized = helpers.API(port, "", context)
        ensure(unauthorized.request(base + "/work")[0] == 401, "Unauthenticated gateway request was accepted")
        status, receipt = event("task.json")
        ensure(status == 202 and receipt["attempt"] == 1, "Task was not durably accepted")
        path = receipt["work_url"]
        evidence.write("receipt.json", receipt)
        ensure(event("task.json") == (202, receipt), "Duplicate task changed its original receipt")
        ensure(event("task.json", text="changed content")[0] == 409, "Changed duplicate did not conflict")

        def view():
            status, value = api.request(path)
            return value if status == 200 else None

        def completed(turn_count, attempt):
            value = view()
            if not value or value["summary"]["attempt"] != attempt or not value.get("conversation"):
                return None
            turns = value["conversation"]["history"].get("turns") or []
            return value if len(turns) == turn_count and all(t["status"] == "completed" for t in turns) else None

        initial = checks.wait("task produced one saved completed turn", lambda: completed(1, 1))
        name, uid, sid = (initial["summary"]["session_name"], initial["summary"]["session_uid"],
                          initial["status"]["openaiSessionID"])
        session = checks.get("session", name)
        ensure(session["spec"]["agentRef"]["name"] == "demo-investigator-v1" and
               session["spec"]["environmentRef"]["name"] == "demo-text-v1", "Generated Session references differ from route")
        ensure(not session["status"].get("podName"), "Text-only environment created an executor")
        sessions = json.loads(checks.kubectl("get", "sessions", "-n", NS, "-o", "json"))["items"]
        ensure(len(sessions) == 1 and fixture_counts()["creates"] == 1, "Duplicate task created extra Sessions")
        report["checks"]["authenticationAcceptanceReplayConflictAndOneSession"] = True

        ensure(event("evidence.json")[0] == 202, "Evidence was not accepted")
        followup = checks.wait("evidence produced a second turn in the same Session", lambda: completed(2, 1))
        ensure(followup["summary"]["session_uid"] == uid, "Evidence replaced the Session")
        evidence.write("before-restart.json", followup)
        replace_gateway()
        restored = checks.wait("saved conversation recovered", lambda: completed(2, 1))
        ensure(restored["summary"]["session_uid"] == uid and restored["status"]["openaiSessionID"] == sid,
               "Gateway restart changed Session identity")
        ensure(event("task.json") == (202, receipt), "Replay after restart lost the original receipt")
        counts = fixture_counts()
        ensure(counts["creates"] == 1 and counts["messageCommits"] == 2, "Gateway restart duplicated remote work")
        evidence.write("after-restart.json", restored)
        report["checks"]["sameSessionEvidenceAndGatewayRestartRecovery"] = True

        def closed():
            value = view()
            return value if value and value["summary"]["state"] == "closed" and \
                all(a["cleaned"] for a in value["work"]["attempts"]) else None

        ensure(event("close.json")[0] == 202, "Close was not accepted")
        retained = checks.wait("Session deleted; work audit retained", closed)
        ensure(checks.get("session", name) is None and fixture_counts()["sessions"] == [], "Close left a Session running")
        ensure(len(retained["conversation"]["history"]["turns"]) == 2, "Close lost saved history")
        ensure(event("task.json") == (202, receipt), "Close lost task replay identity")
        ensure(event("evidence.json", event_id="late-evidence")[0] == 409, "Closed work accepted new evidence")
        ensure(event("restart.json")[0] == 202, "Explicit restart was not accepted after cleanup")
        restarted = checks.wait("explicit restart created attempt two", lambda: completed(1, 2))
        ensure(restarted["summary"]["session_uid"] != uid and fixture_counts()["creates"] == 2,
               "Explicit restart did not create a new Session")
        ensure(event("task.json") == (202, receipt), "New attempt changed the old task receipt")
        ensure(event("close.json", event_id="close-002")[0] == 202, "Second close was not accepted")
        retained = checks.wait("second attempt cleaned; both attempts retained", closed)
        ensure(fixture_counts()["sessions"] == [] and len(retained["work"]["attempts"]) == 2,
               "Second cleanup or retained attempts are incomplete")
        evidence.write("closed-work.json", retained)
        report["checks"]["closureRetainedHistoryAndExplicitNewAttempt"] = True
        test_conversations(checks, api, base, run, evidence, report, replace_gateway, fixture_counts)
        ensure(event("task.json") == (202, receipt), "Conversation flow invalidated the existing Work receipt")
        evidence.protect(["lifecycle-secret-v1", "lifecycle-secret-v2"])
        lifecycle.run(checks, ReplaySafeAPI(api), base, report, replace_gateway, fixture_counts)
        ensure(lifecycle.crd_hashes(checks) == crd_before, "Acceptance changed shipped CRD schemas")
        report["checks"]["unchangedShippedCRDSchemas"] = True
        ensure(helpers.source_hashes() == report["sourceHashes"], "Sources changed during acceptance")
        report["checks"]["sourcesUnchanged"] = True
        report["result"] = "passed"
    except Exception as error:
        report["error"] = str(error)
        report["traceback"] = traceback.format_exc()
        print("Acceptance failed:", report["error"], flush=True)
        for name, command in (
            ("pods.json", ("get", "pods", "-n", NS, "-o", "json")),
            ("events.json", ("get", "events", "-n", NS, "-o", "json")),
            ("manager.log", ("logs", "deployment/" + helpers.DEPLOYMENT, "-n", NS, "-c", "manager")),
            ("gateway.log", ("logs", "deployment/" + GATEWAY, "-n", NS, "-c", "gateway"))):
            try:
                evidence.write(name, checks.kubectl(*command))
            except Exception:
                report["diagnosticsIncomplete"] = True
    finally:
        cleanup_errors = []
        try:
            stop_forward(forward)
        except Exception as error:
            cleanup_errors.append("port-forward: " + type(error).__name__)
        keep = args.keep and report["result"] == "passed" and not cleanup_errors
        if created and not keep:
            try:
                checks.run(["kind", "delete", "cluster", "--name", cluster])
            except Exception as error:
                cleanup_errors.append("cluster: " + type(error).__name__)
        after = config.read_bytes() if config.exists() else None
        report["checks"]["globalKubeconfigUnchanged"] = before == after
        try:
            remaining = checks.run(["kind", "get", "clusters"]).splitlines()
            report["checks"]["existingClustersPreserved"] = sorted(existing) == sorted(c for c in remaining if c != cluster)
            report["checks"]["ownedClusterLifecycle"] = (cluster in remaining) == keep
        except Exception as error:
            cleanup_errors.append("verify cluster lifecycle: " + type(error).__name__)
        if cleanup_errors or not all(report["checks"].values()):
            report["result"] = "failed"
            if keep:
                try:
                    checks.run(["kind", "delete", "cluster", "--name", cluster])
                    keep = False
                except Exception as error:
                    cleanup_errors.append("failed playground cleanup: " + type(error).__name__)
        if cleanup_errors:
            report["cleanupErrors"] = cleanup_errors
        report["clusterKept"] = keep
        evidence.write("report.json", report)
    print("Demo", report["result"] + "; report:", directory / "report.json", flush=True)
    if report["result"] == "passed" and report["clusterKept"]:
        prepare_manual(directory, kubeconfig, token, port)
        print("For manual testing: source " + shlex.quote(str(directory / "env.sh")), flush=True)
        print('Then: kubectl -n "$DEMO_NAMESPACE" port-forward --address 127.0.0.1 service/' +
              GATEWAY + ' "$DEMO_PORT:8082"', flush=True)
        print("UI: https://127.0.0.1:" + str(port) + "/ui", flush=True)
        print("Use the bearer token from private " + str(directory / "curl.conf") + " in the UI token field.", flush=True)
        print("The disposable HTTPS certificate is self-signed; trust " + str(directory / "gateway.crt") + " for this local demo.", flush=True)
        print("Cleanup: kind delete cluster --name " + cluster, flush=True)
    return 0 if report["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
