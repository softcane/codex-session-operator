"""Current CRD lifecycle scenarios within the runner's already-owned cluster.

Profile creation/edit/retirement/recreation and namespace independence are covered
by run.py; all six shipped schemas are fingerprinted here.
"""

import hashlib
import json

import support as helpers

NS, ensure = helpers.NS, helpers.ensure


def current_condition(obj, reason=None):
    if not obj:
        return False
    return any(c["type"] == "Ready" and c.get("observedGeneration") == obj["metadata"]["generation"] and
               (c["reason"] == reason and c["status"] == "False" if reason else c["status"] == "True")
               for c in obj.get("status", {}).get("conditions", []))


def projection(obj):
    status = obj.get("status", {})
    return {"name": obj["metadata"]["name"], "uid": obj["metadata"]["uid"],
            "generation": obj["metadata"]["generation"], "deleting": bool(obj["metadata"].get("deletionTimestamp")),
            "conditions": status.get("conditions", []),
            "identities": {k: status.get(k) for k in ("openaiSessionID", "openaiAgentID", "openaiVaultID",
                           "openaiCredentialID", "openaiTemplateID", "podName", "workspaceClaimUID")}}


def crd_hashes(checks):
    return {name: hashlib.sha256(checks.kubectl("get", "crd", name + ".agents.softcane.io",
            "-o", "jsonpath={.spec.versions}").encode()).hexdigest()
            for name in ("agents", "environments", "sessions", "vaults", "vaultcredentials", "profiles")}


def run(checks, api, base, report, replace_gateway, fixture_counts):
    initial = fixture_counts()

    def provider(path, body=None, method=None):
        options = {"method": method or ("POST" if body is not None else "GET")}
        if body is not None:
            options.update({"headers": {"Content-Type": "application/json"}, "body": json.dumps(body)})
        program = "fetch('http://127.0.0.1:8080/" + path + "'," + json.dumps(options) + ").then(async r=>console.log(JSON.stringify({status:r.status,body:await r.json()})))"
        value = json.loads(checks.kubectl("exec", "pod/provider-fixture", "-n", NS, "--", "node", "-e", program))
        return value["status"], value["body"]

    def counts():
        current = fixture_counts()
        return {key: value - initial[key] if isinstance(value, (int, float)) else value
                for key, value in current.items()}

    def pods(selector):
        values = json.loads(checks.kubectl("get", "pods", "-n", NS, "-l", selector, "-o", "json"))["items"]
        return sorted(p["metadata"]["uid"] for p in values if not p["metadata"].get("deletionTimestamp") and
                      any(c["type"] == "Ready" and c["status"] == "True" for c in p.get("status", {}).get("conditions", [])))

    def same_claim(before, after, field):
        ensure(after["metadata"]["uid"] == before["metadata"]["uid"] and
               not after["metadata"].get("deletionTimestamp") and after["status"][field] == before["status"][field],
               "Externally removed dependency identity was reset or Kubernetes CR was replaced")

    def apply(kind, name, desired):
        checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": kind,
                      "metadata": {"name": name, "namespace": NS}, "spec": desired})

    def ready(kind, name):
        return checks.wait(kind + "/" + name + " current Ready", lambda: obj if
                           current_condition(obj := checks.get(kind.lower(), name)) else None, 120)

    def reason(kind, name, expected):
        return checks.wait(kind + "/" + name + " " + expected, lambda: obj if
                           current_condition(obj := checks.get(kind.lower(), name), expected) else None, 120)

    def passed(identifier, detail):
        report["scenarios"].append({"id": identifier, "status": "passed", "detail": detail})
        print("Scenario passed:", identifier, flush=True)

    session_spec = {"agentRef": {"name": "later-agent"}, "environmentRef": {"name": "later-env"},
                    "spendControl": {"mode": "Unlimited"}}
    apply("Session", "late-session", session_spec)
    held = reason("Session", "late-session", "EnvironmentNotReady")
    ensure(not held.get("status", {}).get("creationAttempted") and counts()["creates"] == 0,
           "Missing dependencies consumed a remote creation claim")
    apply("Agent", "later-agent", {"model": "gpt-6-luna", "name": "original name", "instructions": "original instruction",
                                  "tools": [], "deletionPolicy": "Delete"})
    agent = ready("Agent", "later-agent")
    apply("Environment", "later-env", {"mode": "None", "deletionPolicy": "Delete"})
    ready("Environment", "later-env")
    reason("Session", "late-session", "GatewayInputRequired")
    ensure(counts()["creates"] == 0, "None mode created without durable initial input")
    passed("dependencies_then_input", "Missing refs hold the claim; none mode awaits durable first input")

    def message(name, text, key):
        return api.request(base + "/sessions/" + name + "/messages", "POST", {"text": text}, key)

    def completed(name, number):
        status, value = api.request(base + "/sessions/" + name + "/history")
        if status != 200:
            return None
        turns = value.get("history", {}).get("turns") or []
        return value if len(turns) == number and all(t["status"] == "completed" for t in turns) else None

    ensure(message("late-session", "first input", "first")[0] == 202, "First message acceptance failed")
    first = ready("Session", "late-session")
    checks.wait("saved completed turn", lambda: completed("late-session", 1))
    ensure(message("late-session", "first input", "first")[0] == 202, "Exact replay failed")
    ensure(message("late-session", "changed input", "first")[0] == 409, "Changed replay accepted")
    ensure(counts()["creates"] == 1 and counts()["messageCommits"] == 1, "Replay created duplicate remote work")
    passed("create_and_message_replay", "One remote Session and one logical input, with changed replay rejected")
    sid = first["status"]["openaiSessionID"]
    old_remote = provider("v1/agents/sessions/" + sid)[1]
    checks.kubectl("patch", "agent/later-agent", "-n", NS, "--type=merge", "-p",
                   json.dumps({"spec": {"name": "updated name", "instructions": "updated instruction"}}))
    ready("Agent", "later-agent")
    apply("Session", "next-session", session_spec)
    reason("Session", "next-session", "GatewayInputRequired")
    ensure(message("next-session", "second session", "next")[0] == 202, "Second Session message failed")
    second = ready("Session", "next-session")
    saved_history = checks.wait("second Session saved completed turn", lambda: completed("next-session", 1))["history"]
    sid2 = second["status"]["openaiSessionID"]
    remote2 = provider("v1/agents/sessions/" + sid2)[1]
    ensure(sid != sid2 and old_remote["agent"]["name"] == "original name" and remote2["agent"]["name"] == "updated name" and
           old_remote["agent"]["instructions"] == "original instruction" and remote2["agent"]["instructions"] == "updated instruction",
           "Future Session did not copy updated Agent independently")
    ensure(provider("v1/agents/sessions/" + sid)[1]["agent"] == old_remote["agent"], "Existing Session configuration changed")
    passed("frozen_and_future_agent", "Distinct remote Sessions retain old/new copied names and instructions")

    ensure(provider("v1/agents/sessions/" + sid2, method="DELETE")[0] == 200, "External delete failed")
    ensure(provider("v1/agents/sessions/" + sid2)[0] == 404, "Accepted external delete did not remove remote Session")
    missing = reason("Session", "next-session", "RemoteSessionMissing")
    ensure(missing["metadata"]["uid"] == second["metadata"]["uid"] and not missing["metadata"].get("deletionTimestamp") and
           missing["status"]["openaiSessionID"] == sid2, "External delete replaced or deleted the CR")
    before_fence = api.request(base + "/sessions/next-session")[1]["submissions"]
    ensure(message("next-session", "must be fenced", "after-console-delete")[0] in (410, 503), "Missing remote accepted new input")
    ensure(api.request(base + "/sessions/next-session")[1]["submissions"] == before_fence,
           "Fenced message changed durable submissions")
    history_status, history = api.request(base + "/sessions/next-session/history")
    ensure(history_status == 200 and history["history"] == saved_history, "Saved transcript changed after remote deletion")
    ensure(counts()["creates"] == 2, "Missing remote was silently recreated")
    passed("external_session_delete", projection(missing))

    old_manager, old_gateways = pods("control-plane=controller-manager"), pods("app.kubernetes.io/component=gateway")
    ensure(len(old_manager) == 1 and len(old_gateways) == 2, "Expected ready manager/two gateway Pods before restart")
    checks.kubectl("rollout", "restart", "deployment/" + helpers.DEPLOYMENT, "-n", NS)
    checks.kubectl("rollout", "status", "deployment/" + helpers.DEPLOYMENT, "-n", NS, "--timeout=180s")
    replace_gateway()
    new_manager, new_gateways = pods("control-plane=controller-manager"), pods("app.kubernetes.io/component=gateway")
    ensure(len(new_manager) == 1 and len(new_gateways) == 2 and not set(old_manager) & set(new_manager) and
           not set(old_gateways) & set(new_gateways), "Deployment restart did not replace all owned Pods")
    report["podReplacement"] = {"oldManager": old_manager, "newManager": new_manager, "oldGateways": old_gateways, "newGateways": new_gateways}
    missing = reason("Session", "next-session", "RemoteSessionMissing")
    ensure(missing["status"]["openaiSessionID"] == sid2 and counts()["creates"] == 2, "Restart forgot missing identity")
    ensure(message("next-session", "still fenced", "after-restart")[0] in (410, 503), "Restart revived input")
    ensure(api.request(base + "/sessions/next-session")[1]["submissions"] == before_fence,
           "Restart accepted a mutation of the fenced conversation")
    history_status, history = api.request(base + "/sessions/next-session/history")
    ensure(history_status == 200 and history["history"] == saved_history, "Restart lost retained transcript")
    passed("restart_after_external_delete", "Actual manager/gateway Pod replacement retains identity and input fencing")

    checks.kubectl("delete", "session/next-session", "-n", NS, "--wait=true", "--timeout=120s")
    ensure(checks.get("session", "next-session") is None, "External-delete CR cleanup failed")
    checks.kubectl("delete", "session/late-session", "-n", NS, "--wait=true", "--timeout=120s")
    ensure(provider("v1/agents/sessions/" + sid)[0] == 404, "Cluster deletion left remote Session")
    passed("local_delete_after_external_and_normal", "Both already-absent and normal owned remote cleanup complete")

    apply("Vault", "lifecycle-vault", {"name": "Lifecycle Vault", "deletionPolicy": "Delete"})
    vault = ready("Vault", "lifecycle-vault")
    checks.secret("codex-vault", {"test-token": "lifecycle-secret-v1"})
    apply("VaultCredential", "lifecycle-credential", {"vaultRef": {"name": "lifecycle-vault"}, "name": "Lifecycle credential",
          "auth": {"type": "environment_variable", "environmentVariable": {"variableName": "LIFECYCLE_TOKEN",
                   "secretValueSecretRef": {"name": "codex-vault", "key": "test-token"},
                   "networking": {"type": "limited", "allowedHosts": ["example.com"]}}}, "deletionPolicy": "Delete"})
    credential = ready("VaultCredential", "lifecycle-credential")
    cid = credential["status"]["openaiCredentialID"]
    ensure(counts()["credentialWrites"] == 1, "Expected one acknowledged initial credential write")
    checks.secret("codex-vault", {"test-token": "lifecycle-secret-v2"})
    version = json.loads(checks.kubectl("get", "secret/codex-vault", "-n", NS, "-o", "json"))["metadata"]["resourceVersion"]
    checks.wait("rotation exact source acknowledgment", lambda: obj if (obj := checks.get("vaultcredential", "lifecycle-credential")) and
                obj.get("status", {}).get("sourceVersions", [{}])[0].get("resourceVersion") == version and current_condition(obj) else None)
    same_claim(credential, checks.get("vaultcredential", "lifecycle-credential"), "openaiCredentialID")
    ensure(counts()["credentialCreates"] == 1 and counts()["credentialWrites"] == 2,
           "Rotation did not acknowledge exactly one remote update of the existing credential")
    passed("credential_rotation_redacted", "Same credential ID and exact source-version acknowledgment; no secret GET verification claim")
    vid = vault["status"]["openaiVaultID"]
    ensure(provider("v1/vaults/" + vid + "/credentials/" + cid, method="DELETE")[0] == 200, "External credential deletion failed")
    same_claim(credential, reason("VaultCredential", "lifecycle-credential", "ObservationFailed"), "openaiCredentialID")
    ensure(counts()["credentialCreates"] == 1, "Missing credential recreated")
    checks.kubectl("delete", "vaultcredential/lifecycle-credential", "-n", NS, "--wait=true", "--timeout=120s")
    ensure(provider("v1/vaults/" + vid, method="DELETE")[0] == 200, "External Vault deletion failed")
    same_claim(vault, reason("Vault", "lifecycle-vault", "ObservationFailed"), "openaiVaultID")
    ensure(counts()["vaultCreates"] == 1, "Missing Vault recreated")
    checks.kubectl("delete", "vault/lifecycle-vault", "-n", NS, "--wait=true", "--timeout=120s")
    passed("external_vault_and_credential_delete", "Each original identity remains claimed; explicit CR cleanup completes")

    apply("Environment", "hosted-template", {"mode": "OpenAIHosted", "deletionPolicy": "Delete",
          "openaiHosted": {"name": "scenario", "containerSize": "small", "network": {"access": "disabled"}}})
    hosted = ready("Environment", "hosted-template")
    tid = hosted["status"]["openaiTemplateID"]
    ensure(provider("v1/agents/environments/templates/" + tid, method="DELETE")[0] == 200, "External template delete failed")
    same_claim(hosted, reason("Environment", "hosted-template", "ObservationFailed"), "openaiTemplateID")
    ensure(counts()["templateCreates"] == 1, "Missing template recreated")
    checks.kubectl("delete", "environment/hosted-template", "-n", NS, "--wait=true", "--timeout=120s")
    aid = agent["status"]["openaiAgentID"]
    ensure(provider("v1/agents/" + aid, method="DELETE")[0] == 200, "External Agent deletion failed")
    same_claim(agent, reason("Agent", "later-agent", "ObservationFailed"), "openaiAgentID")
    ensure(counts()["agentCreates"] == 1, "Missing Agent recreated")
    checks.kubectl("delete", "agent/later-agent", "environment/later-env", "-n", NS, "--wait=true", "--timeout=120s")
    passed("external_agent_and_template_delete", "Missing dependencies never recreate; explicit local cleanup releases their obligations")
    remaining = counts()
    ensure(all(remaining[k] == initial[k] for k in ("sessions", "agents", "vaults", "templates", "credentials")), "Lifecycle resources leaked or altered existing profiles")
    report["providerCounts"] = remaining
