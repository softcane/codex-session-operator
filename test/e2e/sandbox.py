"""Actual controller Pod/PVC/filesystem acceptance with simulated provider/executor."""

import hashlib
import json
from pathlib import Path
import secrets
import uuid

import support
import sandbox_resources

NS, ensure = support.NS, support.ensure
REPOSITORY_HOST = "repository-fixture." + NS + ".svc"
POOL_NAMES = ("sandbox-delete", "sandbox-retain", "sandbox-missing-commit", "sandbox-rejected-auth",
              "sandbox-interrupted-command", "sandbox-rejected-executor-grant", "sandbox-unreachable-git", "sandbox-denied-egress")
PUBLIC_URL = "https://" + REPOSITORY_HOST + "/public.git"
PRIVATE_URL = "https://" + REPOSITORY_HOST + "/private.git"
UNREACHABLE_URL = "https://unreachable-repository-fixture." + NS + ".svc/public.git"


def prepare(checks, directory, run, evidence):
    """Only the public CA enters the executor's named build context."""
    certificate, key = directory / "repository.crt", directory / "repository.key"
    checks.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                "-keyout", str(key), "-out", str(certificate), "-subj", "/CN=" + REPOSITORY_HOST,
                "-addext", "subjectAltName=DNS:" + REPOSITORY_HOST], sensitive=True)
    key.chmod(0o600)
    evidence.protect([key.read_text()])
    trust = directory / "repository-ca"
    trust.mkdir(mode=0o700)
    (trust / "ca.crt").write_text(certificate.read_text())
    executor_base = "codex-executor-base:" + run
    executor, repository = "codex-test-executor:" + run, "codex-test-repository:" + run
    checks.run(["docker", "build", "--target", "executor", "-t", executor_base, "."], timeout=1200)
    checks.run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "sh", executor_base,
                "-c", "! command -v go"], timeout=30)
    checks.run(["docker", "build", "--target", "test-executor", "--build-context", "repository-ca=" + str(trust),
                "-t", executor, "."], timeout=1200)
    checks.run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "codex", executor, "exec-server", "--help"], timeout=30)
    checks.run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "sh", executor,
                "-c", "command -v go >/dev/null && go version >/dev/null"], timeout=30)
    checks.run(["docker", "build", "--target", "test-repository", "-t", repository, "."], timeout=1200)
    return {"executorBase": executor_base, "executor": executor, "repository": repository,
            "certificate": certificate, "key": key}


def repository_request(checks, path, body=None):
    options = {"hostname": REPOSITORY_HOST, "port": 443, "path": path,
               "method": "GET" if body is None else "POST"}
    program = """const https=require('node:https'),fs=require('node:fs');
const options=JSON.parse(process.argv[1]);options.ca=fs.readFileSync('/tls/tls.crt');
const request=https.request(options,response=>{let raw='';response.on('data',b=>raw+=b);
response.on('end',()=>{if(response.statusCode!==200)process.exitCode=1;else console.log(raw);});});
request.on('error',()=>{process.exitCode=1;});request.setTimeout(3000,()=>request.destroy());
request.end(process.argv[2]||'');"""
    output = checks.kubectl("exec", "pod/repository-fixture", "-n", NS, "--", "node", "-e", program,
                            json.dumps(options), "" if body is None else json.dumps(body), timeout=10)
    return json.loads(output)


def install(checks, fixture, evidence):
    token = secrets.token_hex(32)
    checks.keys["repository-token"] = token
    checks.keys["executor-token"] = "fixture-executor-key-not-real"
    evidence.protect([token, checks.keys["executor-token"]])
    checks.secret("repository-tls", {"tls.crt": fixture["certificate"].read_text(), "tls.key": fixture["key"].read_text()})
    checks.secret("repository-server-auth", {"token": token})
    checks.secret("codex-repository", {"token": token})
    checks.secret("codex-executor", {"CODEX_API_KEY": checks.keys["executor-token"]})
    checks.apply({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "repository-fixture", "namespace": NS,
                  "labels": {"app": "repository-fixture"}}, "spec": {
        "automountServiceAccountToken": False,
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "repository", "image": fixture["repository"], "imagePullPolicy": "IfNotPresent",
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
            "env": [{"name": "REPOSITORY_TOKEN", "valueFrom": {"secretKeyRef": {"name": "repository-server-auth", "key": "token"}}}],
            "volumeMounts": [{"name": "tls", "mountPath": "/tls", "readOnly": True}, {"name": "tmp", "mountPath": "/tmp"}]}],
        "volumes": [{"name": "tls", "secret": {"secretName": "repository-tls"}}, {"name": "tmp", "emptyDir": {}}]}})
    checks.apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "repository-fixture", "namespace": NS},
                  "spec": {"selector": {"app": "repository-fixture"}, "ports": [{"port": 443, "targetPort": 8443}]}})
    checks.kubectl("wait", "pod/repository-fixture", "-n", NS, "--for=condition=Ready", "--timeout=120s")
    listing = checks.run(["docker", "exec", checks.cluster + "-control-plane", "ctr", "-n", "k8s.io", "images", "ls"])
    image = support.immutable_reference(fixture["executor"], listing)
    checks.run(["docker", "exec", checks.cluster + "-control-plane", "ctr", "-n", "k8s.io", "images", "tag",
                "docker.io/library/" + fixture["executor"], image])
    architecture = checks.run(["docker", "image", "inspect", fixture["executor"], "--format", "{{.Architecture}}"])
    checks.kubectl("set", "env", "deployment/" + support.DEPLOYMENT, "-n", NS, "SANDBOX_POOLS=" + ",".join(POOL_NAMES))
    checks.kubectl("rollout", "status", "deployment/" + support.DEPLOYMENT, "-n", NS, "--timeout=180s")
    return image, architecture, repository_request(checks, "/manifest")


def provider_control(checks, body):
    program = "fetch('http://127.0.0.1:8080/control',{method:'POST',headers:{'Content-Type':'application/json'},body:process.argv[1]}).then(r=>{if(!r.ok)process.exitCode=1;})"
    checks.kubectl("exec", "pod/provider-fixture", "-n", NS, "--", "node", "-e", program, json.dumps(body))


def profile(checks, name, image, architecture, commit, policy="Delete", private=False, repository_url=None):
    for resource in sandbox_resources.resources(name, NS, image, architecture,
            repository_url or (PRIVATE_URL if private else PUBLIC_URL), commit,
            private=private, fixture=True):
        checks.apply(resource)
    checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "Environment", "metadata": {"name": name, "namespace": NS},
                  "spec": {"mode": "SelfHosted", "deletionPolicy": "Delete", "selfHosted": {
                      "sandboxPoolRef": {"name": name}, "workspace": {"directory": "/workspace", "deletionPolicy": "Retain" if policy == "Retain" else "Delete"}}}})
    checks.wait("Environment/" + name + " current Ready", lambda: support.current_ready(checks.get("environment", name)), 180)
    support.apply_profile(checks, {"id": name, "namespace": NS, "agent": "demo-investigator-v1", "environment": name,
                                  "spend_control": {"mode": "Limited", "limitCents": 500}})


def binding(checks, detail, image, expected_commit):
    summary = detail["summary"]
    session = checks.get("session", summary["session_name"])
    ensure(session and session["metadata"]["uid"] == summary["session_uid"], "Conversation Session identity mismatch")
    pod = checks.get("pod", session["status"]["podName"])
    ensure(support.prepared_executor(detail, session, pod), "Executor eligibility proof does not match the current process instance")
    support.sandbox_chain(checks, session, pod)
    spec, container = pod["spec"], pod["spec"]["containers"][0]
    ensure(container["image"] == image and spec["automountServiceAccountToken"] is False and
           spec["securityContext"]["runAsNonRoot"] and container["securityContext"]["readOnlyRootFilesystem"] and
           container["securityContext"]["allowPrivilegeEscalation"] is False and
           container["securityContext"]["capabilities"]["drop"] == ["ALL"], "Executor security or image contract differs")
    init = next(item for item in spec["initContainers"] if item["name"] == "repository-prepare")
    ensure(init["command"] == ["/workspace-bootstrap"] and "CODEX_API_KEY" not in json.dumps(init),
           "Preparation must use the fixed helper without executor credentials")
    ensure(not any(mount["mountPath"].startswith("/repository-credentials") for mount in container["volumeMounts"]),
           "Repository credentials leaked into executor mounts")
    ensure(any(mount.get("subPath") == "project" and mount["mountPath"] == "/workspace" for mount in container["volumeMounts"]),
           "Executor project subtree is not isolated from preparation provenance")
    head = support.read_only_exec(checks, pod["metadata"]["name"], "git", "rev-parse", "HEAD")
    ensure(head == expected_commit, "Initial prepared repository commit differs")
    claim = support.workspace_claim(checks, session, pod)
    ensure(claim and claim["metadata"]["uid"] == session["status"]["workspaceClaimUID"] and claim["status"]["phase"] == "Bound",
           "Frozen workspace UID or binding differs")
    return session, pod, claim


def run(checks, api, base, report, evidence, runtime, replace_gateway, fixture_counts):
    image, architecture, commits = runtime
    report["sandboxLayer"] = "actual restricted controller Pods/PVCs; simulated executor/provider; HTTPS Git fixtures"

    def detail(path):
        status, value = api.request(path)
        return value if status == 200 else None

    def bound(path):
        value = detail(path)
        if value and value["summary"].get("session_uid"):
            session = checks.get("session", value["summary"]["session_name"])
            return value if support.current_ready(session) else None
        return None

    def closed(path):
        value = detail(path)
        return value if value and support.cleaned_up(value) else None

    for policy, private in (("Delete", False), ("Retain", True)):
        name = "sandbox-" + policy.lower()
        commit = commits["private" if private else "public"]
        profile(checks, name, image, architecture, commit, policy, private)
        body = {"route_id": name, "text": "SANDBOX_PROOF: inspect the prepared repository and proof.txt"}
        # Hold preparation before acceptance. The model fixture cannot execute until init qualifies.
        repository_request(checks, "/control", {"held": True})
        provider_control(checks, {"executorHold": True})
        before = fixture_counts()["messageCommits"]
        status, receipt = api.request(base + "/conversations", "POST", body, name + "-create")
        ensure(status == 202, "Sandbox input was not durably accepted")
        path = receipt["conversation_url"]
        held = checks.wait("Sandbox Session mapping while preparation is held", lambda: value if
                           (value := detail(path)) and value["summary"].get("session_uid") else None)
        session_name = held["summary"]["session_name"]
        checks.wait("Preparation init is observed before normal delivery", lambda: pod if
                    (session := checks.get("session", session_name)) and session.get("status", {}).get("podName") and
                    (pod := checks.get("pod", session["status"]["podName"])) and pod.get("status", {}).get("initContainerStatuses") else None)
        ensure(fixture_counts()["messageCommits"] == before, "Unprepared sandbox received provider task input")
        if policy == "Delete":
            checks.kubectl("rollout", "restart", "deployment/" + support.DEPLOYMENT, "-n", NS)
            checks.kubectl("rollout", "status", "deployment/" + support.DEPLOYMENT, "-n", NS, "--timeout=180s")
            replace_gateway()
            ensure(api.request(base + "/conversations", "POST", body, name + "-create") == (202, receipt),
                   "Held request lost its original receipt during service replacement")
            ensure(fixture_counts()["messageCommits"] == before, "Service replacement bypassed preparation hold")
        # The first simulated command expects a filesystem-only nonce; seed it only after init.
        repository_request(checks, "/control", {"held": False})
        value, session, pod = support.wait_executor(checks, lambda: detail(path), "current prepared sandbox executor instance")
        pod_name = session["status"]["podName"]
        nonce = uuid.uuid4().hex
        checks.kubectl("exec", "-i", "-n", NS, pod_name, "--", "node", "-e",
                       "require('fs').writeFileSync('proof.txt',require('fs').readFileSync(0,'utf8'))", data=nonce)
        provider_control(checks, {"executorHold": False})
        completed = checks.wait("saved simulated root and actual Pod command", lambda: value if
                               (value := detail(path)) and support.completed_history(value, 1) else None, 240)
        current_detail, _, _ = support.wait_executor(checks, lambda: detail(path), "current executor after completed root")
        session, pod, claim = binding(checks, current_detail, image, commit)
        findings = json.loads(support.read_only_exec(checks, pod_name, "cat", "/workspace/fixture-report.json"))
        ensure(findings["nonce"] == nonce and findings["test_exit"] == 1 and "1099" in findings["output"] and "1000" in findings["output"],
               "Actual executor file/test output differs from the independent fixture oracle")
        commands = [item for item in completed["history"]["items"] if item.get("type") == "command_execution"]
        ensure(commands and nonce in commands[-1]["output"] and commands[-1]["cwd"] == "/workspace",
               "Saved simulated history does not describe actual Pod command output")
        before_uid = pod["metadata"]["uid"]
        remote_id = session["status"]["openaiSessionID"]
        checks.kubectl("exec", "-n", NS, pod_name, "--", "sh", "-c",
                       "printf persistent-change > committed.txt && git add committed.txt && git -c user.name=Fixture -c user.email=fixture@example.invalid commit -qm 'Session change' && printf dirty-change > dirty.txt && rm README.md")
        later_commit = support.read_only_exec(checks, pod_name, "git", "rev-parse", "HEAD")
        checks.kubectl("delete", "pod", pod_name, "-n", NS, "--wait=true", "--timeout=120s")
        _, current, replacement = support.wait_executor(checks, lambda: detail(path), "different current prepared executor instance",
                                                        timeout=180, different_uid=before_uid)
        ensure(current["metadata"]["uid"] == session["metadata"]["uid"] and current["status"]["openaiSessionID"] == remote_id,
               "Executor replacement changed the local or remote Session")
        ensure(current["status"]["workspaceClaimUID"] == claim["metadata"]["uid"], "Replacement changed the PVC UID")
        ensure(support.read_only_exec(checks, pod_name, "cat", "/workspace/proof.txt") == nonce and
               support.read_only_exec(checks, pod_name, "git", "rev-parse", "HEAD") == later_commit and
               support.read_only_exec(checks, pod_name, "cat", "/workspace/dirty.txt") == "dirty-change",
               "Preparation overwrote persistent nonce, later commit, or dirty work")
        support.read_only_exec(checks, pod_name, "test", "!", "-e", "/workspace/README.md")
        ensure(api.request(path + "/messages", "POST", {"text": "SANDBOX_PROOF: continue after replacement"}, name + "-followup")[0] == 202,
               "Follow-up was not accepted")
        checks.wait("new saved root after executor replacement", lambda: value if
                    (value := detail(path)) and support.completed_history(value, 2) else None, 240)
        ensure(api.request(path + "/close", "POST", {}, name + "-close")[0] == 202, "Sandbox close was not accepted")
        checks.wait("sandbox closed and cleaned", lambda: closed(path), 240)
        ensure(checks.get("session", session_name) is None and checks.get("pod", pod_name) is None and
               not any(value["id"] == remote_id for value in fixture_counts()["sessions"]), "Sandbox cleanup left execution resources")
        if policy == "Delete":
            ensure(checks.get("pvc", claim["metadata"]["name"]) is None, "Delete policy left its workspace PVC")
        if policy == "Retain":
            retained = checks.get("pvc", claim["metadata"]["name"])
            ensure(retained and retained["metadata"]["uid"] == claim["metadata"]["uid"] and retained["metadata"].get("ownerReferences"),
                   "Retain policy did not preserve its Sandbox-owned PVC")
            support.retained_sandbox(checks, session)
            probe = read_only_probe(checks, image, claim["metadata"]["name"])
            ensure(support.read_only_exec(checks, probe, "cat", "/workspace/proof.txt", container="reader") == nonce,
                   "Independent read-only Pod could not recover retained files")
            checks.kubectl("delete", "pod", probe, "-n", NS, "--wait=true", "--timeout=90s")
        status, fresh = api.request(path + "/restart", "POST", {"text": "SANDBOX_PROOF: fresh workspace"}, name + "-fresh")
        ensure(status == 202 and fresh["conversation_id"] != receipt["conversation_id"], "Start fresh reused conversation identity")
        # Stop queued execution before seeding; this separately proves cleanup while input is queued.
        fresh_path = fresh["conversation_url"]
        mapping = checks.wait("fresh Session has independent mapping", lambda: value if
            (value := detail(fresh_path)) and value["summary"].get("session_uid") else None)
        fresh_session = checks.get("session", mapping["summary"]["session_name"])
        ensure(fresh_session["metadata"]["uid"] != session["metadata"]["uid"], "Start fresh reused Session UID")
        _, obj, _ = support.wait_executor(checks, lambda: detail(fresh_path), "fresh current prepared executor instance")
        ensure(obj["status"]["openaiSessionID"] != remote_id and obj["status"]["workspaceClaimUID"] != claim["metadata"]["uid"],
               "Start fresh adopted a prior remote Session or PVC")
        support.read_only_exec(checks, obj["status"]["podName"], "test", "!", "-e", "/workspace/dirty.txt")
        ensure(support.read_only_exec(checks, obj["status"]["podName"], "git", "rev-parse", "HEAD") == commit,
               "Start fresh did not independently prepare its pinned repository")
        ensure(api.request(fresh_path + "/close", "POST", {}, name + "-fresh-close")[0] == 202, "Fresh close failed")
        checks.wait("fresh attempt cleaned", lambda: closed(fresh_path), 240)
        if policy == "Retain":
            for retained_session in (session, obj):
                owned = support.retained_sandbox(checks, retained_session)
                checks.kubectl("delete", "sandbox", owned["metadata"]["name"], "-n", NS, "--wait=true", "--timeout=90s")
        evidence.write(name + "-runtime-identities.json", {"sessionUID": session["metadata"]["uid"], "remoteID": remote_id,
            "oldPodUID": before_uid, "newPodUID": replacement["metadata"]["uid"], "pvcUID": claim["metadata"]["uid"] if claim else None,
            "initialCommit": commit, "laterCommit": later_commit, "reportSHA256": hashlib.sha256(json.dumps(findings, sort_keys=True).encode()).hexdigest()})
        report["scenarios"].append({"id": name + "_actual_pod_files_preparation_replacement_cleanup", "status": "passed"})
        checks.kubectl("delete", "profile/" + name, "environment/" + name, "-n", NS, "--wait=true", "--timeout=120s")
    # Negative preparation is observed in real init containers, never simulated by status patches.
    for failure in ("missing-commit", "rejected-auth"):
        name = "sandbox-" + failure
        private = failure == "rejected-auth"
        if private:
            invalid = secrets.token_hex(32)
            evidence.protect([invalid])
            checks.keys["invalid-repository-token"] = invalid
            checks.secret("codex-repository", {"token": invalid})
        profile(checks, name, image, architecture, commits["private"] if private else "f" * 40, "Delete", private)
        before = fixture_counts()["messageCommits"]
        status, receipt = api.request(base + "/conversations", "POST", {"route_id": name, "text": "Input must remain held"}, name + "-create")
        ensure(status == 202, "Failure scenario did not preserve durable acceptance")
        path = receipt["conversation_url"]
        def failed_init():
            value = detail(path)
            if not value or not value["summary"].get("session_uid"):
                return None
            session = checks.get("session", value["summary"]["session_name"])
            pod = checks.get("pod", session.get("status", {}).get("podName", "")) if session.get("status", {}).get("podName") else None
            for item in (pod or {}).get("status", {}).get("initContainerStatuses", []):
                terminated = item.get("state", {}).get("terminated") or item.get("lastState", {}).get("terminated")
                if terminated and terminated.get("exitCode") != 0 and not support.current_ready(session):
                    return session
            return None
        failed = checks.wait("actual preparation failure is safely held", failed_init, 240)
        ensure(fixture_counts()["messageCommits"] == before, "Failed preparation delivered normal task input")
        ensure(api.request(path + "/close", "POST", {}, name + "-close")[0] == 202, "Close was blocked by failed preparation")
        checks.wait("failed preparation cleanup", lambda: closed(path), 240)
        ensure(checks.get("session", failed["metadata"]["name"]) is None, "Failed preparation left a Session")
        report["scenarios"].append({"id": name + "_held_and_closed_without_delivery", "status": "passed"})
        checks.kubectl("delete", "profile/" + name, "environment/" + name, "-n", NS, "--wait=true", "--timeout=120s")


def read_only_probe(checks, image, claim):
    name = "retained-reader-" + uuid.uuid4().hex[:8]
    checks.apply({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": NS}, "spec": {
        "automountServiceAccountToken": False, "restartPolicy": "Never",
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000, "fsGroup": 1000,
                            "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "reader", "image": image, "imagePullPolicy": "IfNotPresent", "command": ["sh", "-c", "sleep 600"],
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": [{"name": "workspace", "mountPath": "/workspace", "subPath": "project", "readOnly": True}]}],
        "volumes": [{"name": "workspace", "persistentVolumeClaim": {"claimName": claim, "readOnly": True}}]}})
    checks.kubectl("wait", "pod/" + name, "-n", NS, "--for=condition=Ready", "--timeout=120s")
    return name
