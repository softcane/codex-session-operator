"""Shared no-key acceptance helpers; every Kubernetes call uses an owned context."""

import base64
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
NS = "codex-session-operator-system"
DEPLOYMENT = "codex-session-operator-controller-manager"
NODE_IMAGE = "kindest/node:v1.35.0@sha256:4613778f3cfcd10e615029370f5786704559103cf27bef934597ba562b269661"


def completed_history(value, minimum_turns):
    """Only saved completed root turns establish completion of accepted input."""
    history = value.get("history") or {}
    turns = [turn for turn in history.get("turns") or [] if not turn.get("subagent_id")]
    if any(turn.get("status") in ("failed", "cancelled") for turn in turns):
        raise RuntimeError("An agent turn failed or was cancelled; inspect the saved conversation history")
    return len(turns) >= minimum_turns and all(turn.get("status") == "completed" for turn in turns)


def cleaned_up(value):
    """Require the gateway's explicit conversation cleanup observation."""
    summary = value.get("summary") or {}
    return summary.get("state") == "closed" and summary.get("cleaned") is True


class CommandError(RuntimeError):
    def __init__(self, executable, code, detail):
        self.detail = detail
        super().__init__(f"Command failed ({code}): {executable}\n{detail}")


class Checks:
    def __init__(self, kubeconfig, cluster, keys):
        if not re.fullmatch(r"codex-(?:e2e|sandbox-live)-[a-f0-9]{12}", cluster):
            raise ValueError("Acceptance requires a generated, isolated codex-e2e cluster")
        self.env = {**os.environ, "KUBECONFIG": str(kubeconfig), "KUBECTL_KUBERC": "false"}
        self.cluster = cluster
        self.keys = keys
        self.deadline = time.monotonic() + 3600

    def run(self, args, data=None, allowed=(0,), timeout=300, sensitive=False):
        print("Run:", " ".join(args), flush=True)
        res = subprocess.run(args, input=data, text=True, capture_output=True,
                             cwd=ROOT, env=self.env, timeout=min(timeout, max(0.1, self.deadline-time.monotonic())), check=False)
        if res.returncode not in allowed:
            # Never echo request manifests, provider bodies, or key-bearing args.
            detail = "Details withheld for credential provisioning" if sensitive else (res.stderr or res.stdout)[-2500:]
            for value in self.keys.values():
                if value:
                    detail = detail.replace(value, "[withheld]")
                    detail = detail.replace(base64.b64encode(value.encode()).decode(), "[withheld]")
            raise CommandError(args[0], res.returncode, detail)
        return res.stdout.strip()

    def kubectl(self, *args, **kwargs):
        return self.run(["kubectl", "--context", "kind-" + self.cluster, *args], **kwargs)

    def apply(self, obj):
        self.kubectl("apply", "-f", "-", data=json.dumps(obj), sensitive=obj.get("kind") == "Secret")

    def get(self, resource, name):
        raw = self.kubectl("get", resource, name, "-n", NS, "-o", "json", "--ignore-not-found")
        return json.loads(raw) if raw else None

    def wait(self, label, check, timeout=90):
        previous = self.deadline
        self.deadline = min(previous, time.monotonic() + timeout)
        try:
            while time.monotonic() < self.deadline:
                value = check()
                if value:
                    print("Passed:", label, flush=True)
                    return value
                time.sleep(min(1, max(0, self.deadline-time.monotonic())))
        finally:
            self.deadline = previous
        raise RuntimeError("Timed out: " + label)

    def secret(self, name, data):
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": NS},
                    "type": "Opaque", "data": {key: base64.b64encode(value.encode()).decode() for key, value in data.items()}})


def ensure(condition, message):
    if not condition:
        raise RuntimeError(message)


def current_ready(obj):
    return bool(obj) and not obj["metadata"].get("deletionTimestamp") and any(c.get("type") == "Ready" and c.get("status") == "True"
                            and c.get("observedGeneration") == obj["metadata"]["generation"]
                            for c in obj.get("status", {}).get("conditions", []))


def prepared_executor(detail, session, pod):
    """Compare current gateway eligibility with the exact kubelet-observed instance."""
    if not detail or not detail.get("lifecycle", {}).get("execution_ready") or not current_ready(session) or not pod:
        return False
    sm, pm = session["metadata"], pod["metadata"]
    proof = session.get("status", {}).get("workspacePreparation") or {}
    if pm.get("deletionTimestamp") or detail.get("summary", {}).get("session_uid") != sm["uid"] or pm.get("uid") != proof.get("podUID"):
        return False
    if pm.get("namespace") != sm.get("namespace") or not any(owner.get("controller") and owner.get("uid") == session.get("status", {}).get("sandboxUID")
            for owner in pm.get("ownerReferences", [])):
        return False
    if not proof.get("configDigest") or not pm.get("annotations", {}).get("agents.softcane.io/executor-config"):
        return False
    if proof.get("claimUID", "") != session.get("status", {}).get("workspaceClaimUID", "") or pod.get("status", {}).get("phase") != "Running":
        return False
    if not any(condition.get("type") == "Ready" and condition.get("status") == "True" for condition in pod.get("status", {}).get("conditions", [])):
        return False
    init = pod.get("status", {}).get("initContainerStatuses", [])
    required = pod.get("spec", {}).get("initContainers", [])
    if len(init) != len(required) or any(not any(value.get("name") == item["name"] and
            value.get("state", {}).get("terminated", {}).get("exitCode") == 0 for value in init) for item in required):
        return False
    containers = pod.get("status", {}).get("containerStatuses", [])
    if len(containers) != 1:
        return False
    instance = containers[0]
    started = instance.get("state", {}).get("running", {}).get("startedAt")
    return bool(instance.get("ready") and instance.get("restartCount") == 0 and instance.get("containerID") and started and
                instance["containerID"] == proof.get("containerID") and started == proof.get("startTime") and
                instance.get("restartCount") == proof.get("restartCount"))


def controlled_by(value, owner):
    return bool(value and owner) and any(ref.get("controller") and ref.get("uid") == owner["metadata"]["uid"]
                                         for ref in value["metadata"].get("ownerReferences", []))


def sandbox_chain(checks, session, pod):
    """Verify the complete Session -> Claim -> Sandbox -> Pod ownership chain."""
    status = session["status"]
    claim = checks.get("sandboxclaim", status["sandboxClaimName"])
    sandbox = checks.get("sandbox", status["sandboxName"])
    ensure(claim and sandbox and claim["metadata"]["uid"] == status["sandboxClaimUID"] and
           sandbox["metadata"]["uid"] == status["sandboxUID"] and controlled_by(claim, session) and
           controlled_by(sandbox, claim) and controlled_by(pod, sandbox), "Sandbox ownership chain differs from Session proof")
    config = claim["metadata"].get("annotations", {}).get("agents.softcane.io/executor-config")
    ensure(config and sandbox["spec"]["podTemplate"].get("metadata", {}).get("annotations", {}).get("agents.softcane.io/executor-config") == config and
           pod["metadata"].get("annotations", {}).get("agents.softcane.io/executor-config") == config,
           "Claim, Sandbox and Pod configuration annotations differ")
    return claim, sandbox


def workspace_claim(checks, session, pod=None):
    pod = pod or checks.get("pod", session["status"]["podName"])
    volume = next(item for item in pod["spec"]["volumes"] if item["name"] == "workspace")
    name = volume.get("persistentVolumeClaim", {}).get("claimName")
    return checks.get("pvc", name) if name else None


def retained_sandbox(checks, session):
    sandbox = checks.get("sandbox", session["status"]["sandboxName"])
    ensure(sandbox and sandbox["metadata"]["uid"] == session["status"]["sandboxUID"] and
           sandbox["spec"]["operatingMode"] == "Suspended" and not sandbox["metadata"].get("ownerReferences") and
           sandbox["metadata"].get("labels", {}).get("agents.softcane.io/session-uid") == session["metadata"]["uid"] and
           checks.get("sandboxclaim", session["status"]["sandboxClaimName"]) is None,
           "Retained Sandbox must be suspended, orphaned, and match the original Session UID")
    return sandbox


def wait_executor(checks, detail, label, timeout=240, different_uid=None):
    previous = None
    def observed():
        nonlocal previous
        value = detail()
        if not value or not value.get("summary", {}).get("session_uid"):
            previous = None
            return None
        session = checks.get("session", value["summary"]["session_name"])
        name = (session or {}).get("status", {}).get("podName")
        pod = checks.get("pod", name) if name else None
        if not prepared_executor(value, session, pod) or pod["metadata"]["uid"] == different_uid:
            previous = None
            return None
        sandbox_chain(checks, session, pod)
        proof = session["status"]["workspacePreparation"]
        identity = (proof["podUID"], proof["containerID"], proof["startTime"], proof["configDigest"])
        stable = previous == identity
        previous = identity
        return (value, session, pod) if stable else None
    return checks.wait(label, observed, timeout)


def read_only_exec(checks, pod, *args, container="executor", timeout=90):
    """Only recognized API/container transitions retry narrowly read-only probes."""
    allowed = bool(args) and (args[0] in ("cat", "test") or args[:3] == ("git", "rev-parse", "HEAD"))
    ensure(allowed, "Read-only retry helper refuses effectful commands")
    deadline = min(checks.deadline, time.monotonic() + timeout)
    while True:
        try:
            return checks.kubectl("exec", "-n", NS, pod, "-c", container, "--", *args,
                                  timeout=min(15, max(0.1, deadline-time.monotonic())))
        except CommandError as error:
            text = error.detail.lower()
            transition = ("unable to upgrade connection" in text and ("container not found" in text or "container is not running" in text)) or (
                "error from server (notfound): pods" in text)
            if not transition or time.monotonic() >= deadline:
                raise
            time.sleep(min(0.5, max(0, deadline-time.monotonic())))


class Evidence:
    def __init__(self, directory, protected):
        self.directory = directory
        self.variants = []
        self.protect(protected)

    def protect(self, values):
        self.variants.extend(variant for value in values if value
                             for variant in (value, base64.b64encode(value.encode()).decode()))

    def write(self, name, value):
        text = value if isinstance(value, str) else json.dumps(value, indent=2)
        ensure(not any(secret in text for secret in self.variants),
               "Protected material detected; evidence withheld")
        path = self.directory / name
        path.write_text(text + "\n")
        path.chmod(0o600)


class API:
    def __init__(self, port, context=None):
        self.base = f"{'https' if context else 'http'}://127.0.0.1:{port}"
        self.context = context

    def request(self, suffix, method="GET", body=None, key=None):
        headers = {"Content-Type": "application/json"}
        if key:
            headers["Idempotency-Key"] = key
        request = urllib.request.Request(self.base + suffix, headers=headers, method=method,
                                         data=None if body is None else json.dumps(body).encode())
        try:
            with urllib.request.urlopen(request, timeout=15, context=self.context) as response:
                raw = response.read()
                value = json.loads(raw) if response.headers.get_content_type() == "application/json" else raw.decode()
                return response.status, value
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            return status, None


def source_hashes():
    paths = [p for parent in ("api", "internal", "cmd", "config", "test/e2e", "hack")
             for p in (ROOT / parent).rglob("*") if p.is_file()
             and p.suffix in (".go", ".sql", ".yaml", ".js", ".html", ".css", ".py", ".txt")]
    paths += [ROOT / name for name in ("go.mod", "go.sum", "Dockerfile", ".dockerignore", "Makefile", "test/postgres.py")]
    paths += list((ROOT / "examples").rglob("Dockerfile*"))
    paths += list((ROOT / ".github/workflows").glob("*.yml"))
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths)}


def start_forward(checks, port, *, pod=None, log=None):
    target = "pod/" + pod if pod else "service/codex-session-operator-gateway"
    return subprocess.Popen(["kubectl", "--context", "kind-" + checks.cluster, "port-forward",
        "--address", "127.0.0.1", "-n", NS, target, f"{port}:8082"],
        env=checks.env, stdout=log if log is not None else subprocess.DEVNULL,
        stderr=subprocess.STDOUT if log is not None else subprocess.DEVNULL)


def apply_profile(checks, route):
    """Publish concrete YAML policy; no route sanitization or implicit defaults."""
    specification = {"agentRef": {"name": route["agent"]}, "environmentRef": {"name": route["environment"]},
                     "spendControl": route["spend_control"]}
    if route.get("vaults"):
        specification["vaultRefs"] = [{"name": name} for name in route["vaults"]]
    checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "Profile",
                  "metadata": {"name": route["id"], "namespace": route["namespace"]}, "spec": specification})
    def observed():
        raw = checks.kubectl("get", "profile", route["id"], "-n", route["namespace"], "-o", "json", "--ignore-not-found")
        return current_ready(json.loads(raw)) if raw else False
    checks.wait("Profile/"+route["id"]+" current Ready", observed, 180)


def immutable_reference(tag, listing):
    """Use the imported manifest digest, never Docker's image configuration ID."""
    normalized = tag if "/" in tag else "docker.io/library/"+tag
    matches = [row.split()[2] for row in listing.splitlines()
               if len(row.split()) >= 3 and row.split()[0] == normalized]
    ensure(len(matches) == 1 and re.fullmatch(r"sha256:[0-9a-f]{64}", matches[0]),
           "Imported image has no unique immutable manifest digest")
    return normalized.rsplit(":", 1)[0]+"@"+matches[0]
