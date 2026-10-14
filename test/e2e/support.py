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


class Checks:
    def __init__(self, kubeconfig, cluster, keys):
        if not re.fullmatch(r"codex-e2e-[a-f0-9]{12}", cluster):
            raise ValueError("Acceptance requires a generated, isolated codex-e2e cluster")
        self.env = {**os.environ, "KUBECONFIG": str(kubeconfig), "KUBECTL_KUBERC": "false"}
        self.cluster = cluster
        self.keys = keys

    def run(self, args, data=None, allowed=(0,), timeout=300, sensitive=False):
        print("Run:", " ".join(args), flush=True)
        res = subprocess.run(args, input=data, text=True, capture_output=True,
                             cwd=ROOT, env=self.env, timeout=timeout, check=False)
        if res.returncode not in allowed:
            # Never echo request manifests, provider bodies, or key-bearing args.
            detail = "Details withheld for credential provisioning" if sensitive else (res.stderr or res.stdout)[-2500:]
            for value in self.keys.values():
                if value:
                    detail = detail.replace(value, "[withheld]")
            raise RuntimeError(f"Command failed ({res.returncode}): {args[0]}\n{detail}")
        return res.stdout.strip()

    def kubectl(self, *args, **kwargs):
        return self.run(["kubectl", "--context", "kind-" + self.cluster, *args], **kwargs)

    def apply(self, obj):
        self.kubectl("apply", "-f", "-", data=json.dumps(obj), sensitive=obj.get("kind") == "Secret")

    def get(self, resource, name):
        raw = self.kubectl("get", resource, name, "-n", NS, "-o", "json", "--ignore-not-found")
        return json.loads(raw) if raw else None

    def wait(self, label, check, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = check()
            if value:
                print("Passed:", label, flush=True)
                return value
            time.sleep(1)
        raise RuntimeError("Timed out: " + label)

    def secret(self, name, data):
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": NS},
                    "type": "Opaque", "data": {key: base64.b64encode(value.encode()).decode() for key, value in data.items()}})


def ensure(condition, message):
    if not condition:
        raise RuntimeError(message)


def current_ready(obj):
    return bool(obj) and any(c.get("type") == "Ready" and c.get("status") == "True"
                            and c.get("observedGeneration") == obj["metadata"]["generation"]
                            for c in obj.get("status", {}).get("conditions", []))


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
    def __init__(self, port, token, context):
        self.base = f"https://127.0.0.1:{port}"
        self.token = token
        self.context = context

    def request(self, suffix, method="GET", body=None, key=None):
        headers = {"Authorization": "Bearer " + self.token,
                   "Content-Type": "application/json"}
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
             and p.suffix in (".go", ".yaml", ".js", ".html", ".css", ".py", ".txt")]
    paths += [ROOT / name for name in ("go.mod", "go.sum", "Dockerfile", ".dockerignore", "Makefile")]
    return {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(paths)}


def start_forward(checks, port):
    return subprocess.Popen(["kubectl", "--context", "kind-" + checks.cluster, "port-forward",
        "--address", "127.0.0.1", "-n", NS, "service/codex-session-operator-gateway", f"{port}:8082"],
        env=checks.env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
