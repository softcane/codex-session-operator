#!/usr/bin/env python3
"""Start fresh local infrastructure for hands-on use of the real OpenAI API."""

import argparse
import base64
import hashlib
import errno
import json
import re
import os
from pathlib import Path
import shlex
import socket
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "test/e2e"))
from support import NODE_IMAGE, NS, DEPLOYMENT, immutable_reference, source_hashes
import sandbox_resources

CLUSTER = "codex-local"
CONTEXT = "kind-" + CLUSTER
CONTROLLER_IMAGE = "codex-session-controller:local"
GATEWAY_IMAGE = "codex-session-gateway:local"
GATEWAY = "codex-session-operator-gateway"
KINDS = ("agents", "environments", "vaults", "vaultcredentials", "sessions", "profiles")
REPOSITORY_URL = "https://github.com/google/uuid.git"
REPOSITORY_COMMIT = "0f11ee6918f41a04c201eceeadf612a377bc7fbc"
EXECUTOR_BASE_TAG = "codex-executor:local"
APPLICATION_EXECUTOR_TAG = "my-go-project-executor:local"


PREFERRED_PORT = 18082


def choose_port(requested=None):
    """Check an explicit port or select a free loopback port before setup starts."""
    if requested is not None and not 1 <= requested <= 65535:
        raise ValueError("Choose a valid loopback port")
    with socket.socket() as available:
        try:
            available.bind(("127.0.0.1", PREFERRED_PORT if requested is None else requested))
        except OSError as error:
            if requested is not None:
                raise ValueError("Requested loopback port is unavailable; choose another --port") from None
            if error.errno != errno.EADDRINUSE:
                raise ValueError("Could not select a loopback gateway port") from None
            available.bind(("127.0.0.1", 0))
        return available.getsockname()[1]


def configuration_key(environment, dotenv, name):
    if name not in ("OPENAI_API_KEY", "OPENAI_EXECUTOR_API_KEY"):
        raise ValueError("Unsupported credential name")
    value = environment.get(name, "")
    if not value and dotenv.exists():
        for line in dotenv.read_text().splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[7:].strip()
            field, separator, raw = line.partition("=")
            if not separator or field.strip() != name:
                continue
            try:
                parts = shlex.split(raw, comments=True)
            except ValueError:
                raise ValueError(name + " in .env has invalid quoting") from None
            if len(parts) != 1:
                raise ValueError(name + " in .env must contain one literal value")
            value = parts[0]
    if not value or any(character.isspace() for character in value) or "\x00" in value:
        raise ValueError("Set " + name + " in the process environment or a private .env file")
    return value


def application_key(environment, dotenv):
    return configuration_key(environment, dotenv, "OPENAI_API_KEY")

def private_write(path, text):
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(text)


class Local:
    def __init__(self, directory, key):
        self.env = {**os.environ, "KUBECONFIG": str(directory / "kubeconfig"), "KUBECTL_KUBERC": "false"}
        self.protected = [key, base64.b64encode(key.encode()).decode()]

    def redact(self, text):
        for value in self.protected:
            text = text.replace(value, "[withheld]")
        return text

    def run(self, args, data=None, timeout=300, sensitive=False):
        print("Run:", " ".join(args), flush=True)
        result = subprocess.run(args, input=data, text=True, capture_output=True, cwd=ROOT,
                                env=self.env, timeout=timeout, check=False)
        if result.returncode:
            detail = "Secret provisioning details withheld" if sensitive else self.redact((result.stderr or result.stdout)[-2000:])
            raise RuntimeError(args[0] + " failed: " + detail)
        return result.stdout.strip()

    def kubectl(self, *args, **kwargs):
        return self.run(["kubectl", "--context", CONTEXT, *args], **kwargs)

    def apply(self, value):
        self.kubectl("apply", "-f", "-", data=json.dumps(value), sensitive=value.get("kind") == "Secret")

    def secret(self, name, values):
        for value in values.values():
            self.protected.extend((value, base64.b64encode(value.encode()).decode()))
        self.apply({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": NS},
                    "type": "Opaque", "stringData": values})


def manifest(local, kustomize):
    patches = [
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": DEPLOYMENT, "namespace": NS},
         "spec": {"template": {"spec": {"containers": [{"name": "manager", "imagePullPolicy": "IfNotPresent",
                  "env": [{"name": "OPENAI_AGENT_OWNER_SCOPE", "value": "local/" + CLUSTER}]}]}}}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": GATEWAY, "namespace": NS},
         "spec": {"template": {"spec": {"containers": [{"name": "gateway", "imagePullPolicy": "IfNotPresent",
                  "args": ["--gateway-database-insecure-local"]}]}}}},
    ]
    overlay = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
               "resources": ["../default"],
               "images": [
                   {"name": "controller", "newName": "codex-session-controller", "newTag": "local"},
                   {"name": "gateway", "newName": "codex-session-gateway", "newTag": "local"},
               ],
               "patches": [{"patch": json.dumps(patch)} for patch in patches]}
    with tempfile.TemporaryDirectory(prefix=".local-", dir=ROOT / "config") as temporary:
        (Path(temporary) / "kustomization.yaml").write_text(json.dumps(overlay))
        return local.run([str(kustomize), "build", temporary])


def check_gateway(local, directory, port, timeout=90):
    """Verify HTTP health and catalog, then stop the temporary tunnel."""
    log_path = directory / "port-forward.log"
    private_write(log_path, "")
    with log_path.open("a") as log:
        forward = subprocess.Popen(["kubectl", "--context", CONTEXT, "-n", NS, "port-forward",
                   "--address", "127.0.0.1", "service/" + GATEWAY, str(port) + ":8082"],
                   cwd=ROOT, env=local.env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if forward.poll() is not None:
                    raise RuntimeError("Temporary gateway tunnel exited; inspect private port-forward.log")
                try:
                    for path in ("/readyz", "/v1/namespaces/" + NS + "/catalog"):
                        request = urllib.request.Request("http://127.0.0.1:" + str(port) + path)
                        with urllib.request.urlopen(request, timeout=3) as response:
                            if response.status != 200:
                                raise RuntimeError("Gateway readiness did not return HTTP 200")
                            if path.endswith("/catalog"):
                                catalog = json.load(response)
                                if catalog.get("caller_subject") != "gateway":
                                    raise RuntimeError("Gateway catalog returned an unexpected replay subject")
                    return
                except (OSError, urllib.error.URLError):
                    time.sleep(0.5)
            raise RuntimeError("Gateway HTTP and catalog readiness timed out")
        finally:
            forward.terminate()
            try:
                forward.wait(10)
            except subprocess.TimeoutExpired:
                forward.kill()
                forward.wait(10)


def global_configs():
    paths = {Path.home() / ".kube/config"}
    paths.update(Path(value).expanduser() for value in os.environ.get("KUBECONFIG", "").split(os.pathsep) if value)
    return {path: path.read_bytes() if path.exists() else None for path in paths}


def author_resources(args, executor, architecture):
    """Author-visible declarations are separate from infrastructure installation."""
    def resource(kind, name, spec):
        return {"apiVersion": "agents.softcane.io/v1alpha1", "kind": kind,
                "metadata": {"name": name, "namespace": NS}, "spec": spec}
    if executor:
        return {
            "agent": resource("Agent", "sandbox-agent", {"name": "Sandbox investigator", "model": args.model,
                             "instructions": "Use the prepared repository and real shell evidence. Do not inspect credentials.",
                             "deletionPolicy": "Delete"}),
            "environment": resource("Environment", "sandbox-environment", {"mode": "SelfHosted", "deletionPolicy": "Delete",
                "selfHosted": {"sandboxPoolRef": {"name": "sandbox-v1"}, "workspace": {"directory": "/workspace", "deletionPolicy": "Delete"}}}),
            **{("sandbox-template" if value["kind"] == "SandboxTemplate" else "sandbox-pool"): value
               for value in sandbox_resources.resources("sandbox-v1", NS, executor, architecture,
                   args.repository_url, args.repository_commit, storage_class=args.storage_class)},
            "profile": resource("Profile", "sandbox", {"agentRef": {"name": "sandbox-agent"},
                "environmentRef": {"name": "sandbox-environment"}, "spendControl": {"mode": "Limited", "limitCents": 500}}),
        }
    return {
        "agent": resource("Agent", "investigator", {"name": "Investigator", "model": args.model,
                         "instructions": "Explain the supplied evidence clearly and state uncertainty.",
                         "deletionPolicy": "Delete"}),
        "environment": resource("Environment", "text-only", {"mode": "None", "deletionPolicy": "Delete"}),
        "profile": resource("Profile", "investigation", {"agentRef": {"name": "investigator"},
            "environmentRef": {"name": "text-only"}, "spendControl": {"mode": "Limited", "limitCents": 500}}),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("text", "sandbox"), default="text", help="Sandbox provisions an approved executor and persistent workspace policy; setup never runs inference")
    parser.add_argument("--executor-image", help="Optional pullable executor repository@sha256:digest; otherwise build and verify the imported manifest")
    parser.add_argument("--repository-url", default=REPOSITORY_URL)
    parser.add_argument("--repository-commit", default=REPOSITORY_COMMIT)
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--storage-class", default="standard")
    parser.add_argument("--port", type=int, help="Require this free loopback port; default prefers 18082 and falls back to a free port")
    parser.add_argument("--kustomize", type=Path, default=ROOT / "bin/kustomize")
    args = parser.parse_args()
    try:
        args.port = choose_port(args.port)
    except ValueError as error:
        parser.error(str(error))
    for executable in ("docker", "go", "kind", "kubectl", "make"):
        if not shutil.which(executable):
            parser.error(executable + " is required")
    if not args.kustomize.exists():
        parser.error("Run make kustomize first")
    try:
        key = application_key(os.environ, ROOT / ".env")
    except ValueError as error:
        parser.error(str(error))
    executor_key = None
    if args.mode == "sandbox":
        if args.executor_image and not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", args.executor_image):
            parser.error("Executor image must be a repository@sha256:digest")
        if not re.fullmatch(r"https://[A-Za-z0-9.-]+/[A-Za-z0-9._/-]+", args.repository_url) or not re.fullmatch(r"[a-f0-9]{40}", args.repository_commit):
            parser.error("Repository requires a credential-free HTTPS URL and immutable full commit")
        if not re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", args.storage_class):
            parser.error("Storage class must be an explicit Kubernetes resource name")
        try:
            executor_key = configuration_key(os.environ, ROOT / ".env", "OPENAI_EXECUTOR_API_KEY")
        except ValueError as error:
            parser.error(str(error))
        if executor_key == key:
            parser.error("Sandbox requires distinct application and executor credentials")
    directory = ROOT / ".private/local"
    if directory.is_symlink() or directory.parent.is_symlink():
        parser.error("Private local state must be an ordinary directory")
    if directory.exists() and any(path.is_symlink() for path in directory.iterdir()):
        parser.error("Private local state must not contain symlinked credential files")
    baseline = global_configs()
    local = Local(directory, key)
    if executor_key:
        local.protected.extend((executor_key, base64.b64encode(executor_key.encode()).decode()))
    if CLUSTER in local.run(["kind", "get", "clusters"]).splitlines():
        parser.error("codex-local already exists; refusing to replace its running work")
    directory.parent.mkdir(mode=0o700, exist_ok=True)
    directory.mkdir(mode=0o700, exist_ok=True)
    directory.parent.chmod(0o700)
    directory.chmod(0o700)
    previous_mask = os.umask(0o077)
    created, installed = False, False
    report = {"cluster": CLUSTER, "provider": "real OpenAI API", "result": "failed", "resourceInstances": None,
              "mode": args.mode, "inferenceCalls": 0, "sourceHashes": source_hashes()}
    try:
        local.run(["go", "build", "-trimpath", "-o", str(ROOT / "bin/gateway-client"), "./cmd/gateway-client"], timeout=180)
        local.run(["make", "docker-build-controller", "CONTROLLER_IMG=" + CONTROLLER_IMAGE], timeout=1200)
        local.run(["make", "docker-build-gateway", "GATEWAY_IMG=" + GATEWAY_IMAGE], timeout=1200)
        if args.mode == "sandbox":
            if args.executor_image:
                local.run(["docker", "pull", args.executor_image], timeout=1200)
            else:
                local.run(["make", "docker-build-application-executor", "EXECUTOR_IMG=" + EXECUTOR_BASE_TAG,
                           "APPLICATION_EXECUTOR_IMG=" + APPLICATION_EXECUTOR_TAG], timeout=1200)
        created = True
        local.run(["kind", "create", "cluster", "--name", CLUSTER, "--image", NODE_IMAGE,
                   "--kubeconfig", local.env["KUBECONFIG"], "--wait", "120s"])
        (directory / "kubeconfig").chmod(0o600)
        local.run(["kind", "load", "docker-image", CONTROLLER_IMAGE, GATEWAY_IMAGE, "--name", CLUSTER])
        local.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS,
                     "labels": {"pod-security.kubernetes.io/enforce": "restricted"}}})
        local.secret("openai-application", {"OPENAI_API_KEY": key})
        executor = None
        if args.mode == "sandbox":
            selected = args.executor_image or APPLICATION_EXECUTOR_TAG
            local.run(["kind", "load", "docker-image", selected, "--name", CLUSTER])
            node = CLUSTER + "-control-plane"
            listing = local.run(["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "ls"])
            executor = args.executor_image or immutable_reference(APPLICATION_EXECUTOR_TAG, listing)
            if not args.executor_image:
                local.run(["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "tag",
                           "docker.io/library/" + APPLICATION_EXECUTOR_TAG, executor])
            architecture = local.run(["docker", "image", "inspect", selected, "--format", "{{.Architecture}}"])
            if executor not in local.run(["docker", "exec", node, "ctr", "-n", "k8s.io", "images", "ls"]):
                raise RuntimeError("Executor manifest was not imported under its verified digest")
            local.secret("codex-executor", {"CODEX_API_KEY": executor_key})
            report.update({"executorImage": executor, "executorArchitecture": architecture, "repositoryURL": args.repository_url, "repositoryCommit": args.repository_commit})
        database_url = "postgres://postgres@gateway-postgres:5432/postgres?sslmode=disable"
        local.secret("gateway-database", {"url": database_url})
        local.kubectl("apply", "-f", "config/demo/postgres.yaml")
        local.kubectl("rollout", "status", "deployment/gateway-postgres", "-n", NS, "--timeout=180s")
        sandbox_resources.install(local)
        installed = True
        local.kubectl("apply", "-f", "-", data=manifest(local, args.kustomize.resolve()))
        local.kubectl("wait", "--for=condition=Established", *["crd/" + kind + ".agents.softcane.io" for kind in KINDS], "--timeout=90s")
        if executor:
            local.kubectl("set", "env", "deployment/" + DEPLOYMENT, "-n", NS, "SANDBOX_POOLS=sandbox-v1")
            local.kubectl("get", "storageclass", args.storage_class)
        for deployment in (DEPLOYMENT, GATEWAY):
            local.kubectl("rollout", "status", "deployment/" + deployment, "-n", NS, "--timeout=180s")
        resources = json.loads(local.kubectl("get", ",".join(KINDS), "-A", "-o", "json"))["items"]
        report["crdSchemaHashes"] = {kind: hashlib.sha256(local.kubectl("get", "crd", kind + ".agents.softcane.io",
                                    "-o", "jsonpath={.spec.versions}").encode()).hexdigest() for kind in KINDS}
        report["resourceInstances"] = len(resources)
        if resources:
            raise RuntimeError("Setup expected no custom-resource instances; apply author YAML explicitly after setup")
        check_gateway(local, directory, args.port)
        report["gatewayHTTPReady"] = True
        if global_configs() != baseline:
            raise RuntimeError("A global kubeconfig changed during setup")
        author = directory / "author"
        author.mkdir(mode=0o700, exist_ok=True)
        for name, resource in author_resources(args, executor, architecture).items():
            private_write(author / (name + ".yaml"), json.dumps(resource, indent=2) + "\n")
        report["authorDirectory"] = str(author)
        values = {"LOCAL_PROFILE": "sandbox" if executor else "investigation", "LOCAL_DIR": str(directory),
                  "LOCAL_CONTEXT": CONTEXT, "NAMESPACE": NS, "LOCAL_PORT": str(args.port),
                  "GATEWAY_URL": "http://127.0.0.1:" + str(args.port), "KUBECONFIG": local.env["KUBECONFIG"]}
        private_write(directory / "env.sh", "".join("export " + name + "=" + shlex.quote(value) + "\n" for name, value in values.items()))
        private_write(directory / "task.txt", "Hello\n")
        if source_hashes() != report["sourceHashes"]:
            raise RuntimeError("Sources changed during setup; do not publish mixed-source infrastructure")
        report["result"] = "passed"
    except Exception as error:
        report["error"] = local.redact(str(error))
        print("Local setup failed:", report["error"], file=sys.stderr)
        remove = not installed
        if installed:
            try:
                remove = not json.loads(local.kubectl("get", ",".join(KINDS), "-A", "-o", "json"))["items"]
            except Exception:
                report["cleanupHeld"] = "Could not confirm that no remote resource instances exist"
        if created and remove:
            try:
                local.run(["kind", "delete", "cluster", "--name", CLUSTER])
            except Exception:
                report["cleanupHeld"] = "Owned cluster cleanup failed"
    finally:
        report["globalKubeconfigUnchanged"] = global_configs() == baseline
        private_write(directory / "report.json", json.dumps(report, indent=2) + "\n")
        os.umask(previous_mask)
    if report["result"] != "passed":
        return 1
    print("Real OpenAI infrastructure ready; all six resource lists are empty; setup ran no inference.")
    print("source " + shlex.quote(str(directory / "env.sh")))
    print('kubectl --context "$LOCAL_CONTEXT" -n "$NAMESPACE" port-forward --address 127.0.0.1 service/' + GATEWAY + ' "$LOCAL_PORT:8082"')
    print('In another sourced terminal: kubectl --context "$LOCAL_CONTEXT" apply -f "$LOCAL_DIR/author"')
    print('Then: bin/gateway-client --origin "$GATEWAY_URL" --namespace "$NAMESPACE" ' +
          '--state-dir "$LOCAL_DIR/operations" start --profile "$LOCAL_PROFILE" ' +
          '--text-file "$LOCAL_DIR/task.txt" --operation first-conversation')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
