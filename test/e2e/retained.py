"""Secure access and ownership-checked cleanup for an explicitly retained live run."""

import argparse
import base64
import json
import os
from pathlib import Path
import shlex
import signal
import shutil
import ssl
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request


def private_write(path, value, mode=0o600):
    path = Path(path)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    os.fchmod(descriptor, mode)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(value)


def private_read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        value = os.fstat(stream.fileno())
        if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid() or value.st_mode & 0o077:
            raise RuntimeError("Access files must be regular owner-only files")
        return stream.read()


def save_state(path, value):
    temporary = Path(str(path) + "." + str(os.getpid()) + ".tmp")
    private_write(temporary, json.dumps(value, indent=2) + "\n")
    os.replace(temporary, path)


def request(config, suffix, method="GET", body=None, key=None):
    headers = {}
    if body is not None:
        headers["Content-Type"] = "application/json"
    if key:
        headers["Idempotency-Key"] = key
    call = urllib.request.Request(config["origin"] + suffix, method=method, headers=headers,
                                  data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(call, context=ssl.create_default_context(cafile=config["caFile"]) if config["origin"].startswith("https://") else None, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        return code, {}


def forward_command(config):
    return ["kubectl", "--kubeconfig", config["kubeconfig"], "--context", "kind-" + config["cluster"],
            "port-forward", "--address", "127.0.0.1", "-n", config["namespace"],
            "service/codex-session-operator-gateway", str(config["port"]) + ":8082"]


def process_identity(pid):
    fields = {}
    for name, option in (("executable", "comm="), ("startTime", "lstart="), ("command", "command=")):
        observed = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", option], text=True, capture_output=True,
                                  env={**os.environ, "LC_ALL": "C", "TZ": "UTC"}, check=False)
        if observed.returncode or not observed.stdout.strip():
            return None
        fields[name] = observed.stdout.strip()
    return fields


def matches_forward_command(config, identity):
    if not identity:
        return False
    executable = identity["executable"]
    expected_binary = shutil.which("kubectl")
    if not expected_binary or (executable != "kubectl" and os.path.realpath(executable) != os.path.realpath(expected_binary)):
        return False
    arguments = forward_command(config)
    allowed = {" ".join(arguments), " ".join([expected_binary, *arguments[1:]]), " ".join([executable, *arguments[1:]])}
    return identity["command"] in allowed


def owned_forward(config, previous):
    pid = previous.get("pid")
    if type(pid) is not int or pid <= 1 or previous.get("command") != forward_command(config):
        return False
    observed = process_identity(pid)
    return matches_forward_command(config, observed) and previous.get("processIdentity") == observed


def stop_forward(config):
    state = Path(config["forwardState"])
    if not state.exists():
        return
    previous = json.loads(private_read(state))
    # A dead or reused PID is not ours to signal.
    if owned_forward(config, previous):
        os.kill(previous["pid"], signal.SIGTERM)
        deadline = time.monotonic() + 5
        while owned_forward(config, previous):
            if time.monotonic() > deadline:
                raise RuntimeError("Owned port-forward has not stopped; preserve its process record")
            time.sleep(0.05)
    state.unlink()


def connect(config, timeout=45):
    code = None
    try:
        code = request(config, config["catalogPath"])[0]
    except (OSError, urllib.error.URLError):
        pass
    if code == 200:
        previous = json.loads(private_read(config["forwardState"]))
        if not owned_forward(config, previous):
            raise RuntimeError("Reachable gateway has no matching owned port-forward record")
        return previous
    if code in (401, 403):
        raise RuntimeError("Deployment access control rejected the connection; check the ingress configuration")
    stop_forward(config)
    log_path = Path(config["forwardLog"])
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as log:
        process = subprocess.Popen(forward_command(config), stdin=subprocess.DEVNULL, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
    identity = process_identity(process.pid)
    if not matches_forward_command(config, identity):
        process.terminate()
        raise RuntimeError("Detached forward executable/arguments could not be verified")
    state = {"pid": process.pid, "command": forward_command(config), "processIdentity": identity, "log": str(log_path)}
    save_state(config["forwardState"], state)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Detached port-forward exited; inspect its private log")
        try:
            if request(config, config["catalogPath"])[0] == 200:
                return state
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    process.terminate()
    raise RuntimeError("Authenticated detached gateway endpoint did not become reachable")


def prepare_access(root, directory, certificate, checks, journey, port, single):
    access = Path(root) / ".private" / "live-access" / journey.report["run"]
    access.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(access.parent, 0o700)
    if certificate:
        private_write(access / "gateway-ca.crt", Path(certificate).read_text())
    entry = next(iter(journey.attempts.values()))
    session = entry["session"]
    status = session["status"]
    config = {"version": 1, "run": journey.report["run"], "model": journey.report["model"], "scope": journey.scope,
              "cluster": checks.cluster, "namespace": session["metadata"]["namespace"],
              "origin": ("https" if certificate else "http") + "://127.0.0.1:" + str(port), "port": port,
              "kubeconfig": str(Path(checks.env["KUBECONFIG"]).resolve()),
              "caFile": str(access / "gateway-ca.crt"), "forwardState": str(access / "forward.json"), "forwardLog": str(access / "port-forward.log"),
              "conversationID": next(iter(journey.attempts)), "conversationPath": entry["path"],
              "session": {"name": entry["name"], "uid": entry["uid"], "remoteID": entry["remoteID"]},
              "resources": list(journey.resources.values()), "reportDirectory": str(directory),
              "execution": {"pod": status["podName"], "sandbox": status["sandboxName"], "sandboxclaim": status["sandboxClaimName"],
                            "pvc": single["claim"]["metadata"]["name"]}}
    config["catalogPath"] = "/v1/namespaces/" + config["namespace"] + "/catalog"
    config["conversationURL"] = config["origin"] + config["conversationPath"]
    config["messagesURL"] = config["conversationURL"] + "/messages"
    if certificate:
        expiry = subprocess.run(["openssl", "x509", "-in", str(certificate), "-noout", "-enddate"], text=True, capture_output=True, check=True)
        config["certificateExpires"] = expiry.stdout.strip().removeprefix("notAfter=")
    else:
        del config["caFile"]
    configuration = access / "connection.json"
    private_write(configuration, json.dumps(config, indent=2) + "\n")
    prefix = "exec " + " ".join(shlex.quote(str(value)) for value in (Path(root) / "bin/gateway-client", "--origin", config["origin"],
        "--namespace", config["namespace"], "--state-dir", access / "operations"))
    if certificate:
        prefix += " --ca-file " + shlex.quote(config["caFile"])
    private_write(access / "gateway-client", "#!/bin/sh\n" + prefix + ' "$@"\n', 0o700)
    for name, command in (("send-message", "continue"), ("read-conversation", "read"), ("watch-conversation", "watch")):
        private_write(access / name, "#!/bin/sh\nexec " + shlex.quote(str(access / "gateway-client")) + " " + command + " " + shlex.quote(config["conversationID"]) + ' "$@"\n', 0o700)
    private_write(access / "manage", "#!/bin/sh\nexec " + " ".join(shlex.quote(str(value)) for value in
        (sys.executable, Path(root) / "test/e2e/retained.py", "--config", configuration)) + ' "$@"\n', 0o700)
    return configuration, config


def kubectl(config, *arguments):
    call = subprocess.run(["kubectl", "--kubeconfig", config["kubeconfig"], "--context", "kind-" + config["cluster"], *arguments],
                          text=True, capture_output=True, check=False)
    if call.returncode:
        raise RuntimeError("Kubernetes observation/mutation failed; resources retained for audit")
    return call.stdout.strip()


def local_resource(config, kind, name):
    raw = kubectl(config, "get", kind, name, "-n", config["namespace"], "-o", "json", "--ignore-not-found")
    return json.loads(raw) if raw else None


def provider_get(key, kind, identifier):
    suffix = "sessions/" if kind == "Session" else ""
    call = urllib.request.Request("https://api.openai.com/v1/agents/" + suffix + identifier,
                                  headers={"Authorization": "Bearer " + key, "OpenAI-Beta": "agents=v1"})
    try:
        with urllib.request.urlopen(call, timeout=20) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        code = error.code
        error.close()
        if code == 404:
            return None
        raise RuntimeError("Provider observation unavailable; resources retained for audit") from None


def verify_owner(remote, uid, scope, identifier):
    if remote is None:
        return
    metadata = remote.get("metadata", {})
    if remote.get("id") != identifier or metadata.get("kubernetes_uid") != uid or metadata.get("owner_scope") != scope or metadata.get("operator") != "codex-session-operator":
        raise RuntimeError("Remote ownership differs; cleanup refused")


def wait_for(label, observe, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if observe():
            return
        time.sleep(1)
    raise RuntimeError(label + " not confirmed; resources retained for audit")


def verify_conversations(config):
    base = "/v1/namespaces/" + config["namespace"] + "/conversations"
    code, collection = request(config, base)
    if code != 200 or collection.get("more") is not False or not isinstance(collection.get("conversations"), list):
        raise RuntimeError("Conversation discovery incomplete; cluster retained")
    if any(item.get("conversation_id") != config["conversationID"] for item in collection.get("conversations", [])):
        raise RuntimeError("Additional conversations exist; cluster retained for their independent cleanup")


def verify_inventory(config, *, empty=False):
    kinds = "agents,sessions,environments,vaults,vaultcredentials,profiles"
    inventory = json.loads(kubectl(config, "get", kinds, "--all-namespaces", "-o", "json"))
    if not isinstance(inventory.get("items"), list):
        raise RuntimeError("API resource discovery incomplete; cluster retained")
    allowed = {(entry["kind"].lower(), config["namespace"], entry["name"]): entry["uid"] for entry in config["resources"]}
    allowed[("session", config["namespace"], config["session"]["name"])] = config["session"]["uid"]
    for item in inventory["items"]:
        metadata = item.get("metadata", {})
        identity = (item.get("kind", "").lower(), metadata.get("namespace"), metadata.get("name"))
        if empty or not metadata.get("uid") or allowed.get(identity) != metadata["uid"]:
            raise RuntimeError("Additional API resources or changed UIDs remain; cluster retained before dependency deletion")


def cleanup(config):
    """Only explicit invocation closes the conversation and tears down owned state."""
    connect(config)
    verify_conversations(config)
    verify_inventory(config)
    secret = local_resource(config, "secret", "openai-application")
    if not secret:
        raise RuntimeError("Application credential unavailable; retain the remote-obligation ledger")
    key = base64.b64decode(secret["data"]["OPENAI_API_KEY"]).decode()
    session = local_resource(config, "session", config["session"]["name"])
    if session and session["metadata"]["uid"] != config["session"]["uid"]:
        raise RuntimeError("Session UID changed; cleanup refused")
    verify_owner(provider_get(key, "Session", config["session"]["remoteID"]), config["session"]["uid"], config["scope"], config["session"]["remoteID"])
    code, _ = request(config, config["conversationPath"] + "/close", "POST", {}, config["run"] + "-retained-close")
    if code != 202:
        raise RuntimeError("Gateway close did not durably commit; resources retained")
    wait_for("Session finalization", lambda: local_resource(config, "session", config["session"]["name"]) is None)
    wait_for("Remote Session absence", lambda: provider_get(key, "Session", config["session"]["remoteID"]) is None)
    for kind, name in config["execution"].items():
        wait_for("Execution resource absence", lambda kind=kind, name=name: local_resource(config, kind, name) is None)
    for entry in reversed(config["resources"]):
        current = local_resource(config, entry["kind"], entry["name"])
        if current:
            if current["metadata"]["uid"] != entry["uid"]:
                raise RuntimeError("Dependency UID changed; cleanup refused")
            if entry["kind"] == "Agent":
                verify_owner(provider_get(key, "Agent", entry["remoteID"]), entry["uid"], config["scope"], entry["remoteID"])
            kubectl(config, "delete", entry["kind"], entry["name"], "-n", config["namespace"], "--wait=false")
            wait_for("Dependency finalization", lambda entry=entry: local_resource(config, entry["kind"], entry["name"]) is None)
        if entry["kind"] == "Agent" and provider_get(key, "Agent", entry["remoteID"]) is not None:
            raise RuntimeError("Remote Agent absence unconfirmed; cluster retained")
    # New resources created after retention are outside this ledger. Never destroy
    # their controllers or credentials merely because the original Session is gone.
    verify_inventory(config, empty=True)
    verify_conversations(config)
    call = subprocess.run(["kind", "delete", "cluster", "--name", config["cluster"]], text=True, capture_output=True, check=False)
    if call.returncode:
        raise RuntimeError("Owned cluster deletion unconfirmed")
    stop_forward(config)
    save_state(Path(config["forwardState"]).parent / "cleanup.json", {"remoteAbsenceConfirmed": True, "clusterRemoved": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("command", choices=("status", "reconnect", "cleanup"))
    args = parser.parse_args()
    try:
        config = json.loads(private_read(args.config))
        if type(config.get("port")) is not int or not 1 <= config["port"] <= 65535 or config.get("cluster") != "codex-sandbox-live-" + config["run"] or config.get("origin") not in {scheme + "://127.0.0.1:" + str(config["port"]) for scheme in ("http", "https")}:
            raise RuntimeError("Retained access cluster/origin identity differs")
        if args.command == "cleanup":
            cleanup(config)
            print("Owned remote resources and cluster cleanup confirmed.")
        elif args.command == "reconnect":
            print(json.dumps(connect(config)))
        else:
            code, value = request(config, config["conversationPath"])
            if code != 200:
                raise RuntimeError("Gateway observation failed: HTTP " + str(code))
            print(json.dumps({"origin": config["origin"], "conversationURL": config["conversationURL"], "summary": value.get("summary"), "lifecycle": value.get("lifecycle")}))
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(type(error).__name__ + ": " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
