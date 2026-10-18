#!/usr/bin/env python3
"""Opt-in real OpenAI/production exec-server acceptance. Billable; limited by default.

Required credentials are read literally from the process/private .env, never argv.
Missing configuration fails preflight. Unknown remote outcomes preserve the owned
cluster, controllers, kubeconfig and private recovery ledger for audit.
--without-session-budget explicitly removes the default aggregate 500-cent cap.
"""

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import socket
import ssl
import tempfile
import time
import urllib.error
import urllib.request
import uuid

import gateway_tls
from reporting import report_path, public_report
import sandbox
import support

ROOT, NS, ensure = support.ROOT, support.NS, support.ensure
REPOSITORY_URL = "https://github.com/google/uuid.git"
REPOSITORY_COMMIT = "0f11ee6918f41a04c201eceeadf612a377bc7fbc"
spec = importlib.util.spec_from_file_location("sandbox_local_setup", ROOT / "hack/local-up.py")
local_setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_setup)


def verify_owner(value, uid, scope, identifier):
    ensure(value and value.get("id") == identifier and value.get("metadata", {}).get("operator") == "codex-session-operator"
           and value["metadata"].get("kubernetes_uid") == uid and value["metadata"].get("owner_scope") == scope,
           "Remote ownership differs; mutation and teardown refused")


def verify_spending(remote, expected_limit):
    spending = remote.get("spend_control")
    if expected_limit is None:
        ensure(spending is None, "Explicit unbudgeted Session returned an unexpected spending policy")
        return False
    ensure(isinstance(spending, dict) and spending.get("limit") == expected_limit and isinstance(spending.get("consumed"), int)
           and 0 <= spending["consumed"] < expected_limit,
           "Provider did not positively acknowledge/enforce the allocated limited spending cap")
    return True


class Provider:
    def __init__(self, key):
        self.key = key

    def get(self, kind, identifier):
        request = urllib.request.Request("https://api.openai.com/v1/agents/" + ("sessions/" if kind == "session" else "") + identifier,
            headers={"Authorization": "Bearer " + self.key, "OpenAI-Beta": "agents=v1"})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
            if code == 404:
                return None
            raise RuntimeError("Provider ownership/cleanup observation unavailable: HTTP " + str(code)) from None
        except urllib.error.URLError:
            raise RuntimeError("Provider ownership/cleanup observation unavailable") from None


class Journey:
    def __init__(self, checks, api, provider, evidence, report, image, architecture, service_restart, *, without_session_budget=False):
        self.checks, self.api, self.provider, self.evidence, self.report = checks, api, provider, evidence, report
        self.image, self.architecture = image, architecture
        self.base = "/v1/namespaces/" + NS
        self.scope = report["scope"]
        self.resources, self.attempts = {}, {}
        self.journal = []
        self.service_restart = service_restart
        self.budgets = {}
        self.without_session_budget = without_session_budget
        self.verified_limits = set()

    def spending_policy(self, limit):
        if self.without_session_budget:
            return {"mode": "unlimited"}
        ensure(sum(self.budgets.values()) + limit <= 500, "Aggregate acceptance budget exhausted; further creation refused")
        return {"mode": "limited", "limitCents": limit}

    def stage(self, name):
        self.report["stage"] = name
        print("Stage:", name, flush=True)

    def publish(self, kind, name, spec):
        # Preserve the obligation before an API acknowledgement can be lost.
        self.resources.setdefault((kind, name), {"kind": kind, "name": name, "uid": None, "remoteID": None})
        self.checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": kind,
                           "metadata": {"name": name, "namespace": NS, "labels": {"agents.softcane.io/acceptance-run": self.report["run"]}}, "spec": spec})
        value = self.checks.get(kind.lower(), name)
        ensure(value, "Published resource cannot be observed; retain cluster")
        self.resources[(kind, name)]["uid"] = value["metadata"]["uid"]
        return self.checks.wait(kind + "/" + name + " current Ready", lambda: value if
                    support.current_ready(value := self.checks.get(kind.lower(), name)) else None, 240)

    def operation(self, path, body, label):
        key = self.report["run"] + "-" + label
        entry = {"path": path, "body": body, "key": key}
        self.journal.append(entry)
        self.evidence.write("private-operations.json", self.journal)
        # Retries preserve the exact immutable operation, including a lost response.
        for attempt in range(3):
            try:
                status, receipt = self.api.request(path, "POST", body, key)
                ensure(status == 202 and receipt.get("acceptance") == "committed", "Operation did not durably commit: HTTP " + str(status))
                return receipt
            except (OSError, urllib.error.URLError):
                if attempt == 2:
                    raise RuntimeError("Acceptance outcome unknown; original operation saved for audit") from None
                time.sleep(0.5)

    def detail(self, path):
        status, value = self.api.request(path)
        ensure(status == 200, "Saved conversation unavailable: HTTP " + str(status))
        return value

    def completed(self, path, prior_ids=()):
        def observe():
            value = self.detail(path)
            turns = [turn for turn in (value.get("history") or {}).get("turns") or [] if not turn.get("subagent_id") and turn["id"] not in prior_ids]
            ensure(not any(turn.get("status") in ("failed", "cancelled") for turn in turns), "Required new root turn failed or was cancelled")
            return value if turns and all(turn.get("status") == "completed" for turn in turns) else None
        return self.checks.wait("new saved completed root turn", observe, 300)

    def remember(self, detail):
        summary = detail["summary"]
        session = self.checks.get("session", summary["session_name"])
        ensure(session and session["metadata"]["uid"] == summary["session_uid"], "Conversation/Session UID differs")
        identifier = session.get("status", {}).get("openaiSessionID")
        entry = self.attempts.setdefault(summary["conversation_id"], {"path": self.base + "/conversations/" + summary["conversation_id"],
            "name": session["metadata"]["name"], "uid": session["metadata"]["uid"], "remoteID": identifier})
        ensure(not entry["remoteID"] or entry["remoteID"] == identifier, "Remote Session identity changed")
        entry["remoteID"] = identifier
        entry["session"] = session
        self.evidence.write("private-resource-ledger.json", {"scope": self.scope, "attempts": self.attempts,
            "dependencies": list(self.resources.values())})
        return session

    def ready(self, path):
        def observe():
            value = self.detail(path)
            if not value["summary"].get("session_uid"):
                return None
            session = self.remember(value)
            if not self.without_session_budget:
                for condition in session.get("status", {}).get("conditions", []):
                    if condition.get("reason") in ("SpendControlUnavailable", "SpendingLimitUnavailable", "SessionSpendControlUnavailable"):
                        raise RuntimeError("Provider cannot enforce required limited spending; no unlimited fallback")
            return value
        value, session, pod = support.wait_executor(self.checks, observe, "production executor current readiness", 300)
        self.remember(value)
        remote = self.provider.get("session", session["status"]["openaiSessionID"])
        verify_owner(remote, session["metadata"]["uid"], self.scope, session["status"]["openaiSessionID"])
        expected_limit = None if self.without_session_budget else self.budgets[path]
        if verify_spending(remote, expected_limit):
            self.verified_limits.add(path)
            self.report["spendingLimitQualified"] = len(self.verified_limits) == 4 and sum(self.budgets.values()) == 500
        claim = self.checks.get("pvc", "workspace-" + session["metadata"]["uid"])
        ensure(pod and pod["spec"]["containers"][0]["image"] == self.image and
               any(owner.get("controller") and owner["uid"] == session["metadata"]["uid"] for owner in pod["metadata"].get("ownerReferences", []))
               and claim and claim["metadata"]["uid"] == session["status"]["workspaceClaimUID"], "Production executor/PVC ownership differs")
        self.evidence.write(session["metadata"]["name"] + "-runtime.json", {"session": session, "pod": pod, "pvc": claim})
        return value, session, pod, claim

    def exec(self, pod, *args, **kwargs):
        return self.checks.kubectl("exec", "-n", NS, pod, "--", *args, **kwargs)

    def read_exec(self, pod, *args, **kwargs):
        return support.read_only_exec(self.checks, pod, *args, **kwargs)

    def project_manifest(self, pod):
        # Git status omits ignored files. Scan the project independently without
        # following links; only the exact runtime caches and harness backup are exempt.
        program = r"""const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=process.cwd(),result={},excluded=new Set(['.git','.cache/go-build','.cache/go-mod','.cache/go','.sandbox-original-uuid.go']);
if(fs.realpathSync(root)!==root)throw Error('Project root is not canonical');
function scan(relative){for(const name of fs.readdirSync(path.join(root,relative)).sort()){
const entry=relative?relative+'/'+name:name;if(excluded.has(entry))continue;
const full=path.join(root,entry),state=fs.lstatSync(full);
if(state.isSymbolicLink()||fs.realpathSync(full)!==full)throw Error('Project manifest refuses symlinks');
if(state.isDirectory()){scan(entry);continue;}if(!state.isFile())throw Error('Project manifest requires regular files');
const fd=fs.openSync(full,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
try{const current=fs.fstatSync(fd);if(!current.isFile()||current.dev!==state.dev||current.ino!==state.ino)throw Error('Project changed during manifest');
result[entry]=crypto.createHash('sha256').update(fs.readFileSync(fd)).digest('hex');}finally{fs.closeSync(fd);}}}
scan('');process.stdout.write(JSON.stringify(result));"""
        return json.loads(self.exec(pod, "node", "-e", program))

    def verify_project(self, pod, baseline):
        ensure(self.read_exec(pod, "git", "rev-parse", "HEAD") == REPOSITORY_COMMIT,
               "Independent project HEAD differs from the pinned source")
        ensure(self.exec(pod, "git", "diff", "HEAD", "--") == "",
               "Independent tracked project changed beyond removing the injected regression")
        current = self.project_manifest(pod)
        allowed_outputs = {"proof.txt", "sandbox-findings.json"}
        actual = {name: digest for name, digest in current.items() if name in baseline or name not in allowed_outputs}
        ensure(actual == baseline, "Independent project filesystem changed beyond the explicit acceptance outputs")

    def prove_commands(self, detail, before, required):
        prior = {item["id"] for item in (before.get("history") or {}).get("items") or []}
        commands = [item for item in (detail.get("history") or {}).get("items") or []
                    if item.get("id") not in prior and item.get("type") == "command_execution"]
        ensure(commands and all(item.get("cwd", "").startswith("/workspace") for item in commands), "New command items did not execute in the workspace")
        text = "\n".join((item.get("command") or "") + "\n" + (item.get("output") or "") for item in commands)
        ensure(all(value in text for value in required), "Actual saved command/nonce/project evidence is incomplete")
        ensure(any(item.get("status") == "completed" and item.get("exit_code") == 0 and
                   "go test" in (item.get("command") or "") and
                   re.search(r"(?:^|\n)ok\s+github\.com/google/uuid\b", item.get("output") or "") for item in commands),
               "No successful actual project test command/output was saved")
        return [item["id"] for item in commands]

    def command_history(self, path, before, required):
        # The provider's items scan precedes its turns scan. A completed root can
        # therefore be saved before its final command output reaches this read.
        prior_ids = {turn["id"] for turn in (before.get("history") or {}).get("turns") or []}
        def observe():
            value = self.detail(path)
            turns = [turn for turn in (value.get("history") or {}).get("turns") or []
                     if not turn.get("subagent_id") and turn["id"] not in prior_ids]
            ensure(not any(turn.get("status") in ("failed", "cancelled") for turn in turns),
                   "Required new root turn failed or was cancelled")
            if not turns or not all(turn.get("status") == "completed" for turn in turns):
                return None
            try:
                command_ids = self.prove_commands(value, before, required)
            except RuntimeError:
                return None
            return value, command_ids
        return self.checks.wait("new saved completed root and successful command evidence", observe, 300)

    def close(self, path, label):
        self.operation(path + "/close", {}, label)
        value = self.checks.wait("closed and cleaned", lambda: value if support.cleaned_up(value := self.detail(path)) else None, 240)
        entry = self.attempts[value["summary"]["conversation_id"]]
        ensure(self.checks.get("session", entry["name"]) is None and
               self.checks.get("pod", entry["session"]["status"]["podName"]) is None and
               self.provider.get("session", entry["remoteID"]) is None, "Local/provider cleanup was not independently confirmed")
        return value

    def exercise(self, policy):
        name = "live-" + policy.lower()
        self.stage(name + " authoring")
        self.publish("Environment", name, {"mode": "selfHosted", "deletionPolicy": "Delete", "selfHosted": {
            "image": self.image, "architecture": self.architecture, "workspaceDirectory": "/workspace", "workspaceSize": "1Gi",
            "resources": {"cpuRequest": "250m", "cpuLimit": "2", "memoryRequest": "256Mi", "memoryLimit": "2Gi"},
            "workspaceStorage": {"type": "Persistent", "storageClassName": "standard", "claimDeletionPolicy": policy},
            "repository": {"url": REPOSITORY_URL, "commit": REPOSITORY_COMMIT}}})
        self.publish("Profile", name, {"agentRef": {"name": "live-agent"}, "environmentRef": {"name": name},
            "spendControl": self.spending_policy(200)})
        receipt = self.operation(self.base + "/conversations", {"route_id": name,
            "text": "Acknowledge SANDBOX_CREATED without tools. Wait for the next task."}, name + "-create")
        path = receipt["conversation_url"]
        if not self.without_session_budget:
            self.budgets[path] = 200
            ensure(sum(self.budgets.values()) <= 500, "Aggregate acceptance budget exceeded; further creation refused")
        initial, session, pod, claim = self.ready(path)
        initial = self.completed(path)
        pod_name = pod["metadata"]["name"]
        ensure(self.read_exec(pod_name, "git", "rev-parse", "HEAD") == REPOSITORY_COMMIT, "Real repository initial commit differs")
        baseline = self.exec(pod_name, "go", "test", "./...", timeout=180)
        self.evidence.write(name + "-baseline.log", baseline)
        source_baseline = self.project_manifest(pod_name)
        self.evidence.write(name + "-source-baseline.json", source_baseline)
        nonce = uuid.uuid4().hex
        seed = "const fs=require('fs');fs.writeFileSync('proof.txt',JSON.parse(fs.readFileSync(0,'utf8')).nonce);"
        self.checks.kubectl("exec", "-i", "-n", NS, pod_name, "--", "node", "-e", seed, data=json.dumps({"nonce": nonce}))
        if policy == "Delete":
            self.stage("actual project controlled regression and repair")
            inject = """const fs=require('fs');const file='uuid.go',source=fs.readFileSync(file,'utf8');
const anchor='func Parse(s string) (UUID, error) {\\n';if(!source.includes(anchor))process.exit(2);
fs.writeFileSync('.sandbox-original-uuid.go',source);fs.writeFileSync(file,source.replace(anchor,
anchor+'\\treturn Nil, fmt.Errorf("sandbox controlled Parse regression")\\n'));"""
            self.exec(pod_name, "node", "-e", inject)
            failure = self.exec(pod_name, "go", "test", "./...", allowed=(1,), timeout=180)
            ensure("FAIL" in failure, "Independent controlled project regression did not fail")
            self.evidence.write("independent-regression.log", failure)
            task = ("Investigate the failing tests in the prepared google/uuid project. Run go test ./... and diagnose the observed Parse regression. "
                    "Repair only the injected regression in uuid.go, rerun the project's actual tests, and read proof.txt. "
                    "Write sandbox-findings.json with root_cause, file, proof_nonce, and test_command. Do not read credentials or change tests.")
        else:
            task = ("Run git rev-parse HEAD and go test ./... in the prepared repository, then read proof.txt. "
                    "Write sandbox-findings.json with root_cause: baseline passes, file: uuid.go, proof_nonce and test_command. Do not change source or tests or inspect credentials.")
        before = self.detail(path)
        self.operation(path + "/messages", {"text": task}, name + "-project")
        result, command_ids = self.command_history(path, before, ["go test", nonce])
        findings = json.loads(self.read_exec(pod_name, "cat", "/workspace/sandbox-findings.json"))
        ensure(findings.get("proof_nonce") == nonce and Path(findings.get("file", "")).name == "uuid.go"
               and "go test" in findings.get("test_command", ""), "Independent output report differs from project/nonce oracle")
        self.exec(pod_name, "go", "test", "./...", timeout=180)
        self.verify_project(pod_name, source_baseline)
        self.evidence.write(name + "-project-history.json", result)
        self.evidence.write(name + "-findings.json", findings)
        self.stage(name + " idle executor replacement")
        self.checks.wait("provider idle before replacement", lambda: (value := self.provider.get("session", session["status"]["openaiSessionID"])) and value.get("status") == "idle", 90)
        self.exec(pod_name, "sh", "-c", "printf continuity > session-note.txt && git add session-note.txt && git -c user.name=Acceptance -c user.email=acceptance@example.invalid commit -qm 'Session continuation' && printf dirty > dirty-note.txt && rm README.md")
        later_commit = self.read_exec(pod_name, "git", "rev-parse", "HEAD")
        self.checks.kubectl("delete", "pod", pod_name, "-n", NS, "--wait=true", "--timeout=120s")
        replacement = self.checks.wait("new production executor Pod UID", lambda: value if
            (value := self.checks.get("pod", pod_name)) and value["metadata"]["uid"] != pod["metadata"]["uid"] else None, 180)
        _, current, replacement, current_claim = self.ready(path)
        ensure(current["metadata"]["uid"] == session["metadata"]["uid"] and current["status"]["openaiSessionID"] == session["status"]["openaiSessionID"]
               and current_claim["metadata"]["uid"] == claim["metadata"]["uid"] and self.read_exec(pod_name, "cat", "/workspace/proof.txt") == nonce
               and self.read_exec(pod_name, "git", "rev-parse", "HEAD") == later_commit and self.read_exec(pod_name, "cat", "/workspace/dirty-note.txt") == "dirty",
               "Replacement lost identity, nonce, later commit or dirty files")
        self.read_exec(pod_name, "test", "!", "-e", "/workspace/README.md")
        before = self.detail(path)
        self.operation(path + "/messages", {"text": "Run go test ./..., hostname and cat proof.txt. Report their actual outputs; do not change files or inspect credentials."}, name + "-continue")
        continued, _ = self.command_history(path, before, ["go test", nonce, pod_name])
        self.evidence.write(name + "-continuation-history.json", continued)
        if policy == "Delete":
            self.stage("actual gateway/controller process replacement")
            self.service_restart()
            self.ready(path)
            ensure(self.read_exec(pod_name, "cat", "/workspace/proof.txt") == nonce and
                   self.read_exec(pod_name, "git", "rev-parse", "HEAD") == later_commit,
                   "Service replacement lost persistent files or commits")
            before = self.detail(path)
            self.operation(path + "/messages", {"text": "Run go test ./... and cat proof.txt after service replacement. Report actual output; do not change files."}, name + "-services")
            recovered, _ = self.command_history(path, before, ["go test", nonce])
            self.evidence.write("service-replacement-history.json", recovered)
        self.report["scenarios"].append({"id": name + "_real_project_commands_files_idle_replacement", "status": "passed",
            "commandIDs": command_ids, "oldPodUID": pod["metadata"]["uid"], "newPodUID": replacement["metadata"]["uid"], "pvcUID": claim["metadata"]["uid"]})
        self.stage(name + " close and workspace policy")
        retained_history = self.close(path, name + "-close")
        if policy == "Delete":
            ensure(self.checks.get("pvc", claim["metadata"]["name"]) is None, "Delete policy left a workspace PVC")
        else:
            old_claim = self.checks.get("pvc", claim["metadata"]["name"])
            ensure(old_claim and old_claim["metadata"]["uid"] == claim["metadata"]["uid"] and not old_claim["metadata"].get("ownerReferences"), "Retain claim ownership changed")
            reader = sandbox.read_only_probe(self.checks, self.image, claim["metadata"]["name"])
            ensure(self.read_exec(reader, "cat", "/workspace/proof.txt", container="reader") == nonce, "Retained file did not survive cleanup")
            self.checks.kubectl("delete", "pod", reader, "-n", NS, "--wait=true", "--timeout=120s")
        self.publish("Profile", name, {"agentRef": {"name": "live-agent"}, "environmentRef": {"name": name},
            "spendControl": self.spending_policy(50)})
        fresh_receipt = self.operation(path + "/restart", {"text": "Run git rev-parse HEAD and go test ./... in the fresh prepared repository. Report actual outputs and do not change files or inspect credentials."}, name + "-fresh")
        fresh_path = fresh_receipt["conversation_url"]
        if not self.without_session_budget:
            self.budgets[fresh_path] = 50
        _, fresh_session, fresh_pod, fresh_claim = self.ready(fresh_path)
        fresh, _ = self.command_history(fresh_path, {"history": {"items": []}}, ["go test", REPOSITORY_COMMIT])
        ensure(fresh_session["metadata"]["uid"] != session["metadata"]["uid"] and fresh_session["status"]["openaiSessionID"] != session["status"]["openaiSessionID"]
               and fresh_claim["metadata"]["uid"] != claim["metadata"]["uid"], "Start fresh adopted prior identities or workspace")
        self.read_exec(fresh_pod["metadata"]["name"], "test", "!", "-e", "/workspace/dirty-note.txt")
        ensure(self.read_exec(fresh_pod["metadata"]["name"], "git", "rev-parse", "HEAD") == REPOSITORY_COMMIT, "Fresh checkout is not pinned")
        ensure(self.detail(path)["history"]["items"] == retained_history["history"]["items"], "Fresh attempt changed retained old history")
        self.close(fresh_path, name + "-fresh-close")
        if policy == "Retain":
            for owned in (claim, fresh_claim):
                current = self.checks.get("pvc", owned["metadata"]["name"])
                ensure(current and current["metadata"]["uid"] == owned["metadata"]["uid"], "Retained claim UID changed before harness cleanup")
                self.checks.kubectl("delete", "pvc", current["metadata"]["name"], "-n", NS, "--wait=true", "--timeout=120s")
        self.report["scenarios"].append({"id": name + "_close_policy_start_fresh_independence", "status": "passed"})

    def cleanup(self):
        """Known exact-owned CRs are finalized; uncertainty preserves the installation."""
        # A lost create receipt can still leave a durable source and Session.
        status, collection = self.api.request(self.base + "/conversations")
        ensure(status == 200 and not collection.get("more"), "Cleanup discovery unavailable or incomplete; installation retained")
        for summary in collection.get("conversations", []):
            if summary["conversation_id"] in self.attempts:
                continue
            value = self.detail(self.base + "/conversations/" + summary["conversation_id"])
            ensure(value["summary"].get("session_uid") and
                   self.checks.get("session", value["summary"]["session_name"]),
                   "Accepted source has no auditable Session outcome; installation retained")
            self.remember(value)
        for entry in self.attempts.values():
            current = self.checks.get("session", entry["name"])
            if current:
                ensure(current["metadata"]["uid"] == entry["uid"], "Cleanup found a replaced Session UID")
                identifier = current.get("status", {}).get("openaiSessionID")
                ensure(identifier or not current.get("status", {}).get("creationAttempted"), "Unresolved remote create claim; cluster retained")
                if identifier:
                    remote = self.provider.get("session", identifier)
                    if remote:
                        verify_owner(remote, entry["uid"], self.scope, identifier)
                self.operation(entry["path"] + "/close", {}, "cleanup-" + entry["uid"])
                self.checks.wait("cleanup Session finalization", lambda: self.checks.get("session", entry["name"]) is None, 240)
                entry["remoteID"] = identifier
            ensure(entry["remoteID"] and self.provider.get("session", entry["remoteID"]) is None, "Remote cleanup outcome unresolved; cluster retained")
            session = entry["session"]
            if session.get("status", {}).get("workspaceClaimUID"):
                claim = self.checks.get("pvc", "workspace-" + entry["uid"])
                if claim:
                    policy = session["status"]["environmentSnapshot"]["profile"]["selfHosted"]["workspaceStorage"]["claimDeletionPolicy"]
                    ensure(policy == "Retain" and claim["metadata"]["uid"] == session["status"]["workspaceClaimUID"]
                           and claim["metadata"].get("labels", {}).get("agents.softcane.io/session-uid") == entry["uid"]
                           and not claim["metadata"].get("ownerReferences"), "Workspace cleanup ownership or policy differs")
                    self.checks.kubectl("delete", "pvc", claim["metadata"]["name"], "-n", NS, "--wait=true", "--timeout=120s")
        for (_, name), entry in reversed(list(self.resources.items())):
            current = self.checks.get(entry["kind"].lower(), name)
            if not current:
                ensure(entry["kind"] != "Agent" or entry["remoteID"], "Agent disappeared without known remote outcome")
                continue
            ensure(current["metadata"]["uid"] == entry["uid"], "Cleanup found a replaced dependency UID")
            if entry["kind"] == "Agent":
                identifier = current.get("status", {}).get("openaiAgentID")
                ensure(identifier or not current.get("status", {}).get("creationAttempted"), "Unresolved Agent create claim; cluster retained")
                entry["remoteID"] = identifier
                if identifier and (remote := self.provider.get("agent", identifier)):
                    verify_owner(remote, entry["uid"], self.scope, identifier)
            self.checks.kubectl("delete", entry["kind"].lower(), name, "-n", NS, "--wait=false")
            self.checks.wait("cleanup dependency finalization", lambda: self.checks.get(entry["kind"].lower(), name) is None, 180)
            if entry["kind"] == "Agent" and entry["remoteID"]:
                ensure(self.provider.get("agent", entry["remoteID"]) is None, "Agent cleanup outcome unresolved")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="gpt-6-luna")
    spending = parser.add_mutually_exclusive_group()
    spending.add_argument("--spend-limit-cents", type=int, default=500)
    spending.add_argument("--without-session-budget", action="store_true", help="Explicitly omit Session budgets; this run has no monetary cap")
    args = parser.parse_args()
    ensure(args.spend_limit_cents == 500, "This acceptance requires limited 500 cents; unlimited fallback is unavailable")
    run = uuid.uuid4().hex[:12]
    cluster = "codex-sandbox-live-" + run
    directory = report_path(cluster, default_root=ROOT / ".private/reports")
    directory.mkdir(mode=0o700)
    report = {"run": run, "cluster": cluster, "scope": "sandbox-live/" + cluster, "result": "failed", "stage": "preflight-incomplete",
              "layer": "real OpenAI / production exec-server / actual Kind Pod/PVC / real PostgreSQL / pinned google/uuid",
              "spendingMode": "unlimited" if args.without_session_budget else "limited",
              "aggregateLimitCents": None if args.without_session_budget else 500,
              "allocationCents": None if args.without_session_budget else {"delete": 200, "deleteFresh": 50, "retain": 200, "retainFresh": 50},
              "spendingLimitQualified": False,
              "scenarios": [], "checks": {}, "sourceHashes": support.source_hashes(),
              "notTested": ["production HA", "node/volume loss", "hosted runtime", "transparent mid-command replay"]}
    evidence = support.Evidence(directory, [])
    created = False
    checks = forward = journey = None
    baseline = local_setup.global_configs()
    existing = None
    result = 1
    try:
        for executable in ("docker", "kind", "kubectl", "openssl", "make"):
            ensure(shutil.which(executable), executable + " is required")
        ensure((ROOT / "bin/kustomize").exists(), "Pinned kustomize must be prepared before live acceptance")
        keys = {name: local_setup.configuration_key(os.environ, ROOT / ".env", name)
                for name in ("OPENAI_API_KEY", "OPENAI_EXECUTOR_API_KEY")}
        ensure(keys["OPENAI_API_KEY"] != keys["OPENAI_EXECUTOR_API_KEY"], "Separate application/executor credentials are required")
        token = secrets.token_hex(32)
        keys["gateway-token"] = token
        evidence.protect(keys.values())
        checks = support.Checks(directory / "kubeconfig", cluster, keys)
        existing = checks.run(["kind", "get", "clusters"]).splitlines()
        ensure(cluster not in existing, "Generated owned cluster already exists; refusing reuse")
        controller_tag = "codex-session-controller:live-" + run
        gateway_tag = "codex-session-gateway:live-" + run
        executor_base_tag = "codex-executor:live-" + run
        executor_tag = "my-go-project-executor:live-" + run
        report["stage"] = "production image build"
        checks.run(["make", "docker-build-controller", "CONTROLLER_IMG=" + controller_tag], timeout=1200)
        checks.run(["make", "docker-build-gateway", "GATEWAY_IMG=" + gateway_tag], timeout=1200)
        checks.run(["make", "docker-build-application-executor", "EXECUTOR_IMG=" + executor_base_tag,
                    "APPLICATION_EXECUTOR_IMG=" + executor_tag], timeout=1200)
        report["stage"] = "production executor login-shell toolchain preflight"
        checks.run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "/bin/bash", executor_tag,
                    "-lc", "command -v go && go version && command -v gofmt"], timeout=30)
        report["checks"]["productionExecutorLoginShellToolchain"] = True
        created = True
        checks.run(["kind", "create", "cluster", "--name", cluster, "--image", support.NODE_IMAGE,
                    "--kubeconfig", checks.env["KUBECONFIG"], "--wait", "120s"])
        Path(checks.env["KUBECONFIG"]).chmod(0o600)
        checks.run(["kind", "load", "docker-image", controller_tag, gateway_tag, executor_tag, "--name", cluster])
        listing = checks.run(["docker", "exec", cluster + "-control-plane", "ctr", "-n", "k8s.io", "images", "ls"])
        image = support.immutable_reference(executor_tag, listing)
        checks.run(["docker", "exec", cluster + "-control-plane", "ctr", "-n", "k8s.io", "images", "tag", "docker.io/library/" + executor_tag, image])
        architecture = checks.run(["docker", "image", "inspect", executor_tag, "--format", "{{.Architecture}}"])
        report["images"] = {tag: checks.run(["docker", "image", "inspect", tag, "--format", "{{.Id}}"])
                            for tag in (controller_tag, gateway_tag, executor_base_tag, executor_tag)}
        checks.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS, "labels": {"pod-security.kubernetes.io/enforce": "restricted"}}})
        checks.secret("openai-application", {"OPENAI_API_KEY": keys["OPENAI_API_KEY"]})
        checks.secret("codex-executor", {"CODEX_API_KEY": keys["OPENAI_EXECUTOR_API_KEY"]})
        checks.secret("gateway-auth", {"principals.json": json.dumps([{"token": token, "subject": "live-acceptance", "namespace": NS}])})
        certificate = gateway_tls.provision(checks, directory)
        evidence.protect([(directory / "gateway.key").read_text()])
        internal = checks.get("secret", "gateway-internal")
        evidence.protect([base64.b64decode(internal["data"]["token"]).decode()])
        database_url = "postgres://postgres@gateway-postgres:5432/postgres?sslmode=disable"
        checks.secret("gateway-database", {"url": database_url})
        checks.kubectl("apply", "-f", "config/demo/postgres.yaml")
        checks.kubectl("rollout", "status", "deployment/gateway-postgres", "-n", NS, "--timeout=180s")
        patches = [
            {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": support.DEPLOYMENT, "namespace": NS},
             "spec": {"template": {"spec": {"containers": [{"name": "manager", "imagePullPolicy": "IfNotPresent", "env": [
                 {"name": "EXECUTOR_IMAGES", "value": image},
                 {"name": "WORKSPACE_STORAGE_CLASSES", "value": "standard"},
                 {"name": "REPOSITORY_URLS", "value": REPOSITORY_URL},
                 {"name": "OPENAI_AGENT_OWNER_SCOPE", "value": report["scope"]},
             ]}]}}}},
            {"apiVersion": "apps/v1", "kind": "Deployment",
             "metadata": {"name": "codex-session-operator-gateway", "namespace": NS},
             "spec": {"template": {"spec": {"containers": [{"name": "gateway", "imagePullPolicy": "IfNotPresent",
                 "args": ["--gateway-database-insecure-local"]}]}}}},
        ]
        overlay = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                   "resources": ["../default"],
                   "images": [{"name": "controller", "newName": controller_tag.rpartition(":")[0], "newTag": controller_tag.rpartition(":")[2]},
                              {"name": "gateway", "newName": gateway_tag.rpartition(":")[0], "newTag": gateway_tag.rpartition(":")[2]}],
                   "patches": [{"patch": json.dumps(patch)} for patch in patches]}
        with tempfile.TemporaryDirectory(prefix=".live-", dir=ROOT / "config") as temporary:
            (Path(temporary) / "kustomization.yaml").write_text(json.dumps(overlay))
            manifest = checks.run([str(ROOT / "bin/kustomize"), "build", temporary])
        checks.kubectl("apply", "-f", "-", data=manifest)
        checks.kubectl("wait", "--for=condition=Established", "crd/agents.agents.softcane.io",
                       "crd/environments.agents.softcane.io", "crd/sessions.agents.softcane.io",
                       "crd/vaults.agents.softcane.io", "crd/vaultcredentials.agents.softcane.io",
                       "crd/profiles.agents.softcane.io", "--timeout=90s")
        for deployment in (support.DEPLOYMENT, "codex-session-operator-gateway"):
            checks.kubectl("rollout", "status", "deployment/" + deployment, "-n", NS, "--timeout=180s")
        report["crdSchemaHashes"] = {name: hashlib.sha256(checks.kubectl("get", "crd", name + ".agents.softcane.io", "-o", "jsonpath={.spec.versions}").encode()).hexdigest()
                                    for name in ("agents", "environments", "vaults", "vaultcredentials", "sessions", "profiles")}
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        api = support.API(port, token, ssl.create_default_context(cafile=str(certificate)))
        forward = support.start_forward(checks, port)
        def reachable():
            try:
                return api.request("/v1/namespaces/" + NS + "/catalog")[0] == 200
            except (OSError, urllib.error.URLError):
                return False
        checks.wait("authenticated gateway listening", reachable)
        def service_restart():
            nonlocal forward
            def uids(name):
                deployment = checks.get("deployment", name)
                selector = ",".join(key + "=" + value for key, value in sorted(deployment["spec"]["selector"]["matchLabels"].items()))
                pods = json.loads(checks.kubectl("get", "pods", "-n", NS, "-l", selector, "-o", "json"))["items"]
                return {pod["metadata"]["uid"] for pod in pods if not pod["metadata"].get("deletionTimestamp") and
                        any(condition.get("type") == "Ready" and condition.get("status") == "True" for condition in pod.get("status", {}).get("conditions", []))}
            deployments = (support.DEPLOYMENT, "codex-session-operator-gateway")
            old = {name: uids(name) for name in deployments}
            ensure(all(old.values()), "Service replacement requires observed old ready Pods")
            forward.terminate()
            forward.wait(10)
            forward = None
            for name in deployments:
                checks.kubectl("rollout", "restart", "deployment/" + name, "-n", NS)
                checks.kubectl("rollout", "status", "deployment/" + name, "-n", NS, "--timeout=180s")
            for name in deployments:
                checks.wait("old service Pods absent", lambda: not old[name].intersection(uids(name)), 180)
            forward = support.start_forward(checks, port)
            checks.wait("gateway listening after service replacement", reachable)
            report["checks"]["actualServicePodReplacement"] = True
        journey = Journey(checks, api, Provider(keys["OPENAI_API_KEY"]), evidence, report, image, architecture, service_restart,
                          without_session_budget=args.without_session_budget)
        agent = journey.publish("Agent", "live-agent", {"name": "Live repository investigator", "model": args.model,
            "instructions": "Use actual shell commands and repository evidence. Do not inspect credentials. Follow task write limits.", "deletionPolicy": "Delete"})
        journey.resources[("Agent", "live-agent")]["remoteID"] = agent["status"]["openaiAgentID"]
        for policy in ("Delete", "Retain"):
            journey.exercise(policy)
        ensure(support.source_hashes() == report["sourceHashes"], "Sources changed during live acceptance")
        report["checks"]["sourcesUnchanged"] = True
        report["result"] = "passed"
        result = 0
    except (Exception, KeyboardInterrupt) as error:
        report["error"] = str(error) or type(error).__name__
        if checks:
            for value in checks.keys.values():
                report["error"] = report["error"].replace(value, "[withheld]").replace(base64.b64encode(value.encode()).decode(), "[withheld]")
        print("Live acceptance failed; private report records the stage and recovery obligations.", flush=True)
    finally:
        clean = not created
        try:
            if journey:
                journey.cleanup()
                clean = True
            if created and clean:
                checks.run(["kind", "delete", "cluster", "--name", cluster])
            elif created:
                report["cleanupAuditRequired"] = "Installation exists without a complete remote-obligation ledger; cluster retained"
                result = 1
        except (Exception, KeyboardInterrupt) as error:
            report["cleanupAuditRequired"] = type(error).__name__
            result = 1
        if forward:
            forward.terminate()
            try:
                forward.wait(10)
            except Exception:
                forward.kill()
                forward.wait(10)
                result = 1
        report["checks"]["remoteCleanupConfirmed"] = clean
        report["checks"]["globalKubeconfigUnchanged"] = local_setup.global_configs() == baseline
        if checks and existing is not None:
            try:
                remaining = checks.run(["kind", "get", "clusters"]).splitlines()
                report["checks"]["existingClustersPreserved"] = set(existing).issubset(remaining)
                report["clusterKept"] = cluster in remaining
                if report["clusterKept"]:
                    report["recoveryKubeconfig"] = checks.env["KUBECONFIG"]
                elif created:
                    report["checks"]["ownedClusterRemoved"] = True
            except Exception:
                result = 1
        if not all(report["checks"].values()):
            result = 1
        report["result"] = "passed" if result == 0 else "failed"
        text = json.dumps(report, indent=2)
        for protected in evidence.variants:
            text = text.replace(protected, "[withheld]")
        evidence.write("report.json", text)
        public_report(directory, report)
        print("Live acceptance", report["result"] + "; private evidence:", directory, flush=True)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
