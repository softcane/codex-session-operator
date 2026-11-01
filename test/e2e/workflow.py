"""First delegation through production CLI/API, PostgreSQL and real Sandbox resources.

Only the inference/provider boundary is controlled. Never patch application state.
"""

import hashlib
import json

import cli
import sandbox
import support
import research

NS, ensure = support.NS, support.ensure


def verify_command_report(actual, command_output, pod_name):
    """Compare the executor's complete structured observation to its saved file."""
    observed = json.loads(command_output)
    ensure(observed == actual and actual["test_exit"] == 1 and actual["hostname"] == pod_name,
           "Saved command did not observe member Pod filesystem")


def run(checks, api, base, directory, certificate, report, evidence, run_id, runtime,
        replace_gateway, fixture_counts):
    image, architecture, commits = runtime
    binary = support.ROOT / "bin/gateway-client"
    for policy in ("Delete", "Retain"):
        name, commit, private_repository = sandbox.workspace_recipe(policy, commits)
        workflow = "workflow-first-" + policy.lower()
        sandbox.profile(checks, name, image, architecture, commit, policy, private_repository)
        before_sessions = checks.kubectl("get", "sessions", "-n", NS, "-o", "json")
        before_creates = fixture_counts()["creates"]
        checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "WorkflowTemplate",
            "metadata": {"name": workflow, "namespace": NS}, "spec": {
                "coordinator": {"profileRef": {"name": name}, "instructions": "Choose a diagnosis assignment."},
                "roles": [{"name": "diagnose", "profileRef": {"name": name},
                           "instructions": "Observe the repository test and save diagnosis.", "outputs": ["diagnosis"]}]}})
        private_root = directory / workflow
        private_root.mkdir(mode=0o700)
        private, text, state, ca = cli.private_paths(private_root, "WORKFLOW_FIRST_DELEGATION " + run_id, certificate)
        prefix = [str(binary), "--origin", api.base, "--namespace", NS, "--state-dir", str(state)]
        if ca:
            prefix += ["--ca-file", str(ca)]

        def invoke(*arguments):
            raw = checks.run(["env", "KUBECONFIG=" + str(private / "no-kubernetes-credentials"),
                              *prefix, *arguments], timeout=120)
            values = [json.loads(line) for line in raw.splitlines() if line.strip()]
            ensure(values, "Workflow production CLI emitted no JSON")
            return values[-1]

        def get(path):
            status, value = api.request(path)
            return value if status == 200 else None

        listing = invoke("workflows", "list")
        ensure(any(value["name"] == workflow for value in listing["workflows"]), "Workflow discovery omitted registered template")
        shown = invoke("workflows", "show", workflow)
        ensure(shown["spec"]["roles"][0]["outputs"] == ["diagnosis"], "Workflow show lost arbitrary output contract")
        ensure(checks.kubectl("get", "sessions", "-n", NS, "-o", "json") == before_sessions and
               fixture_counts()["creates"] == before_creates, "Configuration registration started execution")
        operation = workflow + "-start-" + run_id
        started = invoke("task", "start", "--workflow", workflow, "--text-file", str(text), "--operation", operation)
        receipt = started["receipt"]
        path, identifier = receipt["task_url"], receipt["task_id"]
        ensure(receipt["acceptance"] == "committed" and started["state"] == "accepted", "Task start did not commit acceptance")
        task = checks.wait("workflow distinct assignment and saved result", lambda: value if
                           (value := get(path)) and len(value.get("members", [])) == 2 and value.get("results") else None, 300)
        ensure(task["state"] == "open", "Saved result prematurely closed task")
        members = task["members"]
        ensure(members[0]["is_coordinator"] and not members[1]["is_coordinator"] and members[1]["role"] == "diagnose", "Wrong assignment roles")
        resources = []
        for member in members:
            conversation = member["conversation_url"]
            ensure(conversation.startswith(base + "/conversations/conv-") and
                   not conversation.startswith(base + "/conversations/conv-record-"), "Task did not expose a public conversation URL")
            detail, session, pod = support.wait_executor(checks, lambda: get(conversation), "workflow member prepared executor")
            session, pod, pvc = sandbox.binding(checks, detail, image, commit)
            resources.append((member, conversation, session, pod, pvc))
        for field in (lambda item: item[2]["metadata"]["uid"], lambda item: item[2]["status"]["openaiSessionID"],
                      lambda item: item[3]["metadata"]["uid"], lambda item: item[4]["metadata"]["uid"]):
            ensure(len({field(item) for item in resources}) == 2, "Delegation reused coordinator execution/workspace")
        result = next(value for value in task["results"] if value["role"] == "diagnose")
        saved = invoke("task", "result", identifier, result["id"])
        ensure(saved["id"] == result["id"] and saved["outcome"] == "success", "User cannot retrieve saved diagnosis")
        raw_result = json.loads(saved["raw"])
        ensure(hashlib.sha256(saved["raw"].encode()).hexdigest() == saved["sha256"], "Saved result bytes differ from digest")
        output = raw_result["outputs"][0]
        ensure(output["name"] == "diagnosis" and "1099" in output["content"] and "1000" in output["content"],
               "Saved result lacks observed repository failure")
        member_detail = get(resources[1][1])
        command = next(item for item in member_detail["history"]["items"] if item.get("id") == output["checks"][0]["source_item_id"])
        ensure(command["type"] == "command_execution" and command["output"] == output["checks"][0]["output"] and
               command["command"] == output["checks"][0]["command"], "Saved result source differs from actual command history")
        ensure(saved["action"]["session_uid"] == resources[1][2]["metadata"]["uid"] and
               saved["action"]["turn_id"] == command["turn_id"] and saved["verification"] == "reported" and
               any(item.get("id") == command["id"] and item.get("output") == command["output"] for item in saved["source_evidence"]),
               "Saved result lost server attribution or source observations")
        actual = json.loads(support.read_only_exec(checks, resources[1][3]["metadata"]["name"], "cat", "/workspace/fixture-report.json"))
        evidence.write(workflow + "-command-observation.json", {"task": identifier, "result": saved,
            "actual": actual, "command": command, "pod": resources[1][3]["metadata"]["name"]})
        verify_command_report(actual, command["output"], resources[1][3]["metadata"]["name"])

        def coordinator_read():
            value = get(resources[0][1])
            for item in (value or {}).get("history", {}).get("items", []):
                for content in item.get("content", []):
                    text_value = content.get("text", "")
                    try:
                        tool = json.loads(text_value)
                    except (ValueError, TypeError):
                        continue
                    if isinstance(tool, dict) and tool.get("tool") == "workflow_read_result" and tool.get("success"):
                        ensure(tool["arguments"]["result_id"] == result["id"] and tool["output"] == saved["raw"],
                               "Coordinator did not read exact saved result bytes")
                        return value
            return None

        checks.wait("coordinator read exact durable diagnosis", coordinator_read, 180)
        identities = [(item[2]["metadata"]["uid"], item[4]["metadata"]["uid"]) for item in resources]
        original_operation = (state / (operation + ".json")).read_bytes()
        counts_before_replay = fixture_counts()
        replace_gateway()
        # Remove only the response; the immutable request proves server replay after
        # an uncertain response, using the original operation and Idempotency-Key.
        (state / (operation + ".receipt.json")).unlink()
        replay = invoke("resume", operation)
        ensure(replay["receipt"] == receipt and (state / (operation + ".json")).read_bytes() == original_operation,
               "Workflow replay changed accepted identity")
        restored = invoke("task", "read", identifier)
        ensure([member["id"] for member in restored["members"]] == [member["id"] for member in members] and
               fixture_counts()["creates"] == counts_before_replay["creates"], "Restart/replay duplicated an assignment")
        for index, item in enumerate(resources):
            ensure(checks.get("session", item[2]["metadata"]["name"])["metadata"]["uid"] == identities[index][0] and
                   checks.get("pvc", item[4]["metadata"]["name"])["metadata"]["uid"] == identities[index][1], "Replay replaced frozen resource identities")
        journal = json.loads(original_operation)
        status, _ = api.request(base + "/tasks", "POST", {"workflow": workflow, "text": "changed"}, journal["idempotency_key"])
        ensure(status == 409, "Changed task replay was accepted")
        text.write_text("Follow up on the saved diagnosis without a new assignment.")
        followup = invoke("task", "continue", identifier, "--text-file", str(text), "--operation", workflow + "-continue")
        ensure(followup["receipt"]["task_id"] == identifier, "Task follow-up replaced task")
        invoke("task", "stop", identifier, "--operation", workflow + "-stop")
        # Task state rejection is errClosed -> writeTaskError ->
        # writeConversationError -> writeError -> HTTP 410 Gone. Legacy member
        # conversation mutations below are a distinct wrong-route conflict (409).
        ensure(api.request(path + "/messages", "POST", {"text": "fresh work"}, workflow + "-blocked")[0] == 410,
               "Stopping task did not reject fresh input with Gone")
        for _, conversation, _, _, _ in resources:
            for suffix, body in (("/messages", {"text": "fresh work"}), ("/restart", {"text": "fresh attempt"})):
                ensure(api.request(conversation + suffix, "POST", body, workflow + suffix.replace("/", "-"))[0] == 409,
                       "Task member admitted work through legacy conversation route")
        closed = checks.wait("workflow stop confirms all cleanup", lambda: value if
                            (value := get(path)) and value["state"] == "closed" else None, 300)
        ensure(len(closed["members"]) == 2 and fixture_counts()["creates"] == before_creates + 2,
               "Task stop/replay admitted a fresh assignment")
        live_remote_ids = {value["id"] for value in fixture_counts()["sessions"]}
        for _, _, session, pod, pvc in resources:
            ensure(session["status"]["openaiSessionID"] not in live_remote_ids, "Task stop left a remote Session")
            ensure(checks.get("session", session["metadata"]["name"]) is None and checks.get("pod", pod["metadata"]["name"]) is None and
                   checks.get("sandboxclaim", session["status"]["sandboxClaimName"]) is None, "Task stop left execution resources")
            if policy == "Delete":
                ensure(checks.get("pvc", pvc["metadata"]["name"]) is None, "Task Delete stop left workspace")
            else:
                retained = checks.get("pvc", pvc["metadata"]["name"])
                ensure(retained and retained["metadata"]["uid"] == pvc["metadata"]["uid"], "Task Retain stop lost workspace identity")
                owned = support.retained_sandbox(checks, session)
                if session["metadata"]["uid"] == resources[1][2]["metadata"]["uid"]:
                    probe = sandbox.read_only_probe(checks, image, pvc["metadata"]["name"])
                    ensure(json.loads(support.read_only_exec(checks, probe, "cat", "/workspace/fixture-report.json", container="reader")) == actual,
                           "Retain stop lost diagnosis workspace files")
                    checks.kubectl("delete", "pod", probe, "-n", NS, "--wait=true", "--timeout=90s")
                checks.kubectl("delete", "sandbox", owned["metadata"]["name"], "-n", NS, "--wait=true", "--timeout=90s")
        after_cleanup = invoke("task", "result", identifier, result["id"])
        # Handoff delivery is a mutable observation; all saved evidence and its
        # attribution must remain byte-identical after cleanup.
        ensure({key: value for key, value in after_cleanup.items() if key != "handoff_queued"} ==
               {key: value for key, value in saved.items() if key != "handoff_queued"} and
               after_cleanup["handoff_queued"] and closed["results"], "Cleanup erased saved diagnosis")
        exported_path = private / "diagnosis-result.json"
        exported = invoke("task", "result", identifier, result["id"], "--output", str(exported_path))
        ensure(exported["output"] == str(exported_path) and exported["sha256"] == saved["sha256"] and
               exported_path.read_bytes() == saved["raw"].encode("utf-8") and
               exported_path.stat().st_mode & 0o777 == 0o600,
               "Production CLI artifact export changed exact saved bytes, digest or private permissions")
        evidence.write(workflow + "-proof.json", {"task": identifier, "result": saved, "resources": identities,
            "binarySHA256": hashlib.sha256(binary.read_bytes()).hexdigest(), "policy": policy,
            "provider": "controlled inference; real executor command and PostgreSQL"})
        report["scenarios"].append({"id": workflow + "_deployed_delegation_replay_stop", "status": "passed"})
        checks.kubectl("delete", "workflowtemplate/" + workflow, "profile/" + name, "environment/" + name,
                       "-n", NS, "--wait=true", "--timeout=120s")
    report["checks"]["workflowFirstDelegationProductionCLIPostgreSQLSandbox"] = True

    research.run(checks, api, base, directory, certificate, report, evidence, run_id, runtime, fixture_counts)
