"""Bounded workflow recovery/interruption proofs through deployed public surfaces."""

import json
import hashlib

import cli
import faults
import research
import sandbox
import support

NS, ensure = support.NS, support.ensure


def unacknowledged_tool(detail, call_id):
    return next((value for value in ((detail or {}).get("submissions") or [])
                 if (value.get("event") or {}).get("call_id") == call_id and not value["accepted"] and value.get("attempted_at")), None)


def held_worker_input(detail, snapshot, remote_id):
    """Wait until initial input has both a held job and its saved running root."""
    jobs = [job for job in snapshot.get("jobs") or [] if remote_id and job.get("session_id") == remote_id]
    if len(jobs) == 1 and jobs[0].get("started") is False and jobs[0].get("turn_id") and any(
            turn.get("id") == jobs[0].get("turn_id") and turn.get("status") == "in_progress"
            for turn in research.history_values(detail, "turns")):
        return {"job": jobs[0], "detail": detail, "counts": snapshot}
    return None


def failed_prerequisite_rejection(detail):
    for item in research.history_values(detail, "items"):
        for content in item.get("content") or []:
            try:
                tool = json.loads(content.get("text", ""))
            except (TypeError, ValueError):
                continue
            if (isinstance(tool, dict) and tool.get("tool") == "workflow_delegate" and tool.get("success") is False and
                    "explicit saved successful result" in (tool.get("error") or "")):
                return detail
    return None


def missing_remote_fence(detail):
    """Use stable lifecycle fields; last_error deliberately withholds details."""
    return bool(detail and detail.get("recovery_required") and
                detail.get("lifecycle", {}).get("reason") == "RemoteSessionMissing" and
                detail["lifecycle"].get("actions", {}).get("continue") is False and
                detail["lifecycle"].get("execution_ready") is False)


def observation_hold(session, detail):
    """Provider observation can fail while physical compute remains connected."""
    if session and detail and detail.get("last_error") and any(
            condition.get("type") == "Ready" and condition.get("reason") == "ObservationFailed" and
            condition.get("status") == "False" and condition.get("observedGeneration") == session["metadata"]["generation"]
            for condition in session.get("status", {}).get("conditions", [])):
        return {"session": session, "detail": detail}
    return None


def blocked_tool(detail, original):
    """A disappeared provider action preserves an explicitly unknown receipt."""
    candidate = next((value for value in ((detail or {}).get("submissions") or [])
                      if value.get("key") == original["key"] or
                      (value.get("event") or {}).get("call_id") == original["event"]["call_id"]), None)
    if candidate is None:
        return None
    immutable = ("key", "remote_key", "event", "attempted_at", "attempts")
    ensure(not candidate["accepted"] and all(candidate.get(field) == original.get(field) for field in immutable),
           "Lost tool ACK changed unknown receipt or immutable submission")
    if not candidate.get("blocked"):
        return None
    ensure(candidate["blocked"] == "Action no longer pending; result delivery will not be replayed" and
           detail["lifecycle"]["recovery"] == {"required": True, "reason": "DeliveryAuditRequired"},
           "Lost tool ACK was not explicitly fenced for delivery audit")
    return candidate


def run(checks, api, base, directory, certificate, report, evidence, run_id, runtime, replace_gateway, counts):
    image, architecture, commits = runtime
    name, commit, private_repository = sandbox.workspace_recipe("Delete", commits)
    sandbox.profile(checks, name, image, architecture, commit, "Delete", private_repository)
    template = "workflow-faults"
    checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "WorkflowTemplate",
        "metadata": {"name": template, "namespace": NS}, "spec": {
            "coordinator": {"profileRef": {"name": name}}, "roles": [
                {"name": "diagnose", "profileRef": {"name": name}, "outputs": ["diagnosis"]},
                {"name": "review", "profileRef": {"name": name}, "outputs": ["findings"], "dependsOn": ["diagnose"]}]}})
    private_root = directory / template
    private_root.mkdir(mode=0o700)
    private, text, state, ca = cli.private_paths(private_root, "", certificate)
    prefix = [str(support.ROOT / "bin/gateway-client"), "--origin", api.base, "--namespace", NS, "--state-dir", str(state)]
    if ca:
        prefix += ["--ca-file", str(ca)]

    def invoke(*args):
        return research.invoke_json(checks, prefix, private, *args)

    def get(path):
        status, value = api.request(path)
        return value if status == 200 else None

    def start(mode):
        before_creates = counts()["creates"]
        text.write_text("WORKFLOW_FAULT_" + mode)
        value = invoke("task", "start", "--workflow", template, "--text-file", str(text),
                       "--operation", "fault-" + mode.lower() + "-" + run_id)
        receipt = value["receipt"]
        evidence.write("workflow-fault-" + mode.lower() + "-receipt.json", receipt)
        return {"mode": mode, "path": receipt["task_url"], "id": receipt["task_id"], "receipt": receipt, "before_creates": before_creates}

    def mapped(context, number=2):
        value = get(context["path"])
        return value if value and len(value["members"]) == number and all(member.get("session_uid") for member in value["members"]) else None

    def member_resources(context, coordinator=False):
        task = checks.wait("workflow fault member mapping", lambda: mapped(context, 1 if context["mode"] == "PENDING" else 2), 180)
        member = next(member for member in task["members"] if member["is_coordinator"] == coordinator)
        conversation = member["conversation_url"]
        detail, session, pod = support.wait_executor(checks, lambda: get(conversation), "workflow fault prepared executor")
        session, pod, pvc = sandbox.binding(checks, detail, image, commit)
        return member, conversation, session, pod, pvc

    def result_ready(context):
        value = get(context["path"])
        return value if value and value["results"] else None

    def capture(context):
        task = get(context["path"])
        resources = []
        for member in task["members"]:
            session = checks.get("session", member.get("session_name", "")) if member.get("session_name") else None
            if not session:
                continue
            status = session["status"]
            pod = checks.get("pod", status["podName"])
            pvc = support.workspace_claim(checks, session, pod)
            support.sandbox_chain(checks, session, pod)
            resources.append((session, pod, pvc))
        return resources

    def cleanup(context):
        task = get(context["path"])
        saved = {result["id"]: invoke("task", "result", context["id"], result["id"]) for result in task["results"]}
        resources = context.get("captured") or capture(context)
        ensure(len(resources) == len(task["members"]), "Fault cleanup omitted an accepted member resource chain")
        evidence.write("workflow-fault-" + context["mode"].lower() + "-before-stop.json", {
            "task": task, "results": saved, "identities": [{"session": item[0]["metadata"]["uid"],
                "pod": item[1]["metadata"]["uid"], "pvc": item[2]["metadata"]["uid"]} for item in resources]})
        invoke("task", "stop", context["id"], "--operation", "fault-stop-" + context["mode"].lower() + "-" + run_id)
        before_cleanup_creates = counts()["creates"]
        closed = checks.wait("fault explicit stop verified cleanup", lambda: value if
                            (value := get(context["path"])) and value["state"] == "closed" else None, 300)
        ensure(len(closed["members"]) == len(task["members"]) and counts()["creates"] == before_cleanup_creates,
               "Workflow fault stop admitted replacement work")
        remote = {item["id"] for item in counts()["sessions"]}
        for session, pod, pvc in resources:
            ensure(checks.get("session", session["metadata"]["name"]) is None and
                   checks.get("pod", pod["metadata"]["name"]) is None and
                   checks.get("sandboxclaim", session["status"]["sandboxClaimName"]) is None and
                   checks.get("pvc", pvc["metadata"]["name"]) is None and session["status"]["openaiSessionID"] not in remote,
                   "Workflow fault cleanup left execution/workspace resources")
        for identifier, original in saved.items():
            current = invoke("task", "result", context["id"], identifier)
            ensure({k: v for k, v in current.items() if k != "handoff_queued"} ==
                   {k: v for k, v in original.items() if k != "handoff_queued"}, "Fault cleanup erased evidence")
            destination = private / (context["mode"].lower() + "-" + identifier + ".json")
            exported = invoke("task", "result", context["id"], identifier, "--output", str(destination))
            ensure(destination.read_bytes() == current["raw"].encode() and
                   hashlib.sha256(destination.read_bytes()).hexdigest() == exported["sha256"] == current["sha256"] and
                   destination.stat().st_mode & 0o777 == 0o600, "Fault cleanup result export changed saved evidence")
        evidence.write("workflow-fault-" + context["mode"].lower() + "-closed.json", closed)

    def scenario(mode, action, controls=None):
        context = None
        sandbox.provider_control(checks, {"reset": True, **(controls or {})})
        try:
            context = start(mode)
            action(context)
        finally:
            # Restore observation/ACK availability before asking normal finalizers
            # to close; claims and finalizers are never edited by this harness.
            sandbox.provider_control(checks, {"reset": True})
            if context:
                cleanup(context)
        report["scenarios"].append({"id": "workflow_fault_" + mode.lower(), "status": "passed"})

    def lost_ack(context):
        def held():
            snapshot = counts()
            commits = [item for item in snapshot["toolResultCommits"] if item["name"] == "workflow_delegate" and
                       item["session_id"] + ":" + item["call_id"] in snapshot["heldToolResponses"]]
            if not commits:
                return None
            task = mapped(context)
            if not task:
                return None
            coordinator = next(member for member in task["members"] if member["is_coordinator"])
            detail = get(coordinator["conversation_url"])
            return {"commit": commits[0], "task": task, "detail": detail} if unacknowledged_tool(detail, commits[0]["call_id"]) else None
        held_value = checks.wait("provider committed output while gateway ACK pending", held, 180)
        evidence.write("workflow-fault-ack-unacknowledged.json", held_value)
        original_ids = [member["id"] for member in held_value["task"]["members"]]
        member_resources(context, coordinator=True)
        member_resources(context)
        original_resources = capture(context)
        ensure(len(original_resources) == 2, "ACK baseline omitted a member execution chain")
        before_creates = counts()["creates"]
        replace_gateway()
        # The provider consumed the action before its ACK was lost. The gateway
        # must preserve the unknown receipt and fence its now-inactive action;
        # disappearance is not evidence of HTTP acceptance.
        sandbox.provider_control(checks, {"releaseToolResponses": True})
        ensure(invoke("resume", "fault-ack-" + run_id)["receipt"] == context["receipt"], "Fault replay changed original receipt")
        coordinator = next(member for member in held_value["task"]["members"] if member["is_coordinator"])
        call = held_value["commit"]["call_id"]
        original_submission = unacknowledged_tool(held_value["detail"], call)
        fenced = checks.wait("lost tool ACK remains unknown and audit fenced", lambda: value if
                            (value := get(coordinator["conversation_url"])) and blocked_tool(value, original_submission) else None, 180)
        task = checks.wait("ACK recovery saved member result", lambda: result_ready(context), 240)
        accepted = next(item for item in counts()["toolResultCommits"] if item["call_id"] == call and
                        item["session_id"] == held_value["commit"]["session_id"])
        ensure(accepted["accepted"] == 1 and counts()["creates"] == before_creates and
               [member["id"] for member in task["members"]] == original_ids, "Lost ACK duplicated accepted work")
        for session, pod, pvc in original_resources:
            ensure(checks.get("session", session["metadata"]["name"])["metadata"]["uid"] == session["metadata"]["uid"] and
                   checks.get("session", session["metadata"]["name"])["status"]["openaiSessionID"] == session["status"]["openaiSessionID"] and
                   checks.get("pod", pod["metadata"]["name"])["metadata"]["uid"] == pod["metadata"]["uid"] and
                   checks.get("pvc", pvc["metadata"]["name"])["metadata"]["uid"] == pvc["metadata"]["uid"], "Lost ACK replaced execution identity")
        saved = invoke("task", "result", context["id"], task["results"][0]["id"])
        handoff = checks.wait("coordinator independently reads saved result despite unknown ACK", lambda: value if
            (value := get(coordinator["conversation_url"])) and research.tool_reads(value).get(saved["id"]) == saved["raw"] else None, 180)
        ensure(blocked_tool(handoff, original_submission), "Coordinator handoff changed lost ACK fence")
        task = get(context["path"])
        coordinator_observation = next(member for member in task["members"] if member["is_coordinator"])
        ensure(coordinator_observation.get("recovery_required") is True,
               "Task coordinator summary hid the delivery audit recovery requirement")
        evidence.write("workflow-fault-ack-fenced-evidence-recovered.json", {
            "task": task, "providerCommit": accepted, "unknownReceipt": fenced, "savedResult": saved, "coordinatorHandoff": handoff})
    scenario("ACK", lost_ack, {"holdToolResultName": "workflow_delegate"})

    def pending_stop(context):
        def pending():
            value = mapped(context, 1)
            if not value:
                return None
            remote = value["members"][0].get("provider_session_id")
            actions = [item for item in counts()["pendingWorkflowActions"] if item["session_id"] == remote and item["name"] == "workflow_delegate"]
            return {"task": value, "actions": actions} if actions else None
        value = checks.wait("fresh provider delegation pending during observation hold", pending, 180)
        before = counts()["creates"]
        context["captured"] = capture(context)
        invoke("task", "stop", context["id"], "--operation", "fault-stop-pending-" + run_id)
        sandbox.provider_control(checks, {"reset": True})
        ensure(api.request(context["path"] + "/messages", "POST", {"text": "fresh"}, "fault-post-stop")[0] == 410,
               "Stopped workflow accepted input")
        latest = get(context["path"])
        ensure(len(latest["members"]) == 1 and counts()["creates"] == before, "Stop accepted pending fresh delegation")
        evidence.write("workflow-fault-pending-stop.json", {"before": value, "after": latest})
    scenario("PENDING", pending_stop)

    def started(context):
        item = member_resources(context)
        marker = checks.wait("workflow actual long command marker", lambda: faults.child_marker(checks, item[3]["metadata"]["name"]), 45)
        detail = checks.wait("workflow long matching saved running root", lambda: faults.running_root(
            get(item[1]), marker, counts(), item[2]["status"]["openaiSessionID"]), 30)
        evidence.write("workflow-fault-" + context["mode"].lower() + "-running.json", {"marker": marker, "detail": detail,
            "podUID": item[3]["metadata"]["uid"], "fixtureTimeoutMilliseconds": 120000})
        return item, marker

    def interruption(context):
        item, marker = started(context)
        baseline = counts()
        checks.kubectl("delete", "pod", item[3]["metadata"]["name"], "-n", NS, "--wait=true", "--timeout=120s")
        failed = checks.wait("workflow interrupted root honestly unknown", lambda: value if
            (value := get(item[1])) and any(turn["id"] == marker["job"].split(":")[-1] and turn["status"] == "failed"
            for turn in research.history_values(value, "turns")) else None, 180)
        _, session, _ = support.wait_executor(checks, lambda: get(item[1]), "workflow replacement same workspace", different_uid=item[3]["metadata"]["uid"])
        ensure(session["metadata"]["uid"] == item[2]["metadata"]["uid"] and
               session["status"]["openaiSessionID"] == item[2]["status"]["openaiSessionID"] and
               session["status"]["workspaceClaimUID"] == item[4]["metadata"]["uid"] and
               counts()["executorStarts"] == baseline["executorStarts"] and counts()["messageCommits"] == baseline["messageCommits"] and
               not get(context["path"])["results"] and any("outcome unknown" in value.get("output", "") for value in failed["history"]["items"]),
               "Interrupted workflow command reran or became a successful result")
        evidence.write("workflow-fault-long-interrupted.json", failed)
    scenario("LONG", interruption)

    def stop_running(context):
        item, _ = started(context)
        baseline = counts()["executorStarts"]
        context["captured"] = capture(context)
        invoke("task", "stop", context["id"], "--operation", "fault-stop-stop-" + run_id)
        checks.wait("stopped running command Pod absent", lambda: checks.get("pod", item[3]["metadata"]["name"]) is None, 180)
        ensure(counts()["executorStarts"] == baseline and not get(context["path"])["results"], "Stop reran or falsely completed running work")
    scenario("STOP", stop_running)

    def observation(context):
        item = member_resources(context)
        def initial_input_settled():
            return held_worker_input(get(item[1]), counts(), item[2]["status"]["openaiSessionID"])
        settled = checks.wait("held worker input committed and running root saved before503", initial_input_settled, 180)
        evidence.write("workflow-fault-observe-before-fault.json", settled)
        before = settled["counts"]
        sandbox.provider_control(checks, {"sessionGetStatus": 503, "sessionFaultID": item[2]["status"]["openaiSessionID"], "dropStreams": True})
        def observation_held():
            return observation_hold(checks.get("session", item[2]["metadata"]["name"]), get(item[1]))
        held = checks.wait("transient provider observation hold", observation_held, 180)
        current_counts = counts()
        ensure(all(current_counts[key] == before[key] for key in ("creates", "messageCommits", "executorStarts")) and
               not get(context["path"])["results"] and
               checks.get("session", item[2]["metadata"]["name"])["metadata"]["uid"] == item[2]["metadata"]["uid"],
               "Observation outage changed identity or admitted provider work")
        evidence.write("workflow-fault-observe-held.json", held)
        sandbox.provider_control(checks, {"reset": True})
        _, current, _ = support.wait_executor(checks, lambda: get(item[1]), "observation restored identical executor")
        ensure(current["status"]["openaiSessionID"] == item[2]["status"]["openaiSessionID"] and
               current["status"]["workspaceClaimUID"] == item[4]["metadata"]["uid"] and counts()["creates"] == before["creates"],
               "Restored observation adopted replacement identity")
        checks.wait("transient hold recovers saved result", lambda: result_ready(context), 240)
    scenario("OBSERVE", observation, {"executorHold": True})

    def missing(context):
        checks.wait("missing case first saves evidence", lambda: result_ready(context), 240)
        item = member_resources(context)
        context["captured"] = capture(context)
        before = counts()["creates"]
        sandbox.provider_control(checks, {"sessionGetStatus": 404, "sessionFaultID": item[2]["status"]["openaiSessionID"], "dropStreams": True})
        fenced = checks.wait("confirmed provider404 seals identity", lambda: value if
            (value := get(item[1])) and missing_remote_fence(value) else None, 180)
        sandbox.provider_control(checks, {"reset": True})
        replace_gateway()
        after = get(item[1])
        ensure(missing_remote_fence(after) and after["submissions"] == fenced["submissions"] and counts()["creates"] == before and get(context["path"])["results"],
               "Resetting provider404 revived or replaced sealed identity")
        evidence.write("workflow-fault-missing-fenced.json", {"beforeReset": fenced, "afterReset": after})
    scenario("MISSING", missing)

    def failed_dependency(context):
        value = checks.wait("failed assignment result saved", lambda: result_ready(context), 240)
        result = value["results"][0]
        ensure(result["outcome"] == "failed", "Nonzero member command became success")
        member = member_resources(context)
        saved = invoke("task", "result", context["id"], result["id"])
        check = json.loads(saved["raw"])["outputs"][0]["checks"][0]
        command = next(item for item in get(member[1])["history"]["items"] if item.get("id") == check["source_item_id"])
        actual = json.loads(support.read_only_exec(checks, member[3]["metadata"]["name"], "cat", "/workspace/fixture-report.json"))
        evidence.write("workflow-fault-failed-command.json", {"result": saved, "command": command, "actual": actual})
        ensure(json.loads(command["output"]) == actual and command["exit_code"] == check["exit_code"] == 1 and
               command["output"] == check["output"] and command["command"] == check["command"] and actual["hostname"] == member[3]["metadata"]["name"] and
               any(item.get("id") == command["id"] for item in saved["source_evidence"]),
               "Failed prerequisite lacks actual saved command evidence")
        coordinator = next(member for member in value["members"] if member["is_coordinator"])
        rejected_value = checks.wait("failed prerequisite rejects review delegation", lambda:
            failed_prerequisite_rejection(get(coordinator["conversation_url"])), 180)
        task = get(context["path"])
        ensure(task["state"] == "open" and len(task["members"]) == 2 and len(task["results"]) == 1 and
               counts()["creates"] == context["before_creates"] + 2,
               "Failed prerequisite automatically spawned replacement/dependent work")
        evidence.write("workflow-fault-failed-dependency.json", {"task": task, "coordinator": rejected_value})
    scenario("FAIL", failed_dependency)
    checks.kubectl("delete", "workflowtemplate/" + template, "profile/" + name, "environment/" + name,
                   "-n", NS, "--wait=true", "--timeout=120s")
    report["checks"]["workflowFaultRecoveryInterruptionStopAndDependencyFences"] = True
