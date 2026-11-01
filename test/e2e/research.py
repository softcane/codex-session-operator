"""Deployed bounded research fixture: parallel workers, independent review, synthesis.

Inference is controlled. Node calculations, gateway state and execution resources
are real; this proves transport and finite observations, not scientific validity.
"""

import hashlib
import json

import cli
import sandbox
import support

NS, ensure = support.NS, support.ensure


def invoke_json(checks, prefix, private, *arguments):
    raw = checks.run(["env", "KUBECONFIG=" + str(private / "no-kubernetes-credentials"),
                      *prefix, *arguments], timeout=120)
    values = [json.loads(line) for line in raw.splitlines() if line.strip()]
    ensure(values, "Research production CLI emitted no JSON")
    return values[-1]


def mapped_task(value, count):
    """Accepted member intents precede Work, conversation and Session mappings.

    This only establishes public identity mapping. Executor readiness still needs
    the independent wait_executor/ownership checks before any resource reads.
    """
    members = (value or {}).get("members") or []
    if len(members) == count and all(member.get("conversation_url") and member.get("session_uid") for member in members):
        return value
    return None


def history_values(detail, field):
    """Unsaved provider history is legitimately missing or null during polling."""
    return ((detail or {}).get("history") or {}).get(field) or []


def tool_reads(detail):
    reads = {}
    for item in history_values(detail, "items"):
        for content in item.get("content") or []:
            try:
                value = json.loads(content.get("text", ""))
            except (TypeError, ValueError):
                continue
            if isinstance(value, dict) and value.get("tool") == "workflow_read_result" and value.get("success"):
                arguments = value.get("arguments") or {}
                if isinstance(arguments, dict) and isinstance(arguments.get("result_id"), str) and isinstance(value.get("output"), str):
                    reads[arguments["result_id"]] = value["output"]
    return reads


def delegated_dependencies(detail, role):
    for item in history_values(detail, "items"):
        for content in item.get("content") or []:
            try:
                value = json.loads(content.get("text", ""))
            except (TypeError, ValueError):
                continue
            if isinstance(value, dict) and value.get("tool") == "workflow_delegate" and value.get("success"):
                arguments = value.get("arguments") or {}
                if isinstance(arguments, dict) and arguments.get("role") == role:
                    return arguments.get("dependencies") or []
    return None


def check_report(actual, command, role, pod, references):
    expected_samples = [{"n": n, "value": str((0 if n == 0 else 2*n-1) if role == "argument" else n*n)}
                        for n in (0, 1, 2, 10, 1000)]
    ensure(json.loads(command["output"]) == actual and actual["samples"] == expected_samples and
           actual["role"] == role and actual["hostname"] == pod and
           actual["cwd"] == "/workspace" and actual["verified"] and actual["max_n"] == 1000 and
           actual["case_count"] == 1001 and actual["references"] == references and command["exit_code"] == 0,
           "Research saved command differs from actual bounded computation")


def run(checks, api, base, directory, certificate, report, evidence, run_id, runtime, fixture_counts):
    image, architecture, commits = runtime
    name, commit, private_repository = sandbox.workspace_recipe("Retain", commits)
    sandbox.profile(checks, name, image, architecture, commit, "Retain", private_repository)
    workflow = "research-bounded"
    checks.apply({"apiVersion": "agents.softcane.io/v1alpha1", "kind": "WorkflowTemplate",
        "metadata": {"name": workflow, "namespace": NS}, "spec": {
            "coordinator": {"profileRef": {"name": name}, "instructions":
                "Assign independent argument and experiment work before either result; review both and synthesize exact saved IDs."},
            "roles": [{"name": role, "profileRef": {"name": name}, "outputs": [output],
                       **({"dependsOn": ["argument", "experiment"]} if role == "review" else {})}
                      for role, output in (("argument", "argument"), ("experiment", "observations"), ("review", "findings"))]}})
    private_root = directory / workflow
    private_root.mkdir(mode=0o700)
    private, text, state, ca = cli.private_paths(private_root,
        "RESEARCH_PILOT supplied claim: sum(k=1..n)(2k-1)=n²; telescope 2k-1=k²-(k-1)²; compute n=0..1000.", certificate)
    binary = support.ROOT / "bin/gateway-client"
    prefix = [str(binary), "--origin", api.base, "--namespace", NS, "--state-dir", str(state)]
    if ca:
        prefix += ["--ca-file", str(ca)]

    def invoke(*args):
        return invoke_json(checks, prefix, private, *args)

    def get(path):
        status, value = api.request(path)
        return value if status == 200 else None

    def task_with_members(count):
        value = get(path)
        return mapped_task(value, count)

    before = fixture_counts()
    sandbox.provider_control(checks, {"executorHold": True})
    try:
        started = invoke("task", "start", "--workflow", workflow, "--text-file", str(text),
                         "--operation", "research-start-" + run_id)
        receipt = started["receipt"]
        ensure(started["state"] == "accepted" and receipt["acceptance"] == "committed", "Research start lacks durable acceptance")
        path, identifier = receipt["task_url"], receipt["task_id"]
        concurrent = checks.wait("research independent assignments mapped to public conversations and Sessions", lambda: task_with_members(3), 240)
        pairs = {}
        for member in concurrent["members"]:
            if member["is_coordinator"]:
                continue
            conversation = member["conversation_url"]
            detail, session, pod = support.wait_executor(checks, lambda: get(conversation), "research worker prepared before release")
            session, pod, pvc = sandbox.binding(checks, detail, image, commit)
            pairs[member["role"]] = (member, conversation, session, pod, pvc)

        def both_dispatched():
            value = task_with_members(3)
            counts = fixture_counts()
            remote_ids = {item[2]["status"]["openaiSessionID"] for item in pairs.values()}
            jobs = [job for job in counts["jobs"] if job["session_id"] in remote_ids]
            if (value and not value["results"] and len(jobs) == 2 and not any(job["started"] for job in jobs) and
                    all(any(turn["status"] == "in_progress" for turn in member["provider_turns"])
                        for member in value["members"] if not member["is_coordinator"])):
                return {"task": value, "jobs": jobs, "executorStarts": counts["executorStarts"]}
            return None

        dispatch = checks.wait("both worker roots in progress before any result", both_dispatched, 180)
        ensure(set(pairs) == {"argument", "experiment"} and
               len({item[2]["metadata"]["uid"] for item in pairs.values()}) == 2,
               "Research workers were serialized or shared a Session")
        evidence.write("research-concurrent-dispatch.json", dispatch)
    finally:
        sandbox.provider_control(checks, {"executorHold": False})

    finished = checks.wait("research review and synthesis saved", lambda: value if
        (value := task_with_members(4)) and len(value["results"]) == 4 else None, 360)
    ensure(finished["state"] == "open", "Research synthesis automatically closed task")
    resources, saved = {}, {}
    for member in finished["members"]:
        conversation = member["conversation_url"]
        detail, session, pod = support.wait_executor(checks, lambda: get(conversation), "research completed member remains available")
        session, pod, pvc = sandbox.binding(checks, detail, image, commit)
        resources[member["role"]] = (member, conversation, session, pod, pvc)
        result_id = member["result_id"]
        saved[member["role"]] = invoke("task", "result", identifier, result_id)
        ensure(saved[member["role"]]["outcome"] == "success", "Research assignment did not save success")
        # A filesystem-only marker independently proves every retained workspace,
        # including the coordinator which does not run a computation command.
        checks.kubectl("exec", "-n", NS, pod["metadata"]["name"], "--", "node", "-e",
                       "require('node:fs').writeFileSync('research-owner.txt',process.argv[1])", member["id"])
    for field in (lambda item: item[2]["metadata"]["uid"], lambda item: item[2]["status"]["openaiSessionID"],
                  lambda item: item[3]["metadata"]["uid"], lambda item: item[4]["metadata"]["uid"]):
        ensure(len({field(item) for item in resources.values()}) == 4, "Research members shared execution/workspace identity")
    ids = {role: result["id"] for role, result in saved.items()}
    observations = {}
    for role in ("argument", "experiment", "review"):
        result, item = saved[role], resources[role]
        raw = json.loads(result["raw"])
        references = [ids["argument"], ids["experiment"]] if role == "review" else []
        ensure(raw.get("references", []) == references, "Research result references wrong prerequisite identities")
        output = raw["outputs"][0]
        command = next(value for value in get(item[1])["history"]["items"] if value.get("id") == output["checks"][0]["source_item_id"])
        actual = json.loads(support.read_only_exec(checks, item[3]["metadata"]["name"], "cat", "/workspace/research-report.json"))
        observations[role] = {"result": result, "actual": actual, "command": command}
        evidence.write("research-observations.json", observations)
        check_report(actual, command, role, item[3]["metadata"]["name"], references)
        ensure(output["content"] == command["output"] and output["checks"][0]["output"] == command["output"] and
               output["checks"][0]["command"] == command["command"] and result["verification"] == "reported" and
               result["action"]["session_uid"] == item[2]["metadata"]["uid"] and
               any(source.get("id") == command["id"] and source.get("output") == command["output"] for source in result["source_evidence"]),
               "Research result lost command/source attribution")
    reviewer = resources["review"]
    ensure(reviewer[0]["id"] != resources["argument"][0]["id"] and reviewer[0]["id"] != resources["experiment"][0]["id"],
           "Reviewer reused worker assignment")
    ensure(delegated_dependencies(get(resources["coordinator"][1]), "review") == [ids["argument"], ids["experiment"]],
           "Review delegation omitted exact successful prerequisites")
    reads = tool_reads(get(reviewer[1]))
    ensure(all(reads.get(ids[role]) == saved[role]["raw"] for role in ("argument", "experiment")),
           "Reviewer did not read both exact saved worker results")
    synthesis = json.loads(saved["coordinator"]["raw"])
    ensure(synthesis["references"] == [ids[role] for role in ("argument", "experiment", "review")] and
           synthesis["outputs"][0]["name"] == "synthesis", "Coordinator synthesis omitted exact evidence references")
    coordinator_reads = tool_reads(get(resources["coordinator"][1]))
    ensure(all(coordinator_reads.get(ids[role]) == saved[role]["raw"] for role in ("argument", "experiment", "review")),
           "Coordinator did not read each exact saved result")
    ensure(fixture_counts()["creates"] == before["creates"] + 4 and all(
        checks.get("session", item[2]["metadata"]["name"]) for item in resources.values()),
        "Research result completion closed or duplicated a member")
    evidence.write("research-before-stop.json", {"task": finished, "results": saved})
    invoke("task", "stop", identifier, "--operation", "research-stop-" + run_id)
    closed = checks.wait("research explicit stop cleanup confirmed", lambda: value if
                        (value := get(path)) and value["state"] == "closed" else None, 300)
    remote_ids = {value["id"] for value in fixture_counts()["sessions"]}
    for role, (member, _, session, pod, pvc) in resources.items():
        ensure(checks.get("session", session["metadata"]["name"]) is None and checks.get("pod", pod["metadata"]["name"]) is None and
               checks.get("sandboxclaim", session["status"]["sandboxClaimName"]) is None and
               session["status"]["openaiSessionID"] not in remote_ids, "Research stop left execution resources")
        retained = checks.get("pvc", pvc["metadata"]["name"])
        ensure(retained and retained["metadata"]["uid"] == pvc["metadata"]["uid"], "Research Retain lost original workspace")
        owned = support.retained_sandbox(checks, session)
        probe = sandbox.read_only_probe(checks, image, pvc["metadata"]["name"])
        ensure(support.read_only_exec(checks, probe, "cat", "/workspace/research-owner.txt", container="reader") == member["id"],
               "Research Retain lost member workspace marker")
        if role != "coordinator":
            ensure(json.loads(support.read_only_exec(checks, probe, "cat", "/workspace/research-report.json", container="reader")) ==
                   observations[role]["actual"], "Research Retain changed saved computation files")
        checks.kubectl("delete", "pod", probe, "-n", NS, "--wait=true", "--timeout=90s")
        final = invoke("task", "result", identifier, ids[role])
        ensure({k: v for k, v in final.items() if k != "handoff_queued"} ==
               {k: v for k, v in saved[role].items() if k != "handoff_queued"}, "Research cleanup erased saved evidence")
        destination = private / (role + "-result.json")
        exported = invoke("task", "result", identifier, ids[role], "--output", str(destination))
        ensure(destination.read_bytes() == final["raw"].encode() and
               hashlib.sha256(destination.read_bytes()).hexdigest() == exported["sha256"] == final["sha256"] and
               destination.stat().st_mode & 0o777 == 0o600, "Research result export changed saved bytes or permissions")
        checks.kubectl("delete", "sandbox", owned["metadata"]["name"], "-n", NS, "--wait=true", "--timeout=90s")
    evidence.write("research-bounded-proof.json", {"task": closed, "results": saved,
        "binarySHA256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "scope": "controlled inference, real bounded integer computation and deployed lifecycle; no scientific correctness claim"})
    report["scenarios"].append({"id": "research_parallel_workers_review_synthesis_retain", "status": "passed"})
    report["checks"]["researchParallelWorkersReviewSynthesisRetain"] = True
    checks.kubectl("delete", "workflowtemplate/" + workflow, "profile/" + name, "environment/" + name,
                   "-n", NS, "--wait=true", "--timeout=120s")
