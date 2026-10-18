"""Deployed production CLI + private journal + HTTPS gateway + PostgreSQL journey."""

import hashlib
import json

import support

NS, ensure = support.NS, support.ensure


def private_paths(directory, token, text, certificate):
    """Canonicalize owned evidence paths before the CLI rejects symlink ancestors."""
    private = (directory / "cli").resolve()
    private.mkdir(mode=0o700)
    token_file, text_file, state = private / "gateway-token", private / "task.txt", private / "operations"
    token_file.write_text(token + "\n")
    token_file.chmod(0o600)
    text_file.write_text(text)
    text_file.chmod(0o600)
    return private, token_file, text_file, state, certificate.resolve()


def run(checks, api, base, directory, token, certificate, report, evidence, run_id, replace_gateway, fixture_counts):
    name = "cli-journey"
    route = {"id": name, "namespace": NS, "agent": "demo-investigator-v1", "environment": "demo-text-v1",
             "spend_control": {"mode": "limited", "limitCents": 500}}
    support.apply_profile(checks, route)
    private, token_file, text_file, state, certificate = private_paths(
        directory, token, "CLI_INITIAL_" + run_id + ": observe the supplied evidence.\n", certificate)
    binary = support.ROOT / "bin/gateway-client"
    prefix = [str(binary), "--origin", api.base, "--namespace", NS, "--token-file", str(token_file),
              "--ca-file", str(certificate), "--state-dir", str(state)]

    def invoke(*arguments, allowed=(0,)):
        raw = checks.run(["env", "KUBECONFIG=" + str(private / "no-kubernetes-credentials"), *prefix, *arguments],
                         allowed=allowed, timeout=120)
        values = [json.loads(line) for line in raw.splitlines() if line.strip()]
        ensure(values, "Production CLI emitted no machine-readable outcome")
        return values[-1]

    def complete(identifier, roots):
        value = invoke("read", identifier)
        return value if support.completed_history(value, roots) else None

    def cleaned(identifier):
        value = invoke("read", identifier)
        return value if support.cleaned_up(value) else None

    listing = invoke("profiles", "list", "--limit", "1")
    ensure(any(profile["name"] == name and profile["namespace"] == NS for profile in listing["profiles"]),
           "CLI pagination omitted its ready namespaced Profile")
    shown = invoke("profiles", "show", name)
    ensure(shown["profile"]["name"] == name and shown["profile"]["ready"], "CLI Profile show did not expose ready selection")
    before = fixture_counts()
    operation = "cli-start-" + run_id
    first = invoke("start", "--profile", name, "--text-file", str(text_file), "--operation", operation)
    receipt = first["receipt"]
    identifier = receipt["conversation_id"]
    ensure(first["state"] == "accepted" and receipt["acceptance"] == "committed" and not first["saved_receipt"],
           "CLI start did not distinguish fresh durable acceptance")
    initial = checks.wait("CLI initial saved completed root", lambda: complete(identifier, 1))
    root_id = next(turn["id"] for turn in initial["history"]["turns"] if not turn.get("subagent_id"))
    ambiguous = invoke("watch", identifier, "--operation", operation, "--timeout", "30s", "--poll", "100ms", allowed=(4,))
    ensure(ambiguous["state"] == "observed_root_outcome" and ambiguous["input_turn_correlation"] == "unavailable",
           "CLI watch attributed an observed root to input without provider evidence")
    explicit = invoke("watch", identifier, "--root-id", root_id, "--timeout", "30s", "--poll", "100ms")
    ensure(explicit["root"]["id"] == root_id and explicit["root"]["status"] == "completed", "CLI explicit root observation failed")
    journal = state / (operation + ".json")
    original_operation = journal.read_bytes()
    ensure(journal.stat().st_mode & 0o777 == 0o600, "CLI operation journal is not private")
    # Every invoke is a separate production CLI process. Replacement additionally
    # exercises storage recovery in entirely new gateway Pods.
    replace_gateway()
    resumed = invoke("resume", operation)
    ensure(resumed["receipt"] == receipt and resumed["saved_receipt"] and journal.read_bytes() == original_operation,
           "CLI process/gateway replacement changed its immutable request or receipt")
    checks.kubectl("delete", "profile", name, "-n", NS, "--wait=true", "--timeout=90s")
    checks.wait("CLI Profile retirement observed", lambda: api.request(base + "/profiles/" + name)[0] == 404)
    # An intact request without its response file exercises real server replay
    # after retirement. Crash/lost-response timing is qualified by CLI unit tests.
    (state / (operation + ".receipt.json")).unlink()
    replayed = invoke("resume", operation)
    ensure(replayed["receipt"] == receipt and not replayed["saved_receipt"] and journal.read_bytes() == original_operation,
           "CLI server replay after Profile retirement did not return its original receipt")
    support.apply_profile(checks, route)
    text_file.write_text("CLI_FOLLOWUP_" + run_id + ": continue the same conversation.\n")
    followup = invoke("continue", identifier, "--text-file", str(text_file), "--operation", "cli-followup-" + run_id)
    ensure(followup["receipt"]["conversation_id"] == identifier, "CLI continue replaced its conversation")
    second = checks.wait("CLI follow-up new saved root", lambda: complete(identifier, 2))
    ensure(second["summary"]["session_uid"] == initial["summary"]["session_uid"], "CLI follow-up replaced the Session UID")
    invoke("close", identifier, "--operation", "cli-close-" + run_id)
    closed = checks.wait("CLI close has verified cleanup", lambda: cleaned(identifier))
    ensure(checks.get("session", initial["summary"]["session_name"]) is None, "CLI close left its Session running")
    text_file.write_text("CLI_FRESH_" + run_id + ": start a new attempt.\n")
    fresh_result = invoke("start-fresh", identifier, "--text-file", str(text_file), "--operation", "cli-fresh-" + run_id)
    fresh_receipt = fresh_result["receipt"]
    ensure(fresh_receipt["attempt"] == 2 and fresh_receipt["conversation_id"] != identifier,
           "CLI start-fresh reused the old conversation identity")
    fresh_id = fresh_receipt["conversation_id"]
    fresh = checks.wait("CLI fresh attempt saved completed root", lambda: complete(fresh_id, 1))
    ensure(fresh["summary"]["session_uid"] != initial["summary"]["session_uid"], "CLI fresh attempt reused the prior Session")
    ensure(invoke("read", identifier)["history"]["items"] == closed["history"]["items"], "CLI fresh attempt changed retained old history")
    invoke("close", fresh_id, "--operation", "cli-fresh-close-" + run_id)
    checks.wait("CLI fresh attempt cleaned", lambda: cleaned(fresh_id))
    operations = invoke("operations")["operations"]
    ensure(len(operations) == 5 and operation in operations, "CLI lost its durable operation identities")
    after = fixture_counts()
    ensure(after["creates"] == before["creates"] + 2 and after["messageCommits"] == before["messageCommits"] + 3 and
           not after["sessions"], "CLI replay/process replacement duplicated remote work or leaked Sessions")
    evidence.write("cli-deployed-proof.json", {"binarySHA256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "firstConversation": identifier, "freshConversation": fresh_id, "firstSessionUID": initial["summary"]["session_uid"],
        "freshSessionUID": fresh["summary"]["session_uid"], "operationCount": len(operations),
        "inputTurnCorrelation": "unavailable; explicit root ID qualified separately"})
    report["checks"]["productionCLIJournalGatewayPostgreSQLLifecycle"] = True
    checks.kubectl("delete", "profile", name, "-n", NS, "--wait=true", "--timeout=90s")
