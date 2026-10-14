#!/usr/bin/env python3
"""Use demo conversations, Session messages and work events over verified TLS."""

import argparse
import json
import os
from pathlib import Path
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def conversation_path(target, base):
    """Accept a conversation ID or its returned URL in the demo namespace."""
    prefix = base + "/conversations/"
    path = target if target.startswith("/") else prefix + target
    identifier = path[len(prefix):] if path.startswith(prefix) else ""
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,253}", identifier):
        raise ValueError("Select a conversation ID or URL in the demo namespace")
    return path


def completed_history(value, minimum_turns):
    """Only saved completed root turns establish completion of accepted input."""
    history = value.get("history") or (value.get("conversation") or {}).get("history") or {}
    turns = [turn for turn in history.get("turns") or [] if not turn.get("subagent_id")]
    if any(turn.get("status") in ("failed", "cancelled") for turn in turns):
        raise RuntimeError("An agent turn failed or was cancelled; inspect the saved conversation history")
    return len(turns) >= minimum_turns and all(turn.get("status") == "completed" for turn in turns)


def cleaned_up(value):
    summary = value.get("summary") or {}
    if summary.get("state") != "closed":
        return False
    if "conversation_id" in summary:
        return summary.get("cleaned") is True
    attempts = (value.get("work") or {}).get("attempts")
    return attempts is not None and all(attempt.get("cleaned") is True for attempt in attempts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("post", "get", "message", "conversation", "close", "restart"))
    parser.add_argument("target", help="Event file, API path, Session name, conversation ID/URL, or route for conversation")
    parser.add_argument("--text", help="Initial or follow-up message text")
    parser.add_argument("--title", help="Optional title for a new conversation")
    parser.add_argument("--key", help="Required Idempotency-Key for conversation changes or a Session message")
    parser.add_argument("--output", type=Path, help="Save JSON instead of printing it")
    parser.add_argument("--demo-dir", default=os.environ.get("DEMO_DIR"))
    parser.add_argument("--url", default=os.environ.get("DEMO_URL", "https://127.0.0.1:18082"))
    parser.add_argument("--wait-for", choices=("completed", "closed"), help="Poll GET for up to 90 seconds")
    parser.add_argument("--turns", type=int, default=1, help="Minimum completed root turns when waiting (default: 1)")
    parser.add_argument("--timeout", type=int, default=90, help="Maximum polling seconds (default: 90)")
    args = parser.parse_args()
    if args.action in ("message", "conversation", "restart") and (not args.text or not args.key):
        parser.error("This action requires --text and --key; retain the key and body when retrying")
    if args.action == "close" and not args.key:
        parser.error("Closing a conversation requires --key")
    if args.title and args.action != "conversation":
        parser.error("--title applies to a new conversation")
    if (args.wait_for and args.action != "get") or args.turns < 1 or args.timeout < 1:
        parser.error("Waiting requires GET and a positive turn count")
    if not args.demo_dir:
        parser.error("Source the env.sh printed by make demo")
    origin = urllib.parse.urlsplit(args.url)
    if origin.scheme != "https" or origin.hostname not in ("127.0.0.1", "localhost", "::1") or origin.path:
        parser.error("The demo URL must be a loopback HTTPS origin, such as https://127.0.0.1:18082")
    directory = Path(args.demo_dir)
    namespace = os.environ.get("DEMO_NAMESPACE", "codex-session-operator-system")
    base = "/v1/namespaces/" + namespace
    auth = (directory / "curl.conf").read_text().strip()
    prefix = 'header = "Authorization: Bearer '
    if not auth.startswith(prefix) or not auth.endswith('"') or "\n" in auth:
        parser.error("Invalid generated demo authentication file")
    token = auth[len(prefix):-1]
    body = None
    if args.action == "post":
        body = json.dumps(json.loads(Path(args.target).read_text())).encode()
        path = base + "/work-events"
    elif args.action == "message":
        body = json.dumps({"text": args.text}).encode()
        if args.target.startswith(("/", "conversations/")) or re.fullmatch(r"conv-[a-f0-9]{64}", args.target):
            try:
                target = base + "/" + args.target if args.target.startswith("conversations/") else args.target
                path = conversation_path(target, base) + "/messages"
            except ValueError as error:
                parser.error(str(error))
        else:
            if len(args.target) > 253 or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?", args.target):
                parser.error("Invalid Session name or conversation identity")
            path = base + "/sessions/" + args.target + "/messages"
    elif args.action == "conversation":
        value = {"route_id": args.target, "text": args.text}
        if args.title is not None:
            value["title"] = args.title
        body = json.dumps(value).encode()
        path = base + "/conversations"
    elif args.action in ("close", "restart"):
        try:
            path = conversation_path(args.target, base) + "/" + args.action
        except ValueError as error:
            parser.error(str(error))
        body = json.dumps({"text": args.text} if args.action == "restart" else {}).encode()
    else:
        path = args.target if args.target.startswith("/") else base + "/" + args.target
        if not path.startswith(base + "/") or ".." in path or "?" in path or "#" in path:
            parser.error("GET must select a path in the demo namespace")
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    if args.action in ("message", "conversation", "close", "restart"):
        headers["Idempotency-Key"] = args.key
    request = urllib.request.Request(args.url + path, data=body,
        method="POST" if body is not None else "GET",
        headers=headers)
    context = ssl.create_default_context(cafile=str(directory / "gateway.crt"))
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            with urllib.request.urlopen(request, context=context, timeout=15) as response:
                value = json.load(response)
                response_status = response.status
        except urllib.error.HTTPError as error:
            print("HTTP", error.code, error.read().decode(errors="replace").strip(), file=sys.stderr)
            error.close()
            return 1
        except urllib.error.URLError as error:
            print("Demo connection failed:", error.reason, file=sys.stderr)
            return 1
        if not args.wait_for:
            break
        if args.wait_for == "closed":
            if cleaned_up(value):
                break
        else:
            try:
                done = completed_history(value, args.turns)
            except RuntimeError as error:
                print(str(error), file=sys.stderr)
                return 1
            if done:
                break
        if time.monotonic() >= deadline:
            print("Timed out waiting for work to be", args.wait_for, file=sys.stderr)
            return 1
        time.sleep(1)
    print("HTTP", response_status, file=sys.stderr)
    output = json.dumps(value, indent=2) + "\n"
    if args.output:
        args.output.touch(mode=0o600, exist_ok=True)
        args.output.chmod(0o600)
        args.output.write_text(output)
    else:
        print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
