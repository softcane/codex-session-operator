"""Credential, context and private-state contracts for the hands-on local setup."""

import contextlib
import importlib.util
import io
import json
import os
import socket
from pathlib import Path
from types import SimpleNamespace
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("local_up", ROOT / "hack/local-up.py")
local_up = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_up)


class LocalSetupTests(unittest.TestCase):
    def test_sandbox_author_is_pinned_persistent_and_limited_without_creating_resources(self):
        args = SimpleNamespace(model="gpt-6-luna", storage_class="standard",
                               repository_url=local_up.REPOSITORY_URL, repository_commit=local_up.REPOSITORY_COMMIT)
        image = "registry.example/executor@sha256:" + "a" * 64
        with patch.object(local_up.subprocess, "run") as run:
            author = local_up.author_resources(args, image, "arm64")
        run.assert_not_called()
        profile = author["profile"]["spec"]
        self.assertEqual(profile["spendControl"], {"mode": "limited", "limitCents": 500})
        runtime = author["environment"]["spec"]["selfHosted"]
        self.assertEqual(runtime["image"], image)
        self.assertEqual(runtime["repository"], {"url": local_up.REPOSITORY_URL, "commit": local_up.REPOSITORY_COMMIT})
        self.assertEqual(runtime["workspaceStorage"]["claimDeletionPolicy"], "Delete")
        self.assertNotIn("Session", [value["kind"] for value in author.values()])

    def test_text_author_declares_current_profile_without_creating_a_session(self):
        args = SimpleNamespace(model="gpt-6-luna")
        author = local_up.author_resources(args, None, None)
        self.assertEqual(author["environment"]["spec"], {"mode": "none", "deletionPolicy": "Delete"})
        self.assertEqual(author["profile"]["spec"], {
            "agentRef": {"name": "investigator"},
            "environmentRef": {"name": "text-only"},
            "spendControl": {"mode": "limited", "limitCents": 500},
        })
        self.assertNotIn("Session", [value["kind"] for value in author.values()])

    def test_executor_key_uses_literal_private_input_and_preserves_application_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("OPENAI_API_KEY=application-placeholder\nOPENAI_EXECUTOR_API_KEY='executor-$(literal)'\n")
            self.assertEqual(local_up.configuration_key({}, path, "OPENAI_EXECUTOR_API_KEY"), "executor-$(literal)")
            self.assertEqual(local_up.application_key({}, path), "application-placeholder")
    def test_help_has_no_runtime_or_credential_side_effects(self):
        with patch.object(local_up.sys, "argv", ["local-up.py", "--help"]), \
                patch.object(local_up, "application_key") as key, \
                patch.object(local_up.subprocess, "run") as run, \
                contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as result:
            local_up.main()
        self.assertEqual(result.exception.code, 0)
        key.assert_not_called()
        run.assert_not_called()

    def test_default_port_uses_another_free_loopback_port_when_preferred_is_occupied(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            preferred = occupied.getsockname()[1]
            with patch.object(local_up, "PREFERRED_PORT", preferred):
                selected = local_up.choose_port()
            self.assertNotEqual(selected, preferred)
            with socket.socket() as available:
                available.bind(("127.0.0.1", selected))

    def test_explicit_occupied_port_fails_before_credentials_or_commands(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            port = occupied.getsockname()[1]
            with patch.object(local_up.sys, "argv", ["local-up.py", "--port", str(port)]), \
                    patch.object(local_up, "application_key") as key, \
                    patch.object(local_up.subprocess, "run") as run, \
                    contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as result:
                local_up.main()
            self.assertEqual(result.exception.code, 2)
            key.assert_not_called()
            run.assert_not_called()

    def test_available_explicit_port_is_preserved(self):
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        self.assertEqual(local_up.choose_port(port), port)

    def test_environment_key_takes_precedence_without_reading_dotenv(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("OPENAI_API_KEY='unterminated")
            self.assertEqual(local_up.application_key({"OPENAI_API_KEY": "process-key"}, path), "process-key")

    def test_dotenv_is_literal_and_never_executes_shell_content(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("IGNORED=$(touch malicious)\nexport OPENAI_API_KEY='literal-$(not-executed)' # comment\n")
            with patch.object(local_up.subprocess, "run") as run:
                self.assertEqual(local_up.application_key({}, path), "literal-$(not-executed)")
            run.assert_not_called()

    def test_invalid_key_errors_never_include_credential_material(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            for value in ("'private-key-with-unclosed-quote", "private-key with-space"):
                with self.subTest(value=value):
                    path.write_text("OPENAI_API_KEY=" + value)
                    with self.assertRaises(ValueError) as error:
                        local_up.application_key({}, path)
                    self.assertNotIn("private-key", str(error.exception))

    def test_secret_only_uses_stdin_and_error_redacts_both_encodings(self):
        key = "private-application-key"
        with tempfile.TemporaryDirectory() as directory:
            instance = local_up.Local(Path(directory), key)
            failure = subprocess.CompletedProcess([], 1, "", key + " " + instance.protected[1])
            with patch.object(local_up.subprocess, "run", return_value=failure) as run, \
                    contextlib.redirect_stdout(io.StringIO()) as output, self.assertRaises(RuntimeError) as error:
                instance.secret("openai-application", {"OPENAI_API_KEY": key})
            args, options = run.call_args
            self.assertNotIn(key, " ".join(args[0]))
            self.assertEqual(json.loads(options["input"])["stringData"]["OPENAI_API_KEY"], key)
            self.assertEqual(options["env"]["KUBECONFIG"], str(Path(directory) / "kubeconfig"))
            self.assertIn(local_up.CONTEXT, args[0])
            self.assertNotIn(key, output.getvalue() + str(error.exception))
            self.assertNotIn(instance.protected[1], str(error.exception))

    def test_tls_duration_rejects_nonpositive_or_noninteger_before_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            for days in (0, -1, 1.5, True, "30"):
                with self.subTest(days=days), patch.object(local_up.subprocess, "run") as run:
                    with self.assertRaises(ValueError):
                        local_up.gateway_tls.provision(local_up.Local(Path(directory), "unused"), Path(directory), days=days)
                    run.assert_not_called()
                    self.assertFalse((Path(directory) / "gateway.key").exists())

    def test_tls_keeps_one_day_default_and_allows_thirty_day_local_certificate(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            def create_certificate(args, **_options):
                if args[0] != "openssl":
                    return
                Path(args[args.index("-keyout") + 1]).write_text("private-fixture")
                Path(args[args.index("-out") + 1]).write_text("public-fixture")
            for options, expected in (({}, "1"), ({"days": 30}, "30")):
                with self.subTest(options=options), patch.object(local_up.Local, "run", side_effect=create_certificate) as run:
                    local_up.gateway_tls.provision(local_up.Local(path, "unused"), path, **options)
                    arguments = run.call_args_list[0].args[0]
                    self.assertEqual(arguments[arguments.index("-days") + 1], expected)

    def test_readiness_checks_private_ca_health_and_authenticated_catalog_then_closes_tunnel(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            responses = [io.BytesIO(b"ok"), io.BytesIO(b'{"caller_subject":"local-user"}')]
            for response in responses:
                response.status = 200
            with patch.object(local_up.ssl, "create_default_context") as ssl_context, \
                    patch.object(local_up.subprocess, "Popen") as popen, \
                    patch.object(local_up.urllib.request, "urlopen", side_effect=responses) as urlopen:
                popen.return_value.poll.return_value = None
                local_up.check_gateway(local_up.Local(path, "unused"), path, 18082, "private-gateway-token")
            ssl_context.assert_called_once_with(cafile=str(path / "gateway.crt"))
            requests = [call.args[0] for call in urlopen.call_args_list]
            self.assertEqual([request.full_url for request in requests], ["https://127.0.0.1:18082/readyz",
                "https://127.0.0.1:18082/v1/namespaces/" + local_up.NS + "/catalog"])
            self.assertTrue(all(request.get_header("Authorization") == "Bearer private-gateway-token" for request in requests))
            self.assertTrue(all(call.kwargs["context"] is ssl_context.return_value for call in urlopen.call_args_list))
            self.assertIn(local_up.CONTEXT, popen.call_args.args[0])
            self.assertEqual(popen.call_args.kwargs["env"]["KUBECONFIG"], str(path / "kubeconfig"))
            popen.return_value.terminate.assert_called_once()
            popen.return_value.wait.assert_called_once_with(10)

    def test_private_files_have_restricted_permissions_and_reject_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            path, target = Path(directory) / "env.sh", Path(directory) / "outside"
            path.touch(mode=0o644)
            local_up.private_write(path, "private-state")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            path.unlink()
            target.write_text("unchanged")
            path.symlink_to(target)
            with self.assertRaises(OSError):
                local_up.private_write(path, "must-not-write")
            self.assertEqual(target.read_text(), "unchanged")


if __name__ == "__main__":
    unittest.main()
