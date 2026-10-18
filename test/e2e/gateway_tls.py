"""Provision disposable gateway TLS and controller RPC credentials for Kind tests."""

from pathlib import Path
import secrets


def provision(checks, directory: Path, days=1) -> Path:
    if type(days) is not int or days < 1:
        raise ValueError("TLS certificate days must be a positive integer")
    certificate = directory / "gateway.crt"
    key = directory / "gateway.key"
    checks.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", str(days),
        "-keyout", str(key), "-out", str(certificate),
        "-subj", "/CN=codex-session-operator-gateway",
        "-addext", "subjectAltName=DNS:codex-session-operator-gateway.codex-session-operator-system.svc,IP:127.0.0.1",
    ], sensitive=True)
    pem = certificate.read_text()
    checks.secret("gateway-tls", {"tls.crt": pem, "tls.key": key.read_text(), "ca.crt": pem})
    checks.secret("gateway-internal", {"token": secrets.token_urlsafe(48)})
    return certificate
