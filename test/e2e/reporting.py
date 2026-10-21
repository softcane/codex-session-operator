"""Put test evidence outside the repository's single-document source tree."""

import os
import json
from pathlib import Path
import tempfile


def report_path(name, default_root=None):
    configured = os.environ.get("CODEX_TEST_REPORT_DIR")
    directory = Path(configured) if configured else (
        Path(default_root) if default_root is not None else Path(tempfile.gettempdir()) / "codex-session-operator-reports")
    if directory.is_symlink() or directory.parent.is_symlink():
        raise RuntimeError("Report state must not use a symlinked directory")
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.stat().st_mode & 0o077:
        raise RuntimeError("Report directory must be private to the current user")
    return directory / name


def public_report(directory, report):
    """CI receives a deliberate allowlist, never private journals or provider bodies."""
    allowed = ("run", "cluster", "result", "exitCode", "provider", "layer", "checks", "sourceHashes",
               "manifestSHA256", "crdSchemaHashes", "images", "clusterKept", "notTested", "stage",
               "spendingMode", "aggregateLimitCents", "allocationCents", "spendingLimitQualified",
               "model", "singleSession", "retentionRequested", "cleanupPerformed", "access")
    value = {key: report[key] for key in allowed if key in report}
    value["scenarios"] = [{key: item[key] for key in ("id", "name", "status", "result") if key in item}
                          for item in report.get("scenarios", [])]
    output = directory / "public"
    output.mkdir(mode=0o700, exist_ok=True)
    path = output / "report.json"
    path.write_text(json.dumps(value, indent=2)+"\n")
    path.chmod(0o600)
