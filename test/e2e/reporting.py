"""Put test evidence outside the repository's single-document source tree."""

import os
from pathlib import Path
import tempfile


def report_path(name):
    configured = os.environ.get("CODEX_TEST_REPORT_DIR")
    directory = Path(configured) if configured else Path(tempfile.gettempdir()) / "codex-session-operator-reports"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.stat().st_mode & 0o077:
        raise RuntimeError("Report directory must be private to the current user")
    return directory / name
