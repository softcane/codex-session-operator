#!/usr/bin/env python3
"""Real PostgreSQL durability, concurrency and recovery acceptance; no cluster access."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

from e2e.reporting import report_path

ROOT = Path(__file__).resolve().parents[1]
IMAGE = "postgres:18.3-alpine@sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7"


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    name = "codex-storage-pg-" + uuid.uuid4().hex[:8]
    directory = report_path(name)
    directory.mkdir(mode=0o700)
    report = {"container": name, "databaseImage": IMAGE, "checks": {},
              "sourceHashes": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                               for pattern in ("*.go", "*.sql")
                               for p in sorted((ROOT / "internal/gateway").rglob(pattern))},
              "notTested": ["power loss", "database volume loss", "standby promotion"]}

    def run(args, **kwargs):
        try:
            return subprocess.check_output(args, text=True, cwd=ROOT, **kwargs).strip()
        except subprocess.CalledProcessError as error:
            print(error.output, flush=True)
            raise

    result = 1
    try:
        run(["docker", "run", "-d", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
             "-p", "127.0.0.1::5432", IMAGE, "-c", "fsync=on", "-c", "full_page_writes=on",
             "-c", "synchronous_commit=on"])
        deadline = time.monotonic() + 60
        while subprocess.run(["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError("PostgreSQL readiness timeout")
            time.sleep(0.25)
        port = run(["docker", "port", name, "5432"]).split(":")[-1]
        env = {**os.environ, "GATEWAY_TEST_POSTGRES": f"postgres://postgres@127.0.0.1:{port}/postgres?sslmode=disable"}
        report["versionAndDurability"] = run(["docker", "exec", name, "psql", "-U", "postgres", "-Atc",
             "SELECT version(),current_setting('fsync'),current_setting('full_page_writes'),current_setting('synchronous_commit')"])
        with (directory / "postgres-tests.log").open("w") as log:
            completed = subprocess.run(["go", "test", "-race", "./internal/gateway", "-count=1", "-v"],
                                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        report["checks"]["backendContractAndFaults"] = completed.returncode == 0
        if completed.returncode:
            raise RuntimeError("PostgreSQL acceptance failed; see " + str(directory / "postgres-tests.log"))
        env["GATEWAY_TEST_CRASH_RECEIPT_FILE"] = str(directory / "original-receipt.json")
        env["GATEWAY_TEST_CRASH_STAGE"] = "seed"
        run(["go", "test", "./internal/gateway", "-run", "^TestPostgreSQLCrashProof$", "-count=1", "-v"], env=env)
        # A real SIGKILL forces WAL crash recovery on the same durable volume.
        run(["docker", "exec", name, "psql", "-U", "postgres", "-c",
             "CREATE TABLE crash_proof(key text PRIMARY KEY); INSERT INTO crash_proof VALUES('acknowledged')"])
        run(["docker", "kill", "--signal", "KILL", name])
        run(["docker", "start", name])
        deadline = time.monotonic() + 60
        while subprocess.run(["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
            if time.monotonic() > deadline:
                raise RuntimeError("PostgreSQL crash recovery timeout")
            time.sleep(0.25)
        assert run(["docker", "exec", name, "psql", "-U", "postgres", "-Atc",
                    "SELECT key FROM crash_proof"]) == "acknowledged"
        port = run(["docker", "port", name, "5432"]).split(":")[-1]
        env["GATEWAY_TEST_POSTGRES"] = f"postgres://postgres@127.0.0.1:{port}/postgres?sslmode=disable"
        env["GATEWAY_TEST_CRASH_STAGE"] = "verify"
        report["processCrashProof"] = run(["go", "test", "./internal/gateway", "-run", "^TestPostgreSQLCrashProof$", "-count=1", "-v"], env=env)
        report["checks"]["databaseSIGKILLDurability"] = True
        report["checks"]["gatewayProcessReceiptReplay"] = True
        result = 0
    finally:
        subprocess.run(["docker", "rm", "-f", "-v", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        report["exitCode"] = result
        (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print("PostgreSQL evidence:", directory, flush=True)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
