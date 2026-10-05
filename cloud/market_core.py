"""Portable, offline launcher for the separately supplied private Market CORE release."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import tempfile
import zipfile

PROJECT = Path(__file__).resolve().parent.parent
LOCK = PROJECT / "cloud/release-lock.json"
DEFAULT_HOME = PROJECT / ".market-core"
MAX_BUNDLE_BYTES = 16 * 1024 * 1024


class ReleaseError(ValueError):
    pass


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ReleaseError("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def read_json(path):
    def invalid(value):
        raise ReleaseError("NON_FINITE_JSON")
    return json.loads(Path(path).read_text(), object_pairs_hook=unique_object,
                      parse_constant=invalid)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False))


def release_lock():
    lock = read_json(LOCK)
    if lock.get("schema") != "market-core-cloud-release.v1" or not lock.get("files"):
        raise ReleaseError("INVALID_RELEASE_LOCK")
    for name, expected in lock["files"].items():
        path = PurePosixPath(name)
        if (path.is_absolute() or ".." in path.parts or "\\" in name
                or str(path) != name or len(expected) != 64
                or any(c not in "0123456789abcdef" for c in expected)):
            raise ReleaseError("INVALID_RELEASE_LOCK_ENTRY")
    return lock


def verify_bytes(home, lock):
    home = Path(home)
    for name, expected in lock["files"].items():
        path = home / name
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ReleaseError("SYMLINK_IN_RELEASE:" + name)
        if not path.is_file() or digest(path.read_bytes()) != expected:
            raise ReleaseError("RELEASE_FILE_MISSING_OR_CHANGED:" + name)
    # Reject unpinned executable/importable files in the release, including pyc files.
    for folder in (home / "engine", home / "policy"):
        if folder.exists():
            for path in folder.rglob("*"):
                if path.is_file() and path.relative_to(home).as_posix() not in lock["files"]:
                    raise ReleaseError("UNPINNED_RELEASE_FILE")


def import_bundle(archive, home, lock):
    home = Path(home)
    if home.exists():
        raise ReleaseError("DESTINATION_EXISTS_USE_A_NEW_PRIVATE_DIRECTORY")
    with zipfile.ZipFile(archive) as source:
        entries = source.infolist()
        names = [entry.filename for entry in entries]
        if len(names) != len(set(names)) or set(names) != set(lock["files"]):
            raise ReleaseError("BUNDLE_CONTENTS_DO_NOT_MATCH_RELEASE")
        if sum(entry.file_size for entry in entries) > MAX_BUNDLE_BYTES:
            raise ReleaseError("BUNDLE_TOO_LARGE")
        payloads = {}
        for entry in entries:
            mode = entry.external_attr >> 16
            if entry.is_dir() or stat.S_ISLNK(mode) or entry.flag_bits & 1:
                raise ReleaseError("UNSUPPORTED_ARCHIVE_ENTRY")
            payload = source.read(entry)
            if digest(payload) != lock["files"][entry.filename]:
                raise ReleaseError("BUNDLE_HASH_MISMATCH:" + entry.filename)
            payloads[entry.filename] = payload
    home.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".market-core-import-", dir=home.parent))
    try:
        for name, payload in payloads.items():
            path = stage / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            path.chmod(0o600)
        verify_bytes(stage, lock)
        stage.rename(home)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    return {"status": "IMPORTED", "resource_revision": lock["resource_revision"],
            "files_verified": len(payloads), "cloud_environment_verified": False}


def run_json(script, args, *, timeout=60):
    # Isolated Python ignores PYTHONPATH and prevents local import shadowing.
    # The maintained suites import their pinned sibling modules, so explicitly
    # add only the verified script directory and disable bytecode generation.
    bootstrap = ("import runpy,sys;sys.dont_write_bytecode=True;"
                 "p=sys.argv[1];sys.argv=sys.argv[1:];"
                 "sys.path.insert(0,str(__import__('pathlib').Path(p).parent));"
                 "runpy.run_path(p,run_name='__main__')")
    result = subprocess.run([sys.executable, "-I", "-B", "-c", bootstrap,
                             str(script), *map(str, args)],
                            capture_output=True, text=True, timeout=timeout)
    try:
        payload = json.loads(result.stdout, object_pairs_hook=unique_object)
    except (ValueError, TypeError) as exc:
        raise ReleaseError("HELPER_DID_NOT_RETURN_JSON") from exc
    if not isinstance(payload, dict):
        raise ReleaseError("HELPER_RESULT_MUST_BE_OBJECT")
    return result.returncode, payload


def validate_release(home, lock):
    verify_bytes(home, lock)
    code, result = run_json(home / "policy/validate_market_bundle.py",
                            ["--root", home / "policy", "--skill-root", home / "engine"])
    if code or result.get("status") != "PASS":
        raise ReleaseError("CANONICAL_POLICY_VALIDATION_FAILED")
    return result


def evaluate(home, lock, evidence_file, read_trace=None, test_now=None):
    validate_release(home, lock)
    evidence = read_json(evidence_file)
    if not isinstance(evidence, dict):
        raise ReleaseError("EVIDENCE_MUST_BE_OBJECT")
    if evidence.get("synthetic") is not True:
        if test_now:
            raise ReleaseError("TEST_CLOCK_REQUIRES_SYNTHETIC_EVIDENCE")
        if read_trace is None:
            raise ReleaseError("CURRENT_CANONICAL_READ_TRACE_REQUIRED")
        code, receipt = run_json(home / "engine/scripts/verify_resource_reads.py", [read_trace])
        if code or receipt.get("status") != "PASS":
            raise ReleaseError("CANONICAL_RESOURCE_READS_FAILED")
        # A complete read of a different release must not validate these bytes.
        pinned = read_json(home / "source-lock.json")["resources"]
        trace = read_json(read_trace)
        expected = trace.get("expected", [])
        observed = {(r.get("library_file_id"), str(r.get("version_id")), r.get("file_id"))
                    for r in expected if isinstance(r, dict)}
        required = {(r["library_file_id"], str(r["version_id"]), r["file_id"])
                    for r in pinned if r["name"] in {
                        "01-MARKET_SOURCE_OF_TRUTH.md", "03-MARKET_RULES.json",
                        "10-MARKET_RUNTIME_CONFIG.json", "MARKET_BUNDLE_MANIFEST.json"}}
        if len(required) != 4 or observed != required:
            raise ReleaseError("CANONICAL_READ_RELEASE_MISMATCH")
    args = ["--rules", home / "policy/03-MARKET_RULES.json", "--evidence", evidence_file]
    if test_now:
        args += ["--test-now", test_now]
    code, result = run_json(home / "engine/scripts/evaluate_candidate.py", args)
    result["execution_surface"] = "PORTABLE_RELEASE_RUNNER"
    result["cloud_environment_verified"] = False
    return code, result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--home", type=Path, default=DEFAULT_HOME)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("verify")
    sub.add_parser("regression")
    imp = sub.add_parser("import-bundle")
    imp.add_argument("archive", type=Path)
    ev = sub.add_parser("evaluate")
    ev.add_argument("--evidence", required=True, type=Path)
    ev.add_argument("--read-trace", type=Path)
    ev.add_argument("--test-now")
    norm = sub.add_parser("normalize")
    norm.add_argument("--input", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        lock = release_lock()
        home = args.home.absolute()
        if args.command == "import-bundle":
            emit(import_bundle(args.archive, home, lock))
            return 0
        if args.command == "evaluate":
            code, result = evaluate(home, lock, args.evidence, args.read_trace, args.test_now)
        else:
            result = validate_release(home, lock)
            code = 0
            if args.command == "regression":
                code, result = run_json(home / "engine/scripts/run_full_regression.py",
                                        ["--root", home / "policy", "--skill-root", home / "engine"],
                                        timeout=360)
            elif args.command == "normalize":
                code, result = run_json(home / "engine/scripts/normalize_ibkr_evidence.py",
                                        ["--input", args.input, "--rules", home / "policy/03-MARKET_RULES.json"])
        emit(result)
        return code
    except (OSError, ValueError, TypeError, KeyError, zipfile.BadZipFile,
            subprocess.SubprocessError) as exc:
        emit({"status": "CONFIG_CONFLICT", "error": str(exc), "orders_allowed": False,
              "recorded_at": datetime.now(timezone.utc).isoformat(),
              "cloud_environment_verified": False})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
