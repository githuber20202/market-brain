import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import tomllib
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("market_cloud_launcher", ROOT / "cloud/market_core.py")
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = {"engine/scripts/example.py": b"pass\n", "policy/rules.json": b"{}\n"}
        self.lock = {"resource_revision": "TEST", "files": {
            k: hashlib.sha256(v).hexdigest() for k, v in self.data.items()}}

    def archive(self, data=None):
        path = self.root / "input.zip"
        with zipfile.ZipFile(path, "w") as z:
            for key, value in (self.data if data is None else data).items():
                z.writestr(key, value)
        return path

    def test_complete_import_preserves_bytes(self):
        home = self.root / "release"
        result = launcher.import_bundle(self.archive(), home, self.lock)
        self.assertEqual(result["status"], "IMPORTED")
        self.assertFalse(result["cloud_environment_verified"])
        launcher.verify_bytes(home, self.lock)

    def test_corrupt_import_has_no_partial_destination(self):
        data = {**self.data, "policy/rules.json": b"modified"}
        home = self.root / "release"
        with self.assertRaises(launcher.ReleaseError):
            launcher.import_bundle(self.archive(data), home, self.lock)
        self.assertFalse(home.exists())

    def test_traversal_and_extra_files_rejected(self):
        home = self.root / "release"
        with self.assertRaises(launcher.ReleaseError):
            launcher.import_bundle(self.archive({**self.data, "../escape": b"x"}), home, self.lock)
        self.assertFalse((self.root / "escape").exists())
        self.assertFalse(home.exists())

    def test_existing_release_never_overwritten(self):
        home = self.root / "release"
        home.mkdir()
        sentinel = home / "keep"
        sentinel.write_text("preserve")
        with self.assertRaises(launcher.ReleaseError):
            launcher.import_bundle(self.archive(), home, self.lock)
        self.assertEqual(sentinel.read_text(), "preserve")

    def test_duplicate_entries_rejected(self):
        archive = self.archive()
        with zipfile.ZipFile(archive, "a") as z:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                z.writestr("policy/rules.json", b"{}\n")
        with self.assertRaises(launcher.ReleaseError):
            launcher.import_bundle(archive, self.root / "release", self.lock)

    def test_local_tampering_and_unpinned_code_rejected(self):
        home = self.root / "release"
        launcher.import_bundle(self.archive(), home, self.lock)
        (home / "engine/extra.py").write_text("raise Exception('bad')")
        with self.assertRaises(launcher.ReleaseError):
            launcher.verify_bytes(home, self.lock)
        (home / "engine/extra.py").unlink()
        (home / "policy/rules.json").write_text("changed")
        with self.assertRaises(launcher.ReleaseError):
            launcher.verify_bytes(home, self.lock)

    def test_symlink_rejected(self):
        home = self.root / "release"
        launcher.import_bundle(self.archive(), home, self.lock)
        path = home / "policy/rules.json"
        path.unlink()
        target = self.root / "outside"
        target.write_bytes(self.data["policy/rules.json"])
        path.symlink_to(target)
        with self.assertRaises(launcher.ReleaseError):
            launcher.verify_bytes(home, self.lock)

    def test_live_inputs_require_read_trace_and_real_clock(self):
        evidence = self.root / "evidence.json"
        evidence.write_text(json.dumps({"synthetic": False}))
        with patch.object(launcher, "validate_release"), patch.object(launcher, "run_json") as run:
            with self.assertRaisesRegex(launcher.ReleaseError, "READ_TRACE_REQUIRED"):
                launcher.evaluate(self.root, self.lock, evidence)
            with self.assertRaisesRegex(launcher.ReleaseError, "TEST_CLOCK"):
                launcher.evaluate(self.root, self.lock, evidence, test_now="2020-01-01T00:00:00Z")
            run.assert_not_called()

    def test_agent_files_have_real_roles_and_inherit_model(self):
        agents = list((ROOT / ".codex/agents").glob("*.toml"))
        self.assertEqual(len(agents), 4)
        names = set()
        for path in agents:
            role = tomllib.loads(path.read_text())
            names.add(role["name"])
            self.assertTrue(role["description"] and role["developer_instructions"])
            self.assertEqual(role["sandbox_mode"], "read-only")
            self.assertNotIn("model", role)
        self.assertEqual(len(names), 4)


if __name__ == "__main__":
    unittest.main()
