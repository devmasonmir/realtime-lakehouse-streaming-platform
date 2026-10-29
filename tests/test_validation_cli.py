"""Subprocess tests for the dependency-free batch validation command."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ValidationCliTests(unittest.TestCase):
    def run_cli(self, path):
        return subprocess.run(
            [sys.executable, "-m", "src.validate_events", str(path)],
            cwd=ROOT, capture_output=True, text=True, timeout=10,
        )

    def run_document(self, text):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic_events.json"
            path.write_text(text, encoding="utf-8")
            result = self.run_cli(path)
            self.assertEqual(path.read_text(encoding="utf-8"), text)
            return result

    def test_repository_sample_is_valid_and_stdout_is_json(self):
        path = ROOT / "sample_data" / "events.json"
        result = self.run_cli(path)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {
            "valid_events": json.loads(path.read_text(encoding="utf-8")),
            "quarantined": [],
        })

    def test_mixed_batch_reports_original_records_and_errors(self):
        event = {
            "event_id": "synthetic_1", "event_type": "CREATE",
            "source_system": "synthetic_source", "payload": {"label": "café"},
            "timestamp_ms": 0,
        }
        invalid = {**event, "timestamp_ms": True}
        result = self.run_document(json.dumps([None, event, invalid, event]))
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(json.loads(result.stdout), {
            "valid_events": [event, event],
            "quarantined": [
                {"event": None, "errors": ["Event must be a JSON object"]},
                {"event": invalid, "errors": [
                    "timestamp_ms must be a non-negative integer",
                ]},
            ],
        })

    def test_duplicate_event_ids_are_reported(self):
        event = {
            "event_id": "synthetic_1", "event_type": "CREATE",
            "source_system": "synthetic_source", "payload": {},
            "timestamp_ms": 0,
        }
        duplicate = {**event, "payload": {"sequence": 2}}

        result = self.run_document(json.dumps([event, duplicate]))

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(json.loads(result.stdout), {
            "valid_events": [event],
            "quarantined": [{
                "event": duplicate,
                "errors": ["Duplicate event_id in batch"],
            }],
        })

    def test_empty_batch_succeeds(self):
        result = self.run_document("[]")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {
            "valid_events": [], "quarantined": [],
        })

    def test_non_array_input_has_no_partial_report(self):
        for value in ({}, None, "synthetic", 42, True):
            with self.subTest(value=value):
                result = self.run_document(json.dumps(value))
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertIn("Event batch must be a JSON array", result.stderr)

    def test_malformed_json_has_no_traceback_or_partial_report(self):
        for text in ("", "[", "[{},]", "[] []"):
            with self.subTest(text=text):
                result = self.run_document(text)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertIn("Unable to read event batch", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_non_finite_json_constants_are_rejected(self):
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                result = self.run_document('[{"payload": {"amount": ' + value + '}}]')
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertIn("Non-finite JSON number", result.stderr)

    def test_missing_file_has_no_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_cli(Path(directory) / "missing.json")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("Unable to read event batch", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_float_overflow_is_rejected_before_writing_a_report(self):
        for value in ("1e999", "-1e999"):
            with self.subTest(value=value):
                result = self.run_document('[{"payload": {"amount": ' + value + '}}]')
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
                self.assertIn("Non-finite JSON number", result.stderr)
                self.assertNotIn("Traceback", result.stderr)

    def test_invalid_utf8_has_no_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "invalid.json"
            path.write_bytes(b"[\xff]")
            result = self.run_cli(path)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertIn("Unable to read event batch", result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
