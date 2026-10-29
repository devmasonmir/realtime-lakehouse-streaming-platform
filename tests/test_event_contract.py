"""Dependency-free regression tests for the decoded JSON event contract."""
import copy
import unittest

from src.data_quality import StreamDataQualityValidator
from src.streaming_pipeline import process_event_batch


class EventContractTests(unittest.TestCase):
    def setUp(self):
        self.event = {
            "event_id": "synthetic_event_1",
            "event_type": "CREATE",
            "source_system": "synthetic_crm",
            "payload": {},
            "timestamp_ms": 0,
        }

    def test_string_fields_reject_wrong_types_and_blank_values(self):
        for field in ("event_id", "event_type", "source_system"):
            for value in (0, False, [], {}, "", " \t\n"):
                with self.subTest(field=field, value=value):
                    event = {**self.event, field: value}
                    self.assertEqual(
                        StreamDataQualityValidator.validate_event(event),
                        (False, [field + " must be a non-empty string"]),
                    )

    def test_timestamp_rejects_booleans_fractions_strings_and_negative_values(self):
        for value in (True, False, 1.5, 0.0, "123", -1, [], {}):
            with self.subTest(value=value):
                self.assertEqual(
                    StreamDataQualityValidator.validate_event(
                        {**self.event, "timestamp_ms": value}
                    ),
                    (False, ["timestamp_ms must be a non-negative integer"]),
                )

    def test_optional_delete_flag_requires_a_boolean(self):
        for value in (None, 0, 1, "false", [], {}):
            with self.subTest(value=value):
                self.assertEqual(
                    StreamDataQualityValidator.validate_event(
                        {**self.event, "is_deleted": value}
                    ),
                    (False, ["is_deleted must be a boolean"]),
                )

    def test_valid_boundaries_and_additional_fields_are_preserved(self):
        for timestamp in (0, 1720000000000):
            for flag in ({}, {"is_deleted": False}, {"is_deleted": True}):
                with self.subTest(timestamp=timestamp, flag=flag):
                    event = {
                        **self.event, "timestamp_ms": timestamp,
                        "metadata": {"synthetic": True}, **flag,
                    }
                    original = copy.deepcopy(event)
                    self.assertEqual(
                        StreamDataQualityValidator.validate_event(event),
                        (True, []),
                    )
                    self.assertEqual(event, original)

    def test_missing_and_null_fields_keep_existing_diagnostics(self):
        for field in ("event_id", "event_type", "source_system", "timestamp_ms"):
            for missing in (True, False):
                with self.subTest(field=field, missing=missing):
                    event = {**self.event, field: None}
                    if missing:
                        del event[field]
                    self.assertEqual(
                        StreamDataQualityValidator.validate_event(event),
                        (False, ["Missing required field: " + field]),
                    )

    def test_batch_quarantines_bad_types_and_continues(self):
        bad = {**self.event, "timestamp_ms": True, "event_type": " "}
        events = [bad, self.event]
        original = copy.deepcopy(events)

        valid, quarantined = process_event_batch(events)

        self.assertEqual(valid, [self.event])
        self.assertIs(valid[0], self.event)
        self.assertEqual(quarantined, [(bad, [
            "event_type must be a non-empty string",
            "timestamp_ms must be a non-negative integer",
        ])])
        self.assertIs(quarantined[0][0], bad)
        self.assertEqual(events, original)


    def test_duplicate_event_ids_quarantine_later_valid_records(self):
        first = {**self.event, "payload": {"sequence": 1}}
        duplicate = {**self.event, "payload": {"sequence": 2}}
        distinct = {**self.event, "event_id": "synthetic_event_2"}
        events = [first, duplicate, distinct]
        original = copy.deepcopy(events)

        valid, quarantined = process_event_batch(events)

        self.assertEqual(valid, [first, distinct])
        self.assertEqual(quarantined, [
            (duplicate, ["Duplicate event_id in batch"]),
        ])
        self.assertIs(valid[0], first)
        self.assertIs(quarantined[0][0], duplicate)
        self.assertEqual(events, original)

    def test_every_later_duplicate_is_quarantined_in_order(self):
        first = {**self.event, "payload": {"sequence": 1}}
        second = {**self.event, "payload": {"sequence": 2}}
        third = {**self.event, "payload": {"sequence": 3}}

        valid, quarantined = process_event_batch([first, second, third])

        self.assertEqual(valid, [first])
        self.assertEqual(quarantined, [
            (second, ["Duplicate event_id in batch"]),
            (third, ["Duplicate event_id in batch"]),
        ])

    def test_invalid_record_does_not_reserve_its_event_id(self):
        invalid = {**self.event, "payload": []}

        valid, quarantined = process_event_batch([invalid, self.event])

        self.assertEqual(valid, [self.event])
        self.assertEqual(quarantined, [
            (invalid, ["Payload must be a JSON object"]),
        ])


if __name__ == "__main__":
    unittest.main()
