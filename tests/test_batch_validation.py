"""Regression tests for isolating malformed JSON records in a batch."""
import copy
import unittest

from src.cdc_producer import generate_cdc_event
from src.data_quality import StreamDataQualityValidator
from src.streaming_pipeline import process_event_batch


class BatchValidationTests(unittest.TestCase):
    def test_non_object_json_values_are_rejected(self):
        for event in (None, [], ["event_id"], "event_id", 42, 1.5, True):
            with self.subTest(event=event):
                self.assertEqual(
                    StreamDataQualityValidator.validate_event(event),
                    (False, ["Event must be a JSON object"]),
                )

    def test_mixed_batch_preserves_records_and_continues_after_bad_input(self):
        first = generate_cdc_event("synthetic_order_1")
        last = generate_cdc_event("synthetic_order_2")
        incomplete = {"event_id": "incomplete"}
        events = [None, first, [], incomplete, "invalid", last]
        original = copy.deepcopy(events)

        valid, quarantined = process_event_batch(events)

        self.assertEqual(valid, [first, last])
        self.assertIs(valid[0], first)
        self.assertIs(valid[1], last)
        self.assertEqual([record for record, _ in quarantined],
                         [None, [], incomplete, "invalid"])
        self.assertIs(quarantined[2][0], incomplete)
        for index in (0, 1, 3):
            self.assertEqual(quarantined[index][1], ["Event must be a JSON object"])
        self.assertIn("Missing required field: payload", quarantined[2][1])
        self.assertEqual(events, original)

    def test_object_validation_still_checks_required_fields_and_payload(self):
        event = generate_cdc_event("synthetic_order_3")
        event["source_system"] = None
        event["payload"] = []
        self.assertEqual(
            StreamDataQualityValidator.validate_event(event),
            (False, ["Missing required field: source_system", "Payload must be a JSON object"]),
        )

    def test_all_invalid_and_empty_batches(self):
        self.assertEqual(process_event_batch([]), ([], []))
        valid, quarantined = process_event_batch([None, False])
        self.assertEqual(valid, [])
        self.assertEqual(len(quarantined), 2)


if __name__ == "__main__":
    unittest.main()
