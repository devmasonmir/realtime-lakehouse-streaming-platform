"""Dependency-free smoke tests for generated CDC events."""
import unittest

from src.data_quality import StreamDataQualityValidator
from src.cdc_producer import generate_cdc_event


class StreamingSmokeTests(unittest.TestCase):
    def test_generated_event_is_valid(self):
        event = generate_cdc_event("synthetic_order_101")

        is_valid, errors = StreamDataQualityValidator.validate_event(event)

        self.assertTrue(is_valid)
        self.assertEqual(errors, [])

    def test_incomplete_event_is_invalid(self):
        event = {"event_id": "synthetic_incomplete_event"}

        is_valid, errors = StreamDataQualityValidator.validate_event(event)

        self.assertFalse(is_valid)
        self.assertTrue(any(
            "Missing required field" in error for error in errors
        ))


if __name__ == "__main__":
    unittest.main()
