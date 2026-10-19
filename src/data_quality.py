"""In-Stream Data Quality & Schema Contract Validator."""
from typing import Any, Tuple, List


class StreamDataQualityValidator:
    REQUIRED_FIELDS = ["event_id", "event_type", "source_system", "payload", "timestamp_ms"]
    STRING_FIELDS = ("event_id", "event_type", "source_system")

    @classmethod
    def validate_event(cls, event: Any) -> Tuple[bool, List[str]]:
        """Validate a decoded JSON value without aborting a batch on non-objects."""
        if not isinstance(event, dict):
            return False, ["Event must be a JSON object"]

        errors = []
        for field in cls.REQUIRED_FIELDS:
            if field not in event or event[field] is None:
                errors.append("Missing required field: " + field)

        for field in cls.STRING_FIELDS:
            value = event.get(field)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                errors.append(field + " must be a non-empty string")

        timestamp = event.get("timestamp_ms")
        # bool is an int subclass in Python, but not a JSON integer timestamp.
        if timestamp is not None and (
            type(timestamp) is not int or timestamp < 0
        ):
            errors.append("timestamp_ms must be a non-negative integer")

        if "is_deleted" in event and not isinstance(event["is_deleted"], bool):
            errors.append("is_deleted must be a boolean")

        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            errors.append("Payload must be a JSON object")

        return len(errors) == 0, errors
