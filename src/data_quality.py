"""In-Stream Data Quality & Schema Contract Validator."""
from typing import Dict, Any, Tuple, List

class StreamDataQualityValidator:
    REQUIRED_FIELDS = ["event_id", "event_type", "source_system", "payload", "timestamp_ms"]

    @classmethod
    def validate_event(cls, event: Dict[str, Any]) -> Tuple[bool, List[str]]:
        errors = []
        for field in cls.REQUIRED_FIELDS:
            if field not in event or event[field] is None:
                errors.append("Missing required field: " + field)

        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            errors.append("Payload must be a JSON object")

        return len(errors) == 0, errors
