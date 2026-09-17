import pytest
from src.data_quality import StreamDataQualityValidator
from src.cdc_producer import generate_cdc_event

def test_valid_event():
    ev = generate_cdc_event("ord_101")
    is_valid, errors = StreamDataQualityValidator.validate_event(ev)
    assert is_valid is True
    assert len(errors) == 0

def test_invalid_event():
    ev = {"event_id": "bad_event"}
    is_valid, errors = StreamDataQualityValidator.validate_event(ev)
    assert is_valid is False
    assert any("Missing required field" in err for err in errors)
