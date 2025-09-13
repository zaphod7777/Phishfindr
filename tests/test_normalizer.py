#!/opt/phishfindr/venv/bin/python
# utils/normalizer.py
# tests/test_normalizer.py
from utils.normalizer import normalize_event

def test_normalizer_adds_standard_fields():
    raw_event = {"UserId": "carol", "Operation": "UserLoggedIn"}
    normalized = normalize_event(raw_event)

    # Check that normalization maps fields correctly
    assert "event_type" in normalized
    assert "user_id" in normalized
    assert normalized["event_type"] == "UserLoggedIn"
    assert normalized["user_id"] == "carol"
