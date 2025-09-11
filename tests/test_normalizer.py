#!/opt/phishfindr/venv/bin/python
from phishfindr.utils.normalizer import normalize_event


def test_normalizer_adds_standard_fields():
    raw_event = {"UserId": "carol", "Operation": "UserLoggedIn"}
    normalized = normalize_event(raw_event)

    assert "timestamp" in normalized
    assert normalized["operation"] == "UserLoggedIn"
    assert normalized["user"] == "carol"
