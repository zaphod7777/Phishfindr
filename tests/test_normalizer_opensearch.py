#!/opt/phishfindr/venv/bin/python
# tests/test_normalizer_opensearch.py

import pytest
from utils.normalizer import normalize_event


@pytest.fixture
def sample_opensearch_doc():
    return {
        "CreationTime": "2025-09-10T15:12:02",
        "Operation": "UserLoggedIn",
        "ResultStatus": "Success",
        "UserId": "asdf@asdf.com",
        "UserType": 0,
        "Workload": "AzureActiveDirectory",
        "ClientIP": "192.168.1.1",
        "ActorIpAddress": "192.168.1.1",
        "RecordType": 15,
        "Version": 1,
        "ApplicationId": "app-12345",
        "ErrorNumber": "0",
        "ExtendedProperties": [
            {"Name": "ResultStatusDetail", "Value": "Redirect"},
            {"Name": "UserAgent", "Value": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            {"Name": "RequestType", "Value": "OAuth2:Authorize"},
        ],
        "DeviceProperties": [
            {"Name": "OS", "Value": "Windows10"},
            {"Name": "BrowserType", "Value": "Firefox"},
            {"Name": "SessionId", "Value": "session-abc123"},
        ],
    }


def test_normalize_event_flattens_fields(sample_opensearch_doc):
    normalized = normalize_event(sample_opensearch_doc)

    # Basic required fields
    assert normalized["timestamp"] == "2025-09-10T15:12:02"
    assert normalized["event_type"] == "UserLoggedIn"
    assert normalized["status"] == "Success"
    assert normalized["status_detail"] == "Redirect"
    assert normalized["user_id"] == "asdf@asdf.com"
    assert normalized["user_type"] == 0
    assert normalized["ip_address"] == "192.168.1.1"
    assert normalized["workload"] == "AzureActiveDirectory"

    # Extended & Device properties
    assert normalized["user_agent"].startswith("Mozilla/")
    assert normalized["request_type"] == "OAuth2:Authorize"
    assert normalized["os"] == "Windows10"
    assert normalized["browser"] == "Firefox"
    assert normalized["session_id"] == "session-abc123"

    # Misc fields
    assert normalized["application_id"] == "app-12345"
    assert normalized["record_type"] == 15
    assert normalized["version"] == 1
    assert normalized["error_number"] == "0"
