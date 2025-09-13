#!/opt/phishfindr/venv/bin/python
# utils/normalizer.py
import json
import logging

from datetime import datetime, timezone
from typing import Any, Dict, List


def _get_extended_property(props: List[Dict[str, Any]], name: str) -> Any:
    """Helper to fetch a value from ExtendedProperties list by name."""
    for p in props or []:
        if p.get("Name") == name:
            return p.get("Value")
    return None


def _get_device_property(props: List[Dict[str, Any]], name: str) -> Any:
    """Helper to fetch a value from DeviceProperties list by name."""
    for p in props or []:
        if p.get("Name") == name:
            return p.get("Value")
    return None


def normalize_event(source: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a raw Office 365 / OpenSearch audit log _source document
    into a flat, pipeline-friendly dict.
    """
    extended = source.get("ExtendedProperties", [])
    device = source.get("DeviceProperties", [])

    normalized = {
        "timestamp": source.get("CreationTime"),
        "event_type": source.get("Operation"),
        "status": source.get("ResultStatus"),
        "status_detail": _get_extended_property(extended, "ResultStatusDetail"),
        "user_id": source.get("UserId"),
        "user_type": source.get("UserType"),
        "ip_address": source.get("ActorIpAddress") or source.get("ClientIP"),
        "workload": source.get("Workload"),
        "user_agent": _get_extended_property(extended, "UserAgent"),
        "request_type": _get_extended_property(extended, "RequestType"),
        "os": _get_device_property(device, "OS"),
        "browser": _get_device_property(device, "BrowserType"),
        "session_id": _get_device_property(device, "SessionId"),
        "application_id": source.get("ApplicationId"),
        "record_type": source.get("RecordType"),
        "version": source.get("Version"),
        "error_number": source.get("ErrorNumber"),
    }

    # Drop None values to keep JSON clean
    return {k: v for k, v in normalized.items() if v is not None}
