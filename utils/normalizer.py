#!/opt/phishfindr/venv/bin/python

# phishfindr/utils/normalizer.py
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union


def _get_extended_value(extended: Union[dict, Iterable], key: str) -> Optional[Any]:
    """Return value for key from ExtendedProperties (handles dict or list-of-dicts)."""
    if isinstance(extended, dict):
        return extended.get(key)
    if isinstance(extended, list):
        for item in extended:
            # item might be {"Name": "...", "Value": "..."} or similar
            if not isinstance(item, dict):
                continue
            if item.get("Name") == key:
                return item.get("Value")
            # fallback: direct key in dict
            if key in item:
                return item.get(key)
    return None


def _get_device_property(device_props: Union[dict, Iterable], key: str) -> Optional[Any]:
    """Return device property value from DeviceProperties (handles dict or list-of-dicts)."""
    if isinstance(device_props, dict):
        return device_props.get(key)
    if isinstance(device_props, list):
        for p in device_props:
            if not isinstance(p, dict):
                continue
            if p.get("Name") == key:
                return p.get("Value")
            if key in p:
                return p.get(key)
    return None


def normalize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a raw Office365/OpenSearch `_source` event into a flat dict.

    Produces keys like:
      - timestamp, event_type, status, status_detail, user_id, user_type,
        ip_address, workload, user_agent, request_type, os, browser, session_id,
        application_id, record_type, version, error_number, id, organization_id, ...
    """
    extended = event.get("ExtendedProperties", [])
    device = event.get("DeviceProperties", [])

    # timestamp: prefer CreationTime, then other common names, else now (UTC)
    timestamp = (
        event.get("CreationTime")
        or event.get("creation_time")
        or event.get("timestamp")
        or datetime.now(timezone.utc).isoformat()
    )

    # id/deduplication
    ev_id = event.get("Id") or event.get("id") or str(uuid.uuid4())

    # top-level mappings with fallbacks to alternate field names
    event_type = event.get("Operation") or event.get("operation")
    status = event.get("ResultStatus") or event.get("status")
    status_detail = (
        event.get("ResultStatusDetail")
        or event.get("status_detail")
        or _get_extended_value(extended, "ResultStatusDetail")
    )

    user_id = event.get("UserId") or event.get("user_id")
    user_type = event.get("UserType")
    if user_type is None:
        # sometimes stored as lower-case key
        user_type = event.get("user_type")

    organization_id = event.get("OrganizationId") or event.get("organization_id")
    user_key = event.get("UserKey") or event.get("user_key")

    # ip selection: prefer actor ip, then client ip, then any normalized keys
    ip_address = (
        event.get("ActorIpAddress")
        or event.get("ActorIp")
        or event.get("ClientIP")
        or event.get("client_ip")
        or event.get("ip_address")
    )

    workload = event.get("Workload") or event.get("workload")
    application_id = event.get("ApplicationId") or event.get("application_id")
    record_type = event.get("RecordType") or event.get("record_type")
    version = event.get("Version") or event.get("version")
    error_number = event.get("ErrorNumber") or event.get("error_number")

    # extended/device values
    user_agent = event.get("UserAgent") or _get_extended_value(extended, "UserAgent")
    request_type = event.get("RequestType") or _get_extended_value(extended, "RequestType")
    os_val = _get_device_property(device, "OS") or _get_device_property(device, "os")
    browser = _get_device_property(device, "BrowserType") or _get_device_property(device, "browser")
    session_id = _get_device_property(device, "SessionId") or event.get("SessionId")

    normalized = {
        "id": str(ev_id),
        "timestamp": timestamp,
        "event_type": event_type,
        "operation": event_type,
        "status": status,
        "status_detail": status_detail,
        "user_id": user_id,
        "user_key": user_key,
        "user_type": user_type,
        "organization_id": organization_id,
        "ip_address": ip_address,
        "workload": workload,
        "user_agent": user_agent,
        "request_type": request_type,
        "os": os_val,
        "browser": browser,
        "session_id": session_id,
        "application_id": application_id,
        "record_type": record_type,
        "version": version,
        "error_number": error_number,
    }

    # drop None values so storage is clean
    return {k: v for k, v in normalized.items() if v is not None}
