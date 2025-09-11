#!/opt/phishfindr/venv/bin/python
import json
import logging

from datetime import datetime, timezone

def normalize_event(event: dict) -> dict:
    """
    Normalize a raw event:
    - Adds a timestamp in ISO8601 UTC format
    - Maps 'Operation' -> 'operation'
    - Maps 'UserId' -> 'user'
    """
    normalized = dict(event)  # shallow copy

    # Add timestamp if missing
    if "timestamp" not in normalized:
        normalized["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

    # Normalize keys
    if "Operation" in normalized:
        normalized["operation"] = normalized.pop("Operation")
    if "UserId" in normalized:
        normalized["user"] = normalized.pop("UserId")

    return normalized
