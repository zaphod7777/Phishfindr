#!/opt/phishfindr/venv/bin/python
import json
import logging

def normalize_event(event):
    """
    Ensure event is a dict.
    - If it's already a dict, return as-is.
    - If it's a JSON string, parse into dict.
    - If it's something else, wrap in a dict.
    """
    if isinstance(event, dict):
        return event

    if isinstance(event, str):
        try:
            return json.loads(event)
        except Exception:
            logging.warning(f"Event string not valid JSON, wrapping: {event}")
            return {"raw_event": event}

    # fallback
    logging.warning(f"Unexpected event type {type(event)}, wrapping")
    return {"raw_event": str(event)}
