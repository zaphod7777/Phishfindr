#!/opt/phishfindr/venv/bin/python
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

class JSONOutput:
    def __init__(self, filepath):
        self.filepath = filepath
        # Ensure parent directory exists
        parent_dir = os.path.dirname(self.filepath)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)

    def write_event(self, event):
        # Add a timestamp if not already present
        if "timestamp" not in event:
            event["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        # Append JSON line to file
        with open(self.filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

