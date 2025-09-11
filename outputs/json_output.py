#!/opt/phishfindr/venv/bin/python
import os
import json
import logging
from datetime import datetime
from pathlib import Path


class JSONOutput:
    def __init__(self, out_dir="events"):
        self.out_dir = Path(out_dir)
        self.out_dir.mkdir(parents=True, exist_ok=True)
        logging.debug(f"JSONOutput initialized with out_dir={self.out_dir}")

    def _write(self, event):
        """Internal write helper — appends one event to a JSON file."""
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
        out_file = self.out_dir / f"{ts}.json"

        try:
            with open(out_file, "a", encoding="utf-8") as f:
                json.dump(event, f, ensure_ascii=False)
                f.write("\n")
            logging.debug(f"Wrote event to {out_file}")
        except Exception as e:
            logging.exception(f"Failed to write event: {e}")

    def write_event(self, event):
        """Primary API for writing a single event."""
        self._write(event)

    def write(self, event):
        """Alias for write_event (for consistency across outputs)."""
        self.write_event(event)
