#!/opt/phishfindr/venv/bin/python
import tempfile
import os
import json

from phishfindr.outputs.json_output import JSONOutput


def test_pipeline_event_roundtrip(tmp_path):
    output_file = tmp_path / "roundtrip.json"
    output = JSONOutput(str(output_file))

    event = {"Operation": "UserLoggedIn", "UserId": "dave"}
    output.write_event(event)

    data = output_file.read_text().strip()
    assert '"UserId": "dave"' in data
