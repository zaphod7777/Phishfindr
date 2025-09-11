#!/opt/phishfindr/venv/bin/python
import os
import json
import tempfile

from phishfindr.outputs.json_output import JSONOutput


def test_json_output_writes_json(tmp_path):
    output_file = tmp_path / "events.json"
    output = JSONOutput(str(output_file))

    event = {"Operation": "UserLoggedIn", "UserId": "bob"}
    output.write_event(event)

    lines = output_file.read_text().splitlines()
    assert len(lines) == 1
    assert '"UserId": "bob"' in lines[0]


