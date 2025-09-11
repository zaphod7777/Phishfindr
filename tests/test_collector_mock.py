#!/opt/phishfindr/venv/bin/python
from phishfindr.outputs.json_output import JSONOutput


def test_json_output_write(tmp_path):
    output_file = tmp_path / "output.json"
    output = JSONOutput(str(output_file))

    event = {"UserId": "alice", "Operation": "LoginSuccess"}
    output.write_event(event)

    contents = output_file.read_text().strip().splitlines()
    assert len(contents) == 1
    assert '"UserId": "alice"' in contents[0]
