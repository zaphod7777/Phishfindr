#!/opt/phishfindr/venv/bin/python
# tests/test_postgres_output.py
import pytest
from unittest.mock import patch, MagicMock
from phishfindr.outputs.postgres_output import PostgresOutput

@patch("phishfindr.outputs.postgres_output.psycopg2.connect")
@patch("phishfindr.outputs.postgres_output.execute_values")
def test_write_events_inserts(mock_execute_values, mock_connect):
    # Mock connection + cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Pretend execute_values succeeded
    mock_execute_values.return_value = None

    # Create PostgresOutput
    output = PostgresOutput(dsn="dbname=test user=test password=test host=localhost")

    # Minimal event
    event = {
        "Id": "123e4567-e89b-12d3-a456-426614174000",
        "Operation": "UserLoginFailed",
        "UserId": "alice",
    }

    result = output.write_events([event])

    assert result["inserted"] == 1
    mock_execute_values.assert_called_once()
