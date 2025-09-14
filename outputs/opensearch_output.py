#!/opt/phishfindr/venv/bin/python
# outputs/opensearch_output.py
#import logging
#from opensearch-py import OpenSearch

import psycopg2
from datetime import datetime
from opensearchpy import OpenSearch, helpers
from psycopg2.extras import execute_values

class PostgresOutput:
    def __init__(self, dsn):
        self.conn = psycopg2.connect(dsn)
        self.cur = self.conn.cursor()

    def write(self, event):
        query = """
        INSERT INTO phishfindr_events (
            id, creation_time, operation, result_status, result_status_detail,
            user_id, client_ip, user_agent, request_type, os, browser,
            session_id, application_id, record_type, version, error_number
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING;
        """
        values = [(event.get(k) for k in [
            "id", "creation_time", "operation", "result_status", "result_status_detail",
            "user_id", "client_ip", "user_agent", "request_type", "os", "browser",
            "session_id", "application_id", "record_type", "version", "error_number"
        ])]
        execute_values(self.cur, query, values)
        self.conn.commit()



class OpenSearchOutput:
    def __init__(self, host="localhost", port=9200, username=None, password=None, use_ssl=False):
        auth = (username, password) if username and password else None
        self.client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=auth,
            use_ssl=use_ssl,
            verify_certs=False
        )

    def _get_index_name(self):
        today = datetime.utcnow().strftime("%Y.%m.%d")
        return f"phishfindr-events-{today}"

    def _ensure_index(self, index_name):
        if not self.client.indices.exists(index=index_name):
            mapping = {
                "mappings": {
                    "_source": {"enabled": True},
                    "properties": {
                        "timestamp": {"type": "date"},
                        "event_type": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "status_detail": {"type": "keyword"},
                        "user_id": {"type": "keyword"},
                        "user_type": {"type": "integer"},
                        "ip_address": {"type": "ip"},
                        "workload": {"type": "keyword"},
                        "user_agent": {"type": "text"},
                        "request_type": {"type": "keyword"},
                        "os": {"type": "keyword"},
                        "browser": {"type": "keyword"},
                        "session_id": {"type": "keyword"},
                        "application_id": {"type": "keyword"},
                        "record_type": {"type": "integer"},
                        "version": {"type": "integer"},
                        "error_number": {"type": "keyword"},
                    }
                }
            }
            self.client.indices.create(index=index_name, body=mapping)

    def write_events(self, events):
        index_name = self._get_index_name()
        self._ensure_index(index_name)

        actions = [
            {"_index": index_name, "_source": event}
            for event in events
        ]
        helpers.bulk(self.client, actions)
