#!/opt/phishfindr/venv/bin/python
import logging
#from opensearch-py import OpenSearch
from datetime import datetime
from opensearchpy import OpenSearch, helpers

BASE_INDEX_NAME = "phishfindr-events"

INDEX_MAPPING = {
    "mappings": {
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
            "error_number": {"type": "keyword"}
        }
    }
}

class OpenSearchOutput:
    def __init__(self, hosts=["http://localhost:9200"], base_index=BASE_INDEX_NAME):
        self.client = OpenSearch(hosts=hosts)
        self.base_index = base_index

    def _get_daily_index(self):
        """Generate today's index name (e.g. phishfindr-events-2025.09.11)."""
        date_str = datetime.utcnow().strftime("%Y.%m.%d")
        return f"{self.base_index}-{date_str}"

    def _ensure_index(self, index_name):
        """Check if index exists, create with mapping if missing."""
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name, body=INDEX_MAPPING)

    def write(self, event):
        """Insert a single normalized event (useful for debugging)."""
        index_name = self._get_daily_index()
        self._ensure_index(index_name)
        self.client.index(index=index_name, body=event)

    def bulk_write(self, events):
        """
        Insert multiple normalized events in one bulk request.
        Expects `events` to be a list of dicts.
        """
        index_name = self._get_daily_index()
        self._ensure_index(index_name)

        actions = [
            {"_index": index_name, "_source": event}
            for event in events
        ]

        success, failed = helpers.bulk(self.client, actions, stats_only=True)
        return {"success": success, "failed": failed}
