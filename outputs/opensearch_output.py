#!/opt/phishfindr/venv/bin/python
import logging
#from opensearch-py import OpenSearch
from opensearchpy import OpenSearch
from datetime import datetime


class OpenSearchOutput:
    def __init__(
        self,
        hosts=["http://localhost:9200"],
        index_prefix="o365-events",
        username=None,
        password=None,
        use_ssl=False,
        verify_certs=True,
    ):
        self.index_prefix = index_prefix

        auth = (username, password) if username and password else None

        self.client = OpenSearch(
            hosts=hosts,
            http_auth=auth,
            use_ssl=use_ssl,
            verify_certs=verify_certs,
        )

        logging.info(f"OpenSearchOutput initialized for {hosts} with prefix {index_prefix}")

    def _get_index_name(self):
        """Daily index rotation, e.g. o365-events-2025.09.09"""
        today = datetime.utcnow().strftime("%Y.%m.%d")
        return f"{self.index_prefix}-{today}"

    def write(self, event):
        """Write a single event to OpenSearch."""
        index_name = self._get_index_name()

        try:
            resp = self.client.index(index=index_name, body=event)
            logging.debug(f"Indexed event into {index_name}, result={resp.get('result')}")
        except Exception as e:
            logging.exception(f"Failed to index event into OpenSearch: {e}")
