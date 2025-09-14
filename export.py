#!/opt/phishfindr/venv/bin/python
from opensearchpy import OpenSearch

# Initialize OpenSearch client
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=('<username>', '<password>'),
    use_ssl=True,
    verify_certs=True,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
)

index_name = "phishfindr-events-2025.09.13"
output_file = "exported_data.json"

# Initiate a scroll search
response = client.search(
    index=index_name,
    scroll='2m',  # Keep the scroll context alive for 2 minutes
    size=1000,     # Retrieve 1000 documents per scroll request
    body={
        "query": {
            "match_all": {}
        }
    }
)

scroll_id = response['_scroll_id']
with open(output_file, 'w') as f:
    while len(response['hits']['hits']) > 0:
        for hit in response['hits']['hits']:
            f.write(json.dumps(hit['_source']) + '\n')  # Write each document as a JSON line

        # Continue scrolling
        response = client.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = response['_scroll_id']

# Clear the scroll context
client.clear_scroll(scroll_id=scroll_id)