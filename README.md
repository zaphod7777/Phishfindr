#  PhishFindr

PhishFindr helps monitor Office 365 activity logs for suspicious events 
(like unusual sign-ins or inbox rules being created) and stores them for 
analysis in JSON, OpenSearch, or PostgreSQL.

##  Features
- Collects Office 365 **Audit Logs** (Sign-ins, Exchange events, inbox rules)
- Stores data in:
  - JSON files (local archive)
  - OpenSearch (log analytics / dashboards)
  - PostgreSQL (easy SQL queries)
- Modular design → plug in new collectors or outputs easily
- `.env` file for secrets and configuration



```
phishfindr
├─ README.md
├─ __main__.py
├─ collectors
│  ├─ __init__.py
│  └─ o365_collector.py
├─ export.py
├─ outputs
│  ├─ __init__.py
│  ├─ json_output.py
│  ├─ opensearch_output.py
│  └─ postgres_output.py
├─ phishfindr
│  ├─ __init__.py
│  └─ pipeline.py
├─ pipeline.py
├─ pytest.ini
├─ requirements.txt
├─ run_pipeline.sh
├─ tests
│  ├─ __init__.py
│  ├─ test_collector_mock.py
│  ├─ test_json_output.py
│  ├─ test_normalizer.py
│  ├─ test_normalizer_opensearch.py
│  ├─ test_pipeline_once.py
│  └─ test_postgres_output.py
└─ utils
   ├─ __init__.py
   └─ normalizer.py

```
```
phishfindr
├─ README.md
├─ __main__.py
├─ collectors
│  ├─ __init__.py
│  └─ o365_collector.py
├─ export.py
├─ outputs
│  ├─ __init__.py
│  ├─ json_output.py
│  ├─ opensearch_output.py
│  └─ postgres_output.py
├─ phishfindr
│  ├─ __init__.py
│  └─ pipeline.py
├─ pipeline.py
├─ pytest.ini
├─ requirements.txt
├─ run_pipeline.sh
├─ tests
│  ├─ __init__.py
│  ├─ test_collector_mock.py
│  ├─ test_json_output.py
│  ├─ test_normalizer.py
│  ├─ test_normalizer_opensearch.py
│  ├─ test_pipeline_once.py
│  └─ test_postgres_output.py
└─ utils
   ├─ __init__.py
   └─ normalizer.py

```