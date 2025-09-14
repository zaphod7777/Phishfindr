#!/opt/phishfindr/venv/bin/python
# phishfindr/outputs/postgres_output.py
import os
import logging
import uuid
from typing import Dict, Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS phishfindr_events (
    id UUID PRIMARY KEY,
    creation_time TIMESTAMP,
    operation TEXT,
    organization_id UUID,
    record_type INT,
    result_status TEXT,
    user_key UUID,
    user_type INT,
    version INT,
    workload TEXT,
    client_ip INET,
    user_id TEXT,
    azure_event_type INT,
    result_status_detail TEXT,
    user_agent TEXT,
    request_type TEXT,
    os TEXT,
    browser TEXT,
    session_id UUID,
    application_id UUID,
    error_number TEXT
);
"""


class PostgresOutput:
    def __init__(self, dsn: Optional[str] = None):
        """
        dsn example: "dbname=phishfindr user=phishuser password=secret host=localhost port=5432"
        If dsn is None, it will be built from environment variables:
            POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT
        """
        if dsn is None:
            db = os.getenv("POSTGRES_DB", "phishfindr")
            user = os.getenv("POSTGRES_USER", "postgres")
            pwd = os.getenv("POSTGRES_PASSWORD", "")
            host = os.getenv("POSTGRES_HOST", "localhost")
            port = os.getenv("POSTGRES_PORT", "5432")
            dsn = f"dbname={db} user={user} password={pwd} host={host} port={port}"

        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self._ensure_table()

    def _ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            self.conn.commit()
        logging.info("Ensured phishfindr_events table exists in Postgres.")

    @staticmethod
    def _pick(*keys: str, event: Dict) -> Optional:
        """Return first present, non-None value for keys in event dict."""
        for k in keys:
            if k in event and event[k] is not None:
                return event[k]
        return None

    def _event_to_tuple(self, event: Dict) -> Tuple:
        """
        Convert a raw/normalized event dict into a tuple matching the table columns order:
        id, creation_time, operation, organization_id, record_type, result_status,
        user_key, user_type, version, workload, client_ip, user_id,
        azure_event_type, result_status_detail, user_agent, request_type, os,
        browser, session_id, application_id, error_number
        """
        # id: prefer explicit Id or id; else generate uuid4
        ev_id = self._pick("Id", "id", event=event) or str(uuid.uuid4())

        creation_time = self._pick("CreationTime", "creation_time", "timestamp", event=event)
        operation = self._pick("Operation", "operation", event=event)
        organization_id = self._pick("OrganizationId", "organization_id", event=event)
        record_type = self._pick("RecordType", "record_type", event=event)
        result_status = self._pick("ResultStatus", "status", "result_status", event=event)
        user_key = self._pick("UserKey", "user_key", event=event)
        user_type = self._pick("UserType", "user_type", event=event)
        version = self._pick("Version", "version", event=event)
        workload = self._pick("Workload", "workload", event=event)

        # IP: accept multiple possible keys
        client_ip = self._pick(
            "ActorIpAddress", "ActorIp", "ClientIP", "client_ip", "ip_address", event=event
        )

        user_id = self._pick("UserId", "user_id", event=event)
        azure_event_type = self._pick("AzureActiveDirectoryEventType", "azure_event_type", event=event)
        result_status_detail = self._pick("status_detail", "ResultStatusDetail", event=event)
        user_agent = self._pick("user_agent", "UserAgent", event=event)
        request_type = self._pick("request_type", "RequestType", event=event)
        os_field = self._pick("os", "OS", event=event)
        browser = self._pick("browser", "BrowserType", event=event)
        session_id = self._pick("session_id", "SessionId", event=event)
        application_id = self._pick("application_id", "ApplicationId", event=event)
        error_number = self._pick("error_number", "ErrorNumber", event=event)

        return (
            str(ev_id),
            creation_time,
            operation,
            organization_id,
            record_type,
            result_status,
            user_key,
            user_type,
            version,
            workload,
            client_ip,
            user_id,
            azure_event_type,
            result_status_detail,
            user_agent,
            request_type,
            os_field,
            browser,
            session_id,
            application_id,
            error_number,
        )

    def write(self, event: Dict):
        """Insert a single event (wraps write_events for convenience)."""
        self.write_events([event])

    def write_events(self, events: Iterable[Dict]):
        """
        Batch insert events using execute_values for performance.
        Uses ON CONFLICT (id) DO NOTHING to skip duplicates.
        """
        if not events:
            return {"inserted": 0, "skipped": 0}

        tuples = [self._event_to_tuple(e) for e in events]

        insert_sql = """
        INSERT INTO phishfindr_events (
            id, creation_time, operation, organization_id, record_type, result_status,
            user_key, user_type, version, workload, client_ip, user_id,
            azure_event_type, result_status_detail, user_agent, request_type, os, browser,
            session_id, application_id, error_number
        ) VALUES %s
        ON CONFLICT (id) DO NOTHING;
        """

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_sql, tuples)
            self.conn.commit()
            logging.debug(f"Inserted {len(tuples)} events into Postgres.")
            return {"inserted": len(tuples)}
        except Exception as e:
            logging.exception("Failed to write events to Postgres, rolling back.")
            self.conn.rollback()
            return {"error": str(e)}

    def close(self):
        try:
            self.conn.close()
        except Exception:
            logging.exception("Error closing Postgres connection.")
