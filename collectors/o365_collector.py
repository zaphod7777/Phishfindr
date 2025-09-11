#!/opt/phishfindr/venv/bin/python
import os
import time
import json
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

class O365Collector:
    def __init__(self, interval=300):
        self.interval = interval
        self.tenant_id = os.getenv("O365_TENANT_ID")
        self.client_id = os.getenv("O365_CLIENT_ID")
        self.client_secret = os.getenv("O365_CLIENT_SECRET")
        self.scope = "https://manage.office.com/.default"
        self.base_url = f"https://manage.office.com/api/v1.0/{self.tenant_id}/activity/feed"
        self.subscriptions = ["Audit.AzureActiveDirectory", "Audit.Exchange"]
        
    def get_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope,
        }
        resp = requests.post(url, data=data)
        resp.raise_for_status()
        return resp.json()["access_token"]

    def ensure_subscription(self, content_type, headers):
        url = f"{self.base_url}/subscriptions/start?contentType={content_type}"
        resp = requests.post(url, headers=headers)
        if not resp.ok and resp.status_code != 409:
            logging.error(
                f"Failed to ensure subscription for {content_type}: {resp.text}"
            )

    def get_content_blobs(self, content_type, headers):
        url = f"{self.base_url}/subscriptions/content?contentType={content_type}"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def collect(self):
        token = self.get_token()
        headers = {"Authorization": f"Bearer {token}"}

        for content_type in self.subscriptions:
            try:
                self.ensure_subscription(content_type, headers)
            except Exception as e:
                logging.error(f"Failed to ensure subscription for {content_type}: {e}")
                continue

            blobs = self.get_content_blobs(content_type, headers)
            for blob in blobs:
                url = blob.get("contentUri")
                logging.info(f"Processing {content_type} blob {url}")

                try:
                    resp = requests.get(url, headers=headers)
                    resp.raise_for_status()
                    events = resp.json()

                    if isinstance(events, list):
                        logging.info(
                            f"Fetched {len(events)} events from {content_type} blob {url}"
                        )
                        for ev in events:
                            yield ev
                    else:
                        logging.warning(
                            f"Unexpected response format from {url}: {type(events)}"
                        )

                except Exception as e:
                    logging.error(f"Failed to fetch blob {url}: {e}")

    def run_forever(self):
        logging.info(f"Starting O365 monitor (polling every {self.interval} sec)")
        while True:
            try:
                for event in self.collect():
                    yield event
            except Exception as e:
                logging.error(f"Unhandled error in poll loop: {e}")
            time.sleep(self.interval)
