import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings

class NativeConnector(BaseConnector):
    def __init__(self, api_url=None, username=None, password=None, report_ids=None):
        self.api_url = (api_url or settings.native_api_url or "https://expoconecta.native-infinity.com.br").rstrip("/")
        self.username = username or settings.native_username
        self.password = password or settings.native_password
        self.report_ids = report_ids or settings.native_report_ids or ""
        self.token = None

    def _authenticate(self):
        url = f"{self.api_url}/api/token"
        payload = {"username": self.username, "password": self.password}
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        self.token = resp.json().get("token") or resp.json().get("access_token")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _fetch_report(self, report_id):
        url = f"{self.api_url}/api/apiReport/{report_id}"
        resp = requests.get(url, headers=self._headers(), timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data

    def get_tables_ddl(self):
        return [
            """
            CREATE TABLE IF NOT EXISTS native_reports (
                report_id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY report_id
            """
        ]

    def extract(self, date_start, date_stop):
        if not self.token:
            self._authenticate()

        ids = [rid.strip() for rid in self.report_ids.split(",") if rid.strip()]
        all_rows = []

        for rid in ids:
            try:
                items = self._fetch_report(rid)
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            item["report_id"] = rid
                    all_rows.extend(items)
                elif isinstance(items, dict):
                    items["report_id"] = rid
                    all_rows.append(items)
                logging.info(f"[Native] Report {rid}: {len(items) if isinstance(items, list) else 1} registros")
            except Exception as e:
                logging.error(f"[Native] Erro no report {rid}: {e}")
            time.sleep(0.5)

        df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        return {"native_reports": df}
