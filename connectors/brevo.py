import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class BrevoConnector(BaseConnector):
    """
    Conector para Brevo (ex-Sendinblue). Email e SMS campaigns.
    Auth: api-key header. Paginação: offset-based.
    """

    ENDPOINTS = {
        "brevo_email_campaigns": {"path": "emailCampaigns", "data_key": "campaigns"},
        "brevo_sms_campaigns": {"path": "smsCampaigns", "data_key": "campaigns"},
    }

    def __init__(self, api_key: str = None):
        self.api_key = api_key or settings.brevo_api_key
        self.base_url = "https://api.brevo.com/v3"

    def _headers(self):
        return {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _fetch_all_pages(self, path: str, data_key: str) -> list:
        all_items = []
        offset = 0
        limit = 100

        while True:
            url = f"{self.base_url}/{path}"
            params = {"limit": limit, "offset": offset, "sort": "desc"}

            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Brevo] Erro ao buscar {path} offset={offset}: {e}")
                break

            data = resp.json()
            items = data.get(data_key, [])
            if not items:
                break

            all_items.extend(items)
            offset += limit
            time.sleep(0.5)

        logging.info(f"[Brevo] {path}: {len(all_items)} registros")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS brevo_email_campaigns (
                id UInt64,
                name String,
                subject Nullable(String),
                type String,
                status String,
                scheduledAt Nullable(String),
                createdAt String,
                modifiedAt String,
                sentDate Nullable(String),
                tag Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS brevo_sms_campaigns (
                id UInt64,
                name String,
                status String,
                content Nullable(String),
                scheduledAt Nullable(String),
                createdAt String,
                modifiedAt String,
                sender Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            items = self._fetch_all_pages(cfg["path"], cfg["data_key"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
