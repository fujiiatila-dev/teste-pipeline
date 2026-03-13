import requests
import re
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class VindiConnector(BaseConnector):
    """Conector para Vindi (billing/assinaturas). Auth: Basic Auth. Paginação: Link header cursor."""

    ENDPOINTS = {
        "vindi_bills": "bills",
        "vindi_subscriptions": "subscriptions",
        "vindi_customers": "customers",
    }

    def __init__(self, api_token: str = None, api_url: str = None):
        self.api_token = api_token or settings.vindi_api_token
        self.api_url = (api_url or settings.vindi_api_url or "https://app.vindi.com.br/api/v1").rstrip("/")

    def _auth(self):
        return (self.api_token, "")

    @staticmethod
    def _flatten_json(record: dict, prefix: str = "") -> dict:
        flat = {}
        for key, value in record.items():
            new_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                flat.update(VindiConnector._flatten_json(value, new_key))
            elif isinstance(value, list):
                flat[new_key] = str(value)
            else:
                flat[new_key] = value
        return flat

    def _parse_link_header(self, link_header: str) -> dict:
        links = {}
        if not link_header:
            return links
        for part in link_header.split(","):
            match = re.match(r'<([^>]+)>;\s*rel="([^"]+)"', part.strip())
            if match:
                links[match.group(2)] = match.group(1)
        return links

    def _fetch_all(self, endpoint: str) -> list:
        all_items = []
        seen_ids = set()
        url = f"{self.api_url}/{endpoint}"
        params = {"per_page": 50}

        while url:
            try:
                resp = requests.get(url, auth=self._auth(), params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Vindi] Erro {endpoint}: {e}")
                break

            data = resp.json()
            items = data.get(endpoint, data.get("data", []))
            if not items:
                break

            for item in items:
                flat = self._flatten_json(item) if isinstance(item, dict) else item
                item_id = flat.get("id", id(flat))
                if item_id not in seen_ids:
                    seen_ids.add(item_id)
                    all_items.append(flat)

            links = self._parse_link_header(resp.headers.get("Link", ""))
            url = links.get("next")
            params = {}  # URL already contains params
            time.sleep(0.5)

        logging.info(f"[Vindi] {endpoint}: {len(all_items)} registros únicos")
        return all_items

    def get_tables_ddl(self) -> list:
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String, data String,
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id
            """ for t in self.ENDPOINTS]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        for table_name, endpoint in self.ENDPOINTS.items():
            items = self._fetch_all(endpoint)
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
