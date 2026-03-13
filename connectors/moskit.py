import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class MoskitConnector(BaseConnector):
    """Conector para Moskit v2 (CRM). Auth: apikey header. Paginação: cursor-based via header."""

    ENDPOINTS = {
        "moskit_deals": "v2/deals",
        "moskit_custom_fields": "v2/customFields",
        "moskit_stages": "v2/stages",
        "moskit_pipelines": "v2/pipelines",
        "moskit_lost_reasons": "v2/lostReasons",
        "moskit_users": "v2/users",
    }

    DEPENDENT_ENDPOINTS = {
        "moskit_deal_custom_fields": {
            "endpoint": "v2/deals/{id}/customFields",
            "parent": "moskit_deals",
            "id_field": "id",
        }
    }

    def __init__(self, api_key: str = None, api_url: str = None):
        self.api_key = api_key or settings.moskit_api_key
        self.api_url = (api_url or settings.moskit_api_url or "https://api.ms.prod.moskit.services").rstrip("/")

    def _headers(self):
        return {"apikey": self.api_key, "Content-Type": "application/json"}

    def _fetch_cursor(self, endpoint: str) -> list:
        all_items = []
        next_token = None
        while True:
            url = f"{self.api_url}/{endpoint}"
            params = {"quantity": 50}
            if next_token:
                params["nextPageToken"] = next_token
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Moskit] Erro {endpoint}: {e}")
                break
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            if not items:
                break
            all_items.extend(items)
            next_token = resp.headers.get("X-Moskit-Listing-Next-Page-Token")
            if not next_token:
                break
            time.sleep(0.2)
        logging.info(f"[Moskit] {endpoint}: {len(all_items)} registros")
        return all_items

    def _fetch_dependent(self, parent_items: list, endpoint_template: str, id_field: str) -> list:
        all_items = []
        max_ids = 100
        for item in parent_items[:max_ids]:
            parent_id = item.get(id_field)
            if not parent_id:
                continue
            endpoint = endpoint_template.replace("{id}", str(parent_id))
            url = f"{self.api_url}/{endpoint}"
            try:
                resp = requests.get(url, headers=self._headers(), timeout=60)
                resp.raise_for_status()
                data = resp.json()
                items = data if isinstance(data, list) else data.get("data", [])
                for i in items:
                    i["deal_id"] = parent_id
                all_items.extend(items)
            except Exception as e:
                logging.error(f"[Moskit] Erro dependente {endpoint}: {e}")
            time.sleep(0.3)
        logging.info(f"[Moskit] Dependente: {len(all_items)} de {min(len(parent_items), max_ids)} pais")
        return all_items

    def get_tables_ddl(self) -> list:
        tables = list(self.ENDPOINTS.keys()) + list(self.DEPENDENT_ENDPOINTS.keys())
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String, data String,
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id
            """ for t in tables]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        parent_data = {}
        for table_name, endpoint in self.ENDPOINTS.items():
            items = self._fetch_cursor(endpoint)
            parent_data[table_name] = items
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        for table_name, cfg in self.DEPENDENT_ENDPOINTS.items():
            parent_items = parent_data.get(cfg["parent"], [])
            items = self._fetch_dependent(parent_items, cfg["endpoint"], cfg["id_field"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
