import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class MauticConnector(BaseConnector):
    """Conector para Mautic (marketing automation). Auth: OAuth2 client_credentials. Paginação: offset-based."""

    ENDPOINTS = {
        "mautic_contacts": {"path": "api/contacts", "data_key": "contacts", "paginated": True, "dict_to_list": True},
        "mautic_segments": {"path": "api/segments", "data_key": "lists", "paginated": False, "dict_to_list": True},
        "mautic_campaigns": {"path": "api/campaigns", "data_key": "campaigns", "paginated": False, "dict_to_list": True},
    }

    def __init__(self, base_url: str = None, client_id: str = None, client_secret: str = None):
        self.base_url = (base_url or settings.mautic_base_url or "").rstrip("/")
        self.client_id = client_id or settings.mautic_client_id
        self.client_secret = client_secret or settings.mautic_client_secret
        self.token = None

    def _authenticate(self):
        url = f"{self.base_url}/oauth/v2/token"
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = requests.post(url, data=payload, timeout=60)
        resp.raise_for_status()
        self.token = resp.json().get("access_token")
        logging.info("[Mautic] Autenticado via OAuth2.")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _fetch_paginated(self, path: str, data_key: str, dict_to_list: bool) -> list:
        all_items = []
        start = 0
        limit = 100
        while True:
            url = f"{self.base_url}/{path}"
            params = {"start": start, "limit": limit, "orderBy": "id"}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Mautic] Erro {path} start={start}: {e}")
                break
            data = resp.json()
            items = data.get(data_key, {})
            if dict_to_list and isinstance(items, dict):
                items = list(items.values())
            if not items:
                break
            all_items.extend(items)
            total = data.get("total", 0)
            if isinstance(total, str):
                total = int(total)
            if start + limit >= total:
                break
            start += limit
            time.sleep(0.2)
        logging.info(f"[Mautic] {path}: {len(all_items)} registros")
        return all_items

    def _fetch_simple(self, path: str, data_key: str, dict_to_list: bool) -> list:
        url = f"{self.base_url}/{path}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            items = data.get(data_key, {})
            if dict_to_list and isinstance(items, dict):
                items = list(items.values())
            return items if isinstance(items, list) else []
        except Exception as e:
            logging.error(f"[Mautic] Erro {path}: {e}")
            return []

    def get_tables_ddl(self) -> list:
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String, data String,
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id
            """ for t in self.ENDPOINTS]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        if not self.token:
            self._authenticate()
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            if cfg["paginated"]:
                items = self._fetch_paginated(cfg["path"], cfg["data_key"], cfg["dict_to_list"])
            else:
                items = self._fetch_simple(cfg["path"], cfg["data_key"], cfg["dict_to_list"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
