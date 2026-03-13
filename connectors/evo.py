import requests
import base64
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class EvoConnector(BaseConnector):
    """Conector para EVO (W12App - academias/fitness). Auth: Basic Auth. Mix de v1/v2 APIs."""

    ENDPOINTS = {
        "evo_activities": {"path": "api/v1/activities", "paginated": False},
        "evo_partnership": {"path": "api/v1/partnership", "paginated": False},
        "evo_prospects": {"path": "api/v1/prospects", "paginated": False},
        "evo_service": {"path": "api/v1/service", "paginated": False},
        "evo_employees": {"path": "api/v2/employees", "paginated": False},
        "evo_members": {"path": "api/v2/members", "paginated": False},
        "evo_sales": {"path": "api/v2/sales", "paginated": False},
        "evo_membership": {"path": "api/v2/membership", "paginated": True},
    }

    def __init__(self, username: str = None, password: str = None, api_url: str = None):
        self.username = username or settings.evo_username
        self.password = password or settings.evo_password
        self.api_url = (api_url or settings.evo_api_url or "https://evo-integracao-api.w12app.com.br").rstrip("/")

    def _headers(self):
        credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return {"Authorization": f"Basic {credentials}", "Content-Type": "application/json"}

    def _fetch_simple(self, path: str) -> list:
        url = f"{self.api_url}/{path}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("data", data.get("list", [data]))
            return []
        except Exception as e:
            logging.error(f"[Evo] Erro {path}: {e}")
            return []

    def _fetch_paginated(self, path: str) -> list:
        all_items = []
        offset = 0
        take = 50
        while True:
            url = f"{self.api_url}/{path}"
            params = {"skip": offset, "take": take}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Evo] Erro {path} offset={offset}: {e}")
                break
            data = resp.json()
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("data", data.get("list", []))
            else:
                break
            if not items:
                break
            all_items.extend(items)
            if len(items) < take:
                break
            offset += take
            time.sleep(0.2)
        logging.info(f"[Evo] {path}: {len(all_items)} registros")
        return all_items

    def get_tables_ddl(self) -> list:
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """ for t in self.ENDPOINTS]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            if cfg["paginated"]:
                items = self._fetch_paginated(cfg["path"])
            else:
                items = self._fetch_simple(cfg["path"])
            logging.info(f"[Evo] {table_name}: {len(items)} registros")
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
