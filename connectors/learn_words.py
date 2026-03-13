import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class LearnWordsConnector(BaseConnector):
    """Conector para Learn Words / MevBrasil. Auth: OAuth2 client_credentials. Endpoints dependentes."""

    ENDPOINTS = {
        "learn_words_users": "v2/users",
        "learn_words_courses": "v2/courses",
        "learn_words_user_subscriptions": "v2/user-subscriptions",
    }

    DEPENDENT_ENDPOINTS = {
        "learn_words_user_progress": {"endpoint": "v2/users/{id}/progress", "parent": "learn_words_users", "id_field": "id", "paginated": False},
        "learn_words_user_courses": {"endpoint": "v2/users/{id}/courses", "parent": "learn_words_users", "id_field": "id", "paginated": True},
        "learn_words_course_users": {"endpoint": "v2/courses/{id}/users", "parent": "learn_words_courses", "id_field": "id", "paginated": True},
        "learn_words_course_analytics": {"endpoint": "v2/courses/{id}/analytics", "parent": "learn_words_courses", "id_field": "id", "paginated": False},
    }

    MAX_IDS = 100

    def __init__(self, base_url: str = None, client_id: str = None, client_secret: str = None):
        self.base_url = (base_url or settings.learn_words_base_url or "").rstrip("/")
        self.client_id = client_id or settings.learn_words_client_id
        self.client_secret = client_secret or settings.learn_words_client_secret
        self.token = None

    def _authenticate(self):
        url = f"{self.base_url}/oauth2/access_token"
        payload = {"grant_type": "client_credentials", "client_id": self.client_id, "client_secret": self.client_secret}
        resp = requests.post(url, data=payload, timeout=60)
        resp.raise_for_status()
        self.token = resp.json().get("access_token")
        logging.info("[LearnWords] Autenticado via OAuth2.")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json", "Accept-Encoding": "gzip, deflate"}

    def _fetch_paginated(self, endpoint: str) -> list:
        all_items = []
        page = 1
        while True:
            url = f"{self.base_url}/{endpoint}"
            params = {"items_per_page": 200, "page": page}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[LearnWords] Erro {endpoint} page={page}: {e}")
                break
            data = resp.json()
            items = data.get("data", data) if isinstance(data, dict) else data
            if not isinstance(items, list) or not items:
                break
            all_items.extend(items)
            meta = data.get("meta", {}) if isinstance(data, dict) else {}
            total_pages = meta.get("totalPages", 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.2)
        logging.info(f"[LearnWords] {endpoint}: {len(all_items)} registros")
        return all_items

    def _fetch_simple(self, url: str) -> list:
        try:
            resp = requests.get(url, headers=self._headers(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("data", [data])
            return []
        except Exception as e:
            logging.error(f"[LearnWords] Erro: {e}")
            return []

    def _fetch_dependent(self, parent_items: list, endpoint_template: str, id_field: str, paginated: bool) -> list:
        all_items = []
        for item in parent_items[:self.MAX_IDS]:
            parent_id = item.get(id_field)
            if not parent_id:
                continue
            endpoint = endpoint_template.replace("{id}", str(parent_id))
            if paginated:
                items = self._fetch_paginated(endpoint)
            else:
                url = f"{self.base_url}/{endpoint}"
                items = self._fetch_simple(url)
            for i in items:
                if isinstance(i, dict):
                    i["parent_id"] = parent_id
            all_items.extend(items)
            time.sleep(0.3)
        logging.info(f"[LearnWords] Dependente: {len(all_items)} de {min(len(parent_items), self.MAX_IDS)} pais")
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
        if not self.token:
            self._authenticate()
        results = {}
        parent_data = {}
        for table_name, endpoint in self.ENDPOINTS.items():
            items = self._fetch_paginated(endpoint)
            parent_data[table_name] = items
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        for table_name, cfg in self.DEPENDENT_ENDPOINTS.items():
            parent_items = parent_data.get(cfg["parent"], [])
            items = self._fetch_dependent(parent_items, cfg["endpoint"], cfg["id_field"], cfg["paginated"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
