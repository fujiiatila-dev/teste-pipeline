import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class C2sConnector(BaseConnector):
    """Conector para Contact2Sale v2. Auth: Bearer Token. Rate limit: 10 req/min."""

    def __init__(self, api_url: str = None, token: str = None):
        self.api_url = (api_url or settings.c2s_api_url or "").rstrip("/")
        self.token = token or settings.c2s_token

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _fetch_paginated(self, path: str, extra_params: dict = None) -> list:
        all_items = []
        page = 1
        params = extra_params or {}
        while True:
            url = f"{self.api_url}/{path}"
            req_params = {"page": page, "perpage": 50, "sort": "-created_at", **params}
            try:
                resp = requests.get(url, headers=self._headers(), params=req_params, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[C2S] Erro {path} page={page}: {e}")
                break
            data = resp.json()
            items = data.get("data", data) if isinstance(data, dict) else data
            if isinstance(items, dict):
                items = [items]
            if not items or not isinstance(items, list):
                break
            all_items.extend(items)
            if len(items) < 50:
                break
            page += 1
            time.sleep(6)  # 10 req/min rate limit
        logging.info(f"[C2S] {path}: {len(all_items)} registros")
        return all_items

    def _fetch_companies(self) -> list:
        url = f"{self.api_url}/me"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            companies = []
            if isinstance(data, dict):
                main = {**data, "type": "main"}
                main.pop("sub_companies", None)
                companies.append(main)
                for sub in data.get("sub_companies", []):
                    sub["type"] = "sub"
                    companies.append(sub)
            return companies
        except Exception as e:
            logging.error(f"[C2S] Erro companies: {e}")
            return []

    def get_tables_ddl(self) -> list:
        return [
            """CREATE TABLE IF NOT EXISTS c2s_companies (
                id String, name Nullable(String), type Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id""",
            """CREATE TABLE IF NOT EXISTS c2s_leads (
                id String, name Nullable(String), email Nullable(String), phone Nullable(String),
                status Nullable(String), source Nullable(String), created_at Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id""",
            """CREATE TABLE IF NOT EXISTS c2s_sellers (
                id String, name Nullable(String), email Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id""",
            """CREATE TABLE IF NOT EXISTS c2s_tags (
                id String, name Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id""",
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        # Companies (non-paginated)
        companies = self._fetch_companies()
        results["c2s_companies"] = pd.DataFrame(companies) if companies else pd.DataFrame()
        time.sleep(6)

        # Leads (with date filter)
        year = datetime.now().year
        leads = self._fetch_paginated("leads", {"created_gte": f"{year}-01-01T00:00:00Z"})
        results["c2s_leads"] = pd.DataFrame(leads) if leads else pd.DataFrame()

        # Sellers (non-paginated via single page)
        sellers_resp = self._fetch_paginated("sellers")
        results["c2s_sellers"] = pd.DataFrame(sellers_resp) if sellers_resp else pd.DataFrame()

        # Tags
        tags = self._fetch_paginated("tags")
        results["c2s_tags"] = pd.DataFrame(tags) if tags else pd.DataFrame()

        return results
