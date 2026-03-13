import requests
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector
from config.settings import settings


class ArboConnector(BaseConnector):
    """Conector para ArboCRM (leads e imóveis). Auth: Bearer Token. Duas APIs separadas."""

    def __init__(self, token_leads: str = None, token_imoveis: str = None,
                 api_url_leads: str = None, api_url_imoveis: str = None):
        self.token_leads = token_leads or settings.arbo_token_leads
        self.token_imoveis = token_imoveis or settings.arbo_token_imoveis
        self.api_url_leads = (api_url_leads or settings.arbo_api_url_leads or "").rstrip("/")
        self.api_url_imoveis = (api_url_imoveis or settings.arbo_api_url_imoveis or "").rstrip("/")

    def _headers(self, token: str):
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _fetch_pages(self, base_url: str, token: str, params: dict = None) -> list:
        all_items = []
        page = 1
        extra_params = params or {}
        while True:
            req_params = {"page": page, "perPage": 500, **extra_params}
            try:
                resp = requests.get(base_url, headers=self._headers(token), params=req_params, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Arbo] Erro page={page}: {e}")
                break
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break
            all_items.extend(items)
            last_page = data.get("lastPage", 1)
            if page >= last_page:
                break
            page += 1
            time.sleep(0.5)
        logging.info(f"[Arbo] {len(all_items)} registros em {page} páginas")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS arbo_leads (
                id String,
                name Nullable(String),
                email Nullable(String),
                phone Nullable(String),
                status Nullable(String),
                source Nullable(String),
                created_at Nullable(String),
                updated_at_source Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS arbo_imoveis (
                id String,
                title Nullable(String),
                type Nullable(String),
                status Nullable(String),
                address Nullable(String),
                city Nullable(String),
                state Nullable(String),
                price Nullable(Float64),
                area Nullable(Float64),
                bedrooms Nullable(Int32),
                created_at Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        # Leads (sem filtro de data)
        if self.api_url_leads and self.token_leads:
            items = self._fetch_pages(self.api_url_leads, self.token_leads)
            results["arbo_leads"] = pd.DataFrame(items) if items else pd.DataFrame()
        else:
            results["arbo_leads"] = pd.DataFrame()

        # Imóveis (com filtro de data 365 dias)
        if self.api_url_imoveis and self.token_imoveis:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=365)
            params = {"startDate": start_date.isoformat(), "endDate": end_date.isoformat()}
            items = self._fetch_pages(self.api_url_imoveis, self.token_imoveis, params)
            results["arbo_imoveis"] = pd.DataFrame(items) if items else pd.DataFrame()
        else:
            results["arbo_imoveis"] = pd.DataFrame()

        return results
