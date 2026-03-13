import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class EduzzConnector(BaseConnector):
    """
    Conector para extração de dados da API Eduzz (produtos digitais).
    Auth: Bearer token. Paginação: page-based.
    """

    ENDPOINTS = {
        "eduzz_customers": {
            "path": "v1/customers",
            "params": {"itemsPerPage": 1000},
        },
        "eduzz_subscriptions_creation": {
            "path": "v1/subscriptions",
            "params": {"filterBy": "creation"},
            "use_dates": True,
        },
        "eduzz_subscriptions_update": {
            "path": "v1/subscriptions",
            "params": {"filterBy": "update"},
            "use_dates": True,
        },
        "eduzz_sales": {
            "path": "v1/sales",
            "params": {"itemsPerPage": 100},
            "use_dates": True,
        },
    }

    def __init__(self, api_url: str = None, auth_token: str = None):
        self.api_url = (api_url or settings.eduzz_api_url or "https://api2.eduzz.com").rstrip("/")
        self.auth_token = auth_token or settings.eduzz_auth_token

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
        }

    def _fetch_all_pages(self, endpoint: str, extra_params: dict = None) -> list:
        """Busca todas as páginas de um endpoint via page-based pagination."""
        all_items = []
        page = 1
        params = extra_params.copy() if extra_params else {}

        while True:
            params["page"] = page
            url = f"{self.api_url}/{endpoint}"

            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao buscar {endpoint} page={page}: {e}")
                break

            data = resp.json()
            items = data.get("items", [])
            all_items.extend(items)

            total_pages = data.get("pages", 0)
            if page >= total_pages or not items:
                break

            page += 1
            time.sleep(0.2)

        logging.info(f"[Eduzz] {endpoint}: {len(all_items)} registros em {page} paginas")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS eduzz_customers (
                id String,
                name Nullable(String),
                email Nullable(String),
                phone Nullable(String),
                document Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS eduzz_subscriptions_creation (
                id String,
                plan_name Nullable(String),
                status Nullable(String),
                created_at Nullable(String),
                updated_at_field Nullable(String),
                value Float64 DEFAULT 0,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS eduzz_subscriptions_update (
                id String,
                plan_name Nullable(String),
                status Nullable(String),
                created_at Nullable(String),
                updated_at_field Nullable(String),
                value Float64 DEFAULT 0,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS eduzz_sales (
                id String,
                product_name Nullable(String),
                sale_status Nullable(String),
                sale_value Float64 DEFAULT 0,
                date_create Nullable(String),
                client_name Nullable(String),
                client_email Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}

        for table_name, config in self.ENDPOINTS.items():
            params = config.get("params", {}).copy()

            if config.get("use_dates"):
                params["startDate"] = date_start.strftime("%Y-%m-%d")
                params["endDate"] = date_stop.strftime("%Y-%m-%d")

            items = self._fetch_all_pages(config["path"], extra_params=params)
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()

        return results
