import requests
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector
from config.settings import settings


class AcertConnector(BaseConnector):
    """Conector para Grupo ACERT (e-commerce). Auth: Bearer Token. Multi-store. Paginação: page-based (0-indexed)."""

    ENDPOINTS = {
        "acert_customers": "customers",
        "acert_products": "products",
        "acert_sales": "sales",
        "acert_payments": "payments",
        "acert_invoice_history": "invoice_history",
        "acert_types_of_sales": "types-of-sales",
        "acert_users": "users",
    }

    def __init__(self, token: str = None, api_url: str = None, store_ids: str = None):
        self.token = token or settings.acert_token
        self.api_url = (api_url or settings.acert_api_url or "https://stores.grupoacert.app/api/v1").rstrip("/")
        self.store_ids = [s.strip() for s in (store_ids or settings.acert_store_ids or "").split(",") if s.strip()]

    def _headers(self):
        return {"Authorization": self.token, "Content-Type": "application/json"}

    def _fetch_pages(self, url: str) -> list:
        all_items = []
        page = 0
        while True:
            params = {"page": page, "size": 200}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Acert] Erro page={page}: {e}")
                break
            data = resp.json()
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("content", data.get("data", []))
            else:
                break
            if not items:
                break
            all_items.extend(items)
            if isinstance(data, dict):
                total_pages = data.get("totalPages", 1)
                if page + 1 >= total_pages:
                    break
            elif len(items) < 200:
                break
            page += 1
            time.sleep(0.5)
        return all_items

    def _fetch_cash_flow(self, store_id: str, date_start: datetime, date_stop: datetime) -> list:
        """Busca cash_flow iterando dia a dia."""
        all_items = []
        current = date_start
        while current <= date_stop:
            url = f"{self.api_url}/stores/{store_id}/cash-flow"
            params = {"receiptDate": current.strftime("%Y-%m-%d")}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                items = data if isinstance(data, list) else data.get("content", data.get("data", []))
                if items:
                    for item in items:
                        item["store_id"] = store_id
                    all_items.extend(items)
            except Exception as e:
                logging.error(f"[Acert] cash_flow {current}: {e}")
            current += timedelta(days=1)
            time.sleep(0.3)
        return all_items

    def _extract_sale_items(self, sales: list) -> list:
        """Extrai items das vendas (relacional)."""
        all_items = []
        for sale in sales:
            sale_id = sale.get("id", "")
            items = sale.pop("items", []) or []
            for item in items:
                item["sale_id"] = sale_id
                all_items.append(item)
        return all_items

    def get_tables_ddl(self) -> list:
        tables = list(self.ENDPOINTS.keys()) + ["acert_cash_flow", "acert_sale_items"]
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """ for t in tables]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {k: pd.DataFrame() for k in list(self.ENDPOINTS.keys()) + ["acert_cash_flow", "acert_sale_items"]}

        for store_id in self.store_ids:
            logging.info(f"[Acert] Extraindo store {store_id}")

            for table_name, endpoint in self.ENDPOINTS.items():
                url = f"{self.api_url}/stores/{store_id}/{endpoint}"
                items = self._fetch_pages(url)
                for item in items:
                    item["store_id"] = store_id

                if table_name == "acert_sales" and items:
                    sale_items = self._extract_sale_items(items)
                    if sale_items:
                        df_items = pd.DataFrame(sale_items)
                        results["acert_sale_items"] = pd.concat([results["acert_sale_items"], df_items], ignore_index=True)

                if items:
                    df = pd.DataFrame(items)
                    results[table_name] = pd.concat([results[table_name], df], ignore_index=True)

            # Cash flow (últimos 6 meses)
            cf_start = datetime.now() - timedelta(days=180)
            cf_items = self._fetch_cash_flow(store_id, cf_start, date_stop)
            if cf_items:
                df_cf = pd.DataFrame(cf_items)
                results["acert_cash_flow"] = pd.concat([results["acert_cash_flow"], df_cf], ignore_index=True)

        return results
