import requests
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector
from config.settings import settings


class HotmartConnector(BaseConnector):
    """
    Conector para extração de dados da API Hotmart (produtos digitais).
    Auth: OAuth 2.0 (client_credentials). Paginação: token-based (page_token).
    """

    BASE_URL = "https://developers.hotmart.com"
    AUTH_URL = "https://api-sec-vlc.hotmart.com/security/oauth/token"

    TRANSACTION_STATUSES = [
        "APPROVED", "BLOCKED", "CANCELLED", "CHARGEBACK", "COMPLETE",
        "EXPIRED", "NO_FUNDS", "OVERDUE", "PARTIALLY_REFUNDED",
        "PRE_ORDER", "PRINTED_BILLET", "PROCESSING_TRANSACTION",
        "PROTESTED", "REFUNDED", "STARTED", "UNDER_ANALISYS", "WAITING_PAYMENT",
    ]

    def __init__(self, basic_auth: str = None, days_to_fetch: int = None):
        self.basic_auth = basic_auth or settings.hotmart_basic_auth
        self.days_to_fetch = days_to_fetch or settings.hotmart_days_to_fetch
        self._token = None

    def _get_token(self) -> str:
        if self._token:
            return self._token

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {self.basic_auth}",
        }
        resp = requests.post(self.AUTH_URL, headers=headers, params={"grant_type": "client_credentials"})
        resp.raise_for_status()
        self._token = resp.json()["access_token"]
        return self._token

    def _auth_headers(self):
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def _get_date_range(self):
        end = datetime.now()
        start = end - timedelta(days=self.days_to_fetch)
        return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    @staticmethod
    def _flatten_json(nested: dict, prefix: str = "") -> dict:
        flat = {}
        for key, value in nested.items():
            new_key = f"{prefix}_{key}" if prefix else key
            if isinstance(value, dict):
                flat.update(HotmartConnector._flatten_json(value, new_key))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        flat.update(HotmartConnector._flatten_json(item, f"{new_key}_{i}"))
                    else:
                        flat[f"{new_key}_{i}"] = item
            else:
                flat[new_key] = value
        return flat

    @staticmethod
    def _convert_dates(df: pd.DataFrame) -> pd.DataFrame:
        date_indicators = ["date", "_at", "time"]
        date_cols = [
            col for col in df.columns
            if any(ind in col.lower() for ind in date_indicators) and df[col].dtype in ["int64", "float64"]
        ]
        for col in date_cols:
            df[f"{col}_readable"] = df[col].apply(
                lambda x: datetime.fromtimestamp(x / 1000).strftime("%Y-%m-%d %H:%M:%S")
                if pd.notnull(x) and x > 0 else None
            )
        return df

    def _fetch_paginated(self, endpoint: str, params: dict = None) -> list:
        """Busca todas as páginas usando page_token."""
        all_items = []
        next_page = None
        url = f"{self.BASE_URL}/{endpoint}"
        base_params = params.copy() if params else {}

        while True:
            req_params = base_params.copy()
            if next_page:
                req_params["page_token"] = next_page

            for retry in range(5):
                try:
                    resp = requests.get(url, headers=self._auth_headers(), params=req_params, timeout=30)

                    if resp.status_code == 429:
                        wait = 60 * (retry + 1)
                        logging.warning(f"[Hotmart] Rate limit - aguardando {wait}s")
                        time.sleep(wait)
                        continue

                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if retry < 4:
                        time.sleep(10 * (2 ** retry))
                        continue
                    logging.error(f"[Hotmart] Falha apos 5 tentativas: {e}")
                    return all_items

            data = resp.json()
            items = data.get("items", [])
            if not items:
                break

            all_items.extend(items)

            page_info = data.get("page_info", {})
            next_page = page_info.get("next_page_token")
            if not next_page:
                break

        logging.info(f"[Hotmart] {endpoint}: {len(all_items)} registros")
        return all_items

    def _extract_sales_nested(self, sales: list) -> list:
        """Extrai campos aninhados de vendas para formato flat."""
        flat_data = []
        for item in sales:
            flat = {k: v for k, v in item.items() if not isinstance(v, (dict, list))}

            for obj_name in ["product", "buyer", "producer"]:
                if obj_name in item and isinstance(item[obj_name], dict):
                    for k, v in item[obj_name].items():
                        flat[f"{obj_name}_{k}"] = v

            if "purchase" in item and isinstance(item["purchase"], dict):
                purchase = item["purchase"]
                for k, v in purchase.items():
                    if not isinstance(v, dict):
                        flat[f"purchase_{k}"] = v

                for nested_name, prefix in [
                    ("price", "purchase_price_"),
                    ("payment", "purchase_payment_"),
                    ("tracking", "purchase_tracking_"),
                    ("offer", "purchase_offer_"),
                    ("hotmart_fee", "purchase_hotmart_fee_"),
                ]:
                    if nested_name in purchase and isinstance(purchase[nested_name], dict):
                        for k, v in purchase[nested_name].items():
                            flat[f"{prefix}{k}"] = v

            flat_data.append(flat)
        return flat_data

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS hotmart_products (
                id String,
                name Nullable(String),
                status Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS hotmart_sales (
                transaction Nullable(String),
                product_id Nullable(String),
                product_name Nullable(String),
                buyer_name Nullable(String),
                buyer_email Nullable(String),
                purchase_status Nullable(String),
                purchase_price_value Float64 DEFAULT 0,
                purchase_order_date Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY transaction
            """,
            """
            CREATE TABLE IF NOT EXISTS hotmart_subscriptions (
                subscriber_code Nullable(String),
                plan_name Nullable(String),
                status Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY subscriber_code
            """,
            """
            CREATE TABLE IF NOT EXISTS hotmart_sales_summary (
                total_items Nullable(Int64),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY updated_at
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        start_ts, end_ts = self._get_date_range()
        status_param = ",".join(self.TRANSACTION_STATUSES)

        # Products
        products_raw = self._fetch_paginated(
            "products/api/v1/products", {"max_results": 100}
        )
        products = [self._flatten_json(p) for p in products_raw]
        df_products = pd.DataFrame(products)
        if not df_products.empty:
            df_products = self._convert_dates(df_products)

        # Sales
        sales_raw = self._fetch_paginated(
            "payments/api/v1/sales/history",
            {"max_results": 500, "start_date": start_ts, "end_date": end_ts, "transaction_status": status_param},
        )
        sales_flat = self._extract_sales_nested(sales_raw)
        df_sales = pd.DataFrame(sales_flat)
        if not df_sales.empty:
            df_sales = self._convert_dates(df_sales)

        # Subscriptions
        subs_raw = self._fetch_paginated(
            "payments/api/v1/subscriptions",
            {"max_results": 500, "status": "ACTIVE,CANCELLED,DELAYED,INACTIVE,OVERDUE"},
        )
        subs = [self._flatten_json(s) for s in subs_raw]
        df_subs = pd.DataFrame(subs)
        if not df_subs.empty:
            df_subs = self._convert_dates(df_subs)

        # Sales Summary (single call, no pagination)
        try:
            resp = requests.get(
                f"{self.BASE_URL}/payments/api/v1/sales/summary",
                headers=self._auth_headers(), timeout=30,
            )
            resp.raise_for_status()
            df_summary = pd.DataFrame([resp.json()])
        except Exception as e:
            logging.error(f"[Hotmart] Erro sales summary: {e}")
            df_summary = pd.DataFrame()

        return {
            "hotmart_products": df_products,
            "hotmart_sales": df_sales,
            "hotmart_subscriptions": df_subs,
            "hotmart_sales_summary": df_summary,
        }
