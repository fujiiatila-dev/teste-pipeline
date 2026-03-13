import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class AsaasConnector(BaseConnector):
    """
    Conector para extração de dados da API Asaas (pagamentos, clientes, assinaturas).
    Auth: access_token header. Paginação: offset-based (limit=100, hasMore flag).
    """

    ENDPOINTS = {
        "asaas_payments": "v3/payments",
        "asaas_customers": "v3/customers",
        "asaas_subscriptions": "v3/subscriptions",
    }

    DEPENDENT_ENDPOINTS = {
        "asaas_subscription_payments": {
            "endpoint": "v3/subscriptions/{id}/payments",
            "parent": "asaas_subscriptions",
            "id_field": "id",
        }
    }

    def __init__(self, api_url: str = None, access_token: str = None):
        self.api_url = (api_url or settings.asaas_api_url or "https://api.asaas.com").rstrip("/")
        self.access_token = access_token or settings.asaas_access_token

    def _headers(self):
        return {
            "access_token": self.access_token,
            "Content-Type": "application/json",
        }

    def _fetch_all_pages(self, endpoint: str) -> list:
        """Busca todas as páginas de um endpoint via offset pagination."""
        all_items = []
        offset = 0
        limit = 100

        while True:
            url = f"{self.api_url}/{endpoint}"
            params = {"limit": limit, "offset": offset}

            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao buscar {endpoint} offset={offset}: {e}")
                break

            data = resp.json()
            items = data.get("data", [])
            all_items.extend(items)

            has_more = data.get("hasMore", False)
            if not has_more or not items:
                break

            offset += limit
            time.sleep(0.2)

        logging.info(f"[Asaas] {endpoint}: {len(all_items)} registros")
        return all_items

    def _fetch_dependent(self, parent_items: list, endpoint_template: str, id_field: str) -> list:
        """Busca dados de endpoint dependente (ex: pagamentos de cada assinatura)."""
        all_items = []
        max_ids = 100

        for item in parent_items[:max_ids]:
            parent_id = item.get(id_field)
            if not parent_id:
                continue

            endpoint = endpoint_template.replace("{id}", str(parent_id))
            try:
                items = self._fetch_all_pages(endpoint)
                for i in items:
                    i["subscription_id"] = parent_id
                all_items.extend(items)
            except Exception as e:
                logging.error(f"[Asaas] Erro no dependente {endpoint}: {e}")

            time.sleep(0.3)

        logging.info(f"[Asaas] Dependente: {len(all_items)} registros de {min(len(parent_items), max_ids)} pais")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS asaas_payments (
                id String,
                customer String,
                value Float64,
                netValue Float64,
                status String,
                billingType String,
                dueDate String,
                paymentDate Nullable(String),
                description Nullable(String),
                externalReference Nullable(String),
                invoiceUrl Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS asaas_customers (
                id String,
                name String,
                email Nullable(String),
                cpfCnpj Nullable(String),
                phone Nullable(String),
                mobilePhone Nullable(String),
                company Nullable(String),
                city Nullable(String),
                state Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS asaas_subscriptions (
                id String,
                customer String,
                value Float64,
                cycle String,
                status String,
                nextDueDate Nullable(String),
                description Nullable(String),
                billingType String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS asaas_subscription_payments (
                id String,
                subscription_id String,
                customer String,
                value Float64,
                status String,
                dueDate String,
                paymentDate Nullable(String),
                billingType String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (subscription_id, id)
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        parent_data = {}

        # Endpoints primários
        for table_name, endpoint in self.ENDPOINTS.items():
            items = self._fetch_all_pages(endpoint)
            parent_data[table_name] = items
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()

        # Endpoints dependentes
        for table_name, config in self.DEPENDENT_ENDPOINTS.items():
            parent_items = parent_data.get(config["parent"], [])
            items = self._fetch_dependent(parent_items, config["endpoint"], config["id_field"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()

        return results
