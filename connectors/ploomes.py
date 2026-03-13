import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class PloomesConnector(BaseConnector):
    """
    Conector para extração de dados da API Ploomes (CRM).
    Auth: User-Key header. Paginação: OData (@odata.nextLink).
    Base URL: https://api2.ploomes.com
    """

    BASE_URL = "https://api2.ploomes.com"

    ENDPOINTS = {
        "ploomes_contacts": "Contacts",
        "ploomes_deals": "Deals",
        "ploomes_deals_stages": "Deals@Stages",
        "ploomes_deals_pipelines": "Deals@Pipelines",
        "ploomes_deals_status": "Deals@Status",
        "ploomes_deals_loss_reasons": "Deals@LossReasons",
        "ploomes_orders": "Orders",
        "ploomes_products": "Products",
        "ploomes_products_families": "Products@Families",
        "ploomes_products_groups": "Products@Groups",
        "ploomes_users": "Users",
        "ploomes_teams": "Teams",
        "ploomes_tags": "Tags",
        "ploomes_contacts_origins": "Contacts@Origins",
        "ploomes_contacts_status": "Contacts@Status",
        "ploomes_contacts_types": "Contacts@Types",
    }

    def __init__(self, user_key: str = None):
        self.user_key = user_key or settings.ploomes_user_key

    def _headers(self):
        return {"User-Key": self.user_key}

    def _fetch_all_odata(self, endpoint_path: str) -> list:
        """Busca todos os dados de um endpoint com paginação OData."""
        all_data = []
        next_url = endpoint_path
        page = 0

        while next_url:
            page += 1
            url = f"{self.BASE_URL}/{next_url}" if not next_url.startswith("http") else next_url

            try:
                resp = requests.get(url, headers=self._headers(), timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao buscar {endpoint_path} page={page}: {e}")
                break

            data = resp.json()
            page_items = data.get("value", [])
            all_data.extend(page_items)

            # OData nextLink
            next_link = data.get("@odata.nextLink")
            if next_link:
                # Se a URL contém domínio completo, usar diretamente
                if next_link.startswith("https://"):
                    next_url = next_link
                else:
                    next_url = next_link
            else:
                next_url = None

            if next_url:
                time.sleep(0.1)

        logging.info(f"[Ploomes] {endpoint_path}: {len(all_data)} registros em {page} paginas")
        return all_data

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS ploomes_contacts (
                Id Int64,
                Name Nullable(String),
                Email Nullable(String),
                Phone Nullable(String),
                LegalName Nullable(String),
                Register Nullable(String),
                CityId Nullable(Int64),
                StateId Nullable(Int64),
                OriginId Nullable(Int64),
                StatusId Nullable(Int64),
                TypeId Nullable(Int64),
                OwnerId Nullable(Int64),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_deals (
                Id Int64,
                ContactId Nullable(Int64),
                Title Nullable(String),
                Amount Float64 DEFAULT 0,
                StageId Nullable(Int64),
                StatusId Nullable(Int64),
                PipelineId Nullable(Int64),
                OwnerId Nullable(Int64),
                LossReasonId Nullable(Int64),
                CreateDate Nullable(String),
                LastUpdateDate Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_deals_stages (
                Id Int64,
                Name Nullable(String),
                PipelineId Nullable(Int64),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_deals_pipelines (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_deals_status (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_deals_loss_reasons (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_orders (
                Id Int64,
                ContactId Nullable(Int64),
                DealId Nullable(Int64),
                Amount Float64 DEFAULT 0,
                StageId Nullable(Int64),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_products (
                Id Int64,
                Name Nullable(String),
                FamilyId Nullable(Int64),
                GroupId Nullable(Int64),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_products_families (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_products_groups (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_users (
                Id Int64,
                Name Nullable(String),
                Email Nullable(String),
                TeamId Nullable(Int64),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_teams (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_tags (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_contacts_origins (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_contacts_status (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
            """
            CREATE TABLE IF NOT EXISTS ploomes_contacts_types (
                Id Int64,
                Name Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY Id
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}

        for table_name, endpoint_path in self.ENDPOINTS.items():
            items = self._fetch_all_odata(endpoint_path)
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()

        return results
