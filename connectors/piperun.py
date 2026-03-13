import re
import time
import unicodedata
import logging
import requests
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class PiperunConnector(BaseConnector):
    """
    Conector para extração de dados da API Piperun (CRM/Pipeline).
    Auth: token header. Paginação: page-based (show=200, meta.links.next).
    """

    ENDPOINTS = {
        "piperun_deals": {"path": "deals", "with": "customFields,tags,owner,pipeline,stage"},
        "piperun_companies": {"path": "companies"},
        "piperun_activities": {"path": "activities"},
        "piperun_lost_reasons": {"path": "lostReasons"},
        "piperun_origins": {"path": "origins"},
    }

    NESTED_FIELDS = ["customFields", "owner", "pipeline", "stage", "tags"]

    def __init__(self, api_url: str = None, api_token: str = None):
        self.api_url = (api_url or settings.piperun_api_url or "https://api.pipe.run/v1").rstrip("/")
        self.api_token = api_token or settings.piperun_api_token

    def _headers(self):
        return {
            "accept": "application/json",
            "token": self.api_token,
        }

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_name = nfkd.encode("ASCII", "ignore").decode("ASCII")
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    @staticmethod
    def _flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(PiperunConnector._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _flatten_deal_nested(self, deals: list) -> list:
        """Aplica flatten de campos aninhados específicos de deals."""
        flat_deals = []
        for deal in deals:
            flat = {k: v for k, v in deal.items() if k not in self.NESTED_FIELDS}

            for field in self.NESTED_FIELDS:
                if field not in deal:
                    continue
                nested = deal[field]

                if field == "customFields" and isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, dict) and item.get("name"):
                            key = self._normalize_column_name(item["name"])
                            flat[key] = item.get("value")

                elif field == "owner" and isinstance(nested, dict):
                    flat["owner_name"] = nested.get("name")

                elif field == "tags" and isinstance(nested, list):
                    names = [t.get("name") for t in nested if isinstance(t, dict) and "name" in t]
                    flat["tags_name"] = "|".join(names)

                elif field == "pipeline" and isinstance(nested, dict):
                    flat["pipeline_name"] = nested.get("name")
                    flat["pipeline_funnel_type_name"] = nested.get("funnel_type_name")

                elif field == "stage" and isinstance(nested, dict):
                    flat["stage_name"] = nested.get("name")

            flat_deals.append(flat)
        return flat_deals

    def _fetch_all_pages(self, endpoint: str, extra_params: dict = None) -> list:
        """Busca todas as páginas via meta.links.next."""
        all_items = []
        params = {"show": 200, "page": 1}
        if extra_params:
            params.update(extra_params)

        while True:
            url = f"{self.api_url}/{endpoint}"
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)

                if resp.status_code == 429:
                    logging.warning("[Piperun] Rate limit - aguardando 30s")
                    time.sleep(30)
                    continue

                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao buscar {endpoint} page={params.get('page')}: {e}")
                break

            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            all_items.extend(items)

            has_next = (
                "meta" in data
                and "links" in data["meta"]
                and "next" in data["meta"]["links"]
            )
            if has_next:
                params["page"] += 1
            else:
                break

            time.sleep(0.5)

        logging.info(f"[Piperun] {endpoint}: {len(all_items)} registros")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS piperun_deals (
                id String,
                title Nullable(String),
                value Float64 DEFAULT 0,
                owner_name Nullable(String),
                pipeline_name Nullable(String),
                stage_name Nullable(String),
                tags_name Nullable(String),
                status Nullable(String),
                created_at Nullable(String),
                updated_at_field Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS piperun_companies (
                id String,
                name Nullable(String),
                cnpj Nullable(String),
                email Nullable(String),
                phone Nullable(String),
                city Nullable(String),
                state Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS piperun_activities (
                id String,
                type Nullable(String),
                deal_id Nullable(String),
                description Nullable(String),
                created_at Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS piperun_lost_reasons (
                id String,
                name Nullable(String),
                active Nullable(Int8),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS piperun_origins (
                id String,
                name Nullable(String),
                active Nullable(Int8),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}

        for table_name, config in self.ENDPOINTS.items():
            extra_params = {}
            if config.get("with"):
                extra_params["with"] = config["with"]

            raw_items = self._fetch_all_pages(config["path"], extra_params=extra_params)

            # Flatten especial para deals
            if table_name == "piperun_deals" and raw_items:
                items = self._flatten_deal_nested(raw_items)
            else:
                items = [self._flatten_dict(item) for item in raw_items] if raw_items else []

            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()

        return results
