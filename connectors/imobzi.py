import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class ImobziConnector(BaseConnector):
    """Conector para Imobzi (CRM imobiliário). Auth: X-Imobzi-Secret header. Paginação: cursor-based."""

    ENDPOINTS = {
        "imobzi_deals": {"path": "v1/deals/search", "paginated": True, "data_field": "deals"},
        "imobzi_deals_rotations": {"path": "v1/deals-rotations", "paginated": False, "data_field": "rotations"},
        "imobzi_deal_lost_reasons": {"path": "v1/deal/lost-reason", "paginated": False, "data_field": "deals_lost_reasons"},
        "imobzi_pipelines": {"path": "v1/pipelines", "paginated": False, "data_field": None},
        "imobzi_pipeline_groups": {"path": "v1/pipeline-groups", "paginated": False, "data_field": None},
        "imobzi_contacts": {"path": "v1/contacts", "paginated": True, "data_field": "contacts"},
        "imobzi_contacts_tags": {"path": "v1/contacts/tags", "paginated": False, "data_field": "tags"},
        "imobzi_person_fields": {"path": "v1/person-fields", "paginated": False, "data_field": "group_contact"},
        "imobzi_organization_fields": {"path": "v1/organization-fields", "paginated": False, "data_field": "group_contact"},
    }

    def __init__(self, api_secret: str = None, api_url: str = None):
        self.api_secret = api_secret or settings.imobzi_api_secret
        self.api_url = (api_url or settings.imobzi_api_url or "https://api.imobzi.app").rstrip("/")

    def _headers(self):
        return {"X-Imobzi-Secret": self.api_secret, "Content-Type": "application/json"}

    def _fetch_cursor(self, path: str, data_field: str) -> list:
        all_items = []
        cursor = None
        while True:
            url = f"{self.api_url}/{path}"
            params = {"cursor": cursor} if cursor else {}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Imobzi] Erro {path}: {e}")
                break
            data = resp.json()
            if data_field:
                items = data.get(data_field, [])
            elif isinstance(data, list):
                items = data
            else:
                items = []
            if not items:
                break
            all_items.extend(items)
            cursor = data.get("cursor") if isinstance(data, dict) else None
            if not cursor:
                break
            time.sleep(0.2)
        logging.info(f"[Imobzi] {path}: {len(all_items)} registros")
        return all_items

    def _fetch_simple(self, path: str, data_field: str) -> list:
        url = f"{self.api_url}/{path}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if data_field:
                return data.get(data_field, [])
            elif isinstance(data, list):
                return data
            return [data] if isinstance(data, dict) else []
        except Exception as e:
            logging.error(f"[Imobzi] Erro {path}: {e}")
            return []

    def get_tables_ddl(self) -> list:
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String, data String,
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id
            """ for t in self.ENDPOINTS]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            if cfg["paginated"]:
                items = self._fetch_cursor(cfg["path"], cfg["data_field"])
            else:
                items = self._fetch_simple(cfg["path"], cfg["data_field"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
