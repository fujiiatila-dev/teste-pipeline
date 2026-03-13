import requests
import re
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class CvcrmCvioConnector(BaseConnector):
    """Conector para CVCRM CVIO. Auth: email+token headers. Paginação: offset-based."""

    def __init__(self, api_dominio: str = None, email: str = None, token: str = None):
        self.api_dominio = api_dominio or settings.cvcrm_api_dominio
        self.email = email or settings.cvcrm_email
        self.token = token or settings.cvcrm_token
        self.base_url = f"https://{self.api_dominio}.cvcrm.com.br/api" if self.api_dominio else ""

    def _headers(self):
        return {"email": self.email, "token": self.token, "Content-Type": "application/json"}

    @staticmethod
    def _filter_fields(record: dict) -> dict:
        """Remove campos interacao_* e tarefa_N_descricao."""
        filtered = {}
        for key, value in record.items():
            if key.startswith("interacao_"):
                continue
            if re.match(r"tarefa_\d+_descricao", key):
                continue
            filtered[key] = value
        return filtered

    def _fetch_all(self) -> list:
        all_items = []
        offset = 0
        limit = 500
        while True:
            url = f"{self.base_url}/cvio/lead"
            params = {"offset": offset, "limit": limit}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[CVCRM-CVIO] Erro offset={offset}: {e}")
                break
            data = resp.json()
            items = data.get("leads", [])
            if not items:
                break
            filtered_items = [self._filter_fields(item) for item in items]
            all_items.extend(filtered_items)
            total = data.get("total", 0)
            if offset + limit >= total:
                break
            offset += limit
            time.sleep(5)
        logging.info(f"[CVCRM-CVIO] leads: {len(all_items)} registros")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS cvcrm_cvio_leads (
                id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        items = self._fetch_all()
        df = pd.DataFrame(items) if items else pd.DataFrame()
        return {"cvcrm_cvio_leads": df}
