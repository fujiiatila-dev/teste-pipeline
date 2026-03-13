import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class CvcrmCvdwConnector(BaseConnector):
    """Conector para CVCRM CVDW. Auth: email+token headers. Paginação: page-based (500/page)."""

    ENDPOINTS = {
        "cvcrm_cvdw_vendas": "cvdw/vendas",
        "cvcrm_cvdw_reservas": "cvdw/reservas",
        "cvcrm_cvdw_leads_visitas": "cvdw/leads/visitas",
        "cvcrm_cvdw_leads": "cvdw/leads",
        "cvcrm_cvdw_leads_historico_situacoes": "cvdw/leads/historico/situacoes",
    }

    def __init__(self, api_dominio: str = None, email: str = None, token: str = None):
        self.api_dominio = api_dominio or settings.cvcrm_api_dominio
        self.email = email or settings.cvcrm_email
        self.token = token or settings.cvcrm_token
        self.base_url = f"https://{self.api_dominio}.cvcrm.com.br/api/v1" if self.api_dominio else ""

    def _headers(self):
        return {"email": self.email, "token": self.token, "Content-Type": "application/json"}

    def _fetch_all_pages(self, endpoint: str) -> list:
        all_items = []
        page = 1
        while True:
            url = f"{self.base_url}/{endpoint}"
            params = {"pagina": page, "registros_por_pagina": 500}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[CVCRM-CVDW] Erro {endpoint} pagina={page}: {e}")
                break
            data = resp.json()
            items = data.get("dados", data.get("data", []))
            if isinstance(items, dict):
                items = list(items.values()) if items else []
            if not items:
                break
            all_items.extend(items)
            total_pages = data.get("total_de_paginas", 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(5)
        logging.info(f"[CVCRM-CVDW] {endpoint}: {len(all_items)} registros em {page} páginas")
        return all_items

    def get_tables_ddl(self) -> list:
        ddls = []
        for table_name in self.ENDPOINTS:
            ddls.append(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """)
        return ddls

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        for table_name, endpoint in self.ENDPOINTS.items():
            items = self._fetch_all_pages(endpoint)
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
