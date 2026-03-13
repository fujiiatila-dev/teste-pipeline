import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class SigaviConnector(BaseConnector):
    """
    Conector para Sigavi360 CRM.
    Auth: form-encoded POST (grant_type=password) → bearer token.
    Paginação: POST-based (pagina no body).
    """

    def __init__(self, api_url: str = None, username: str = None, password: str = None):
        self.api_url = (api_url or settings.sigavi_api_url or "").rstrip("/")
        self.username = username or settings.sigavi_username
        self.password = password or settings.sigavi_password
        self.token = None

    def _authenticate(self):
        url = f"{self.api_url}/Sigavi/api/Acesso/Token"
        payload = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }
        resp = requests.post(url, data=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        self.token = data.get("access_token") or data.get("token")
        logging.info("[Sigavi] Autenticado com sucesso.")

    def _headers(self):
        return {
            "Authorization": f"bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _fetch_paginated_post(self, endpoint: str) -> list:
        all_items = []
        page = 1

        while True:
            url = f"{self.api_url}/{endpoint}"
            payload = {"pagina": page}

            try:
                resp = requests.post(url, headers=self._headers(), json=payload, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Sigavi] Erro pagina {page}: {e}")
                break

            data = resp.json()

            # Extrair itens da resposta
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("data", data.get("items", []))
                if not isinstance(items, list):
                    items = [data]
            else:
                break

            if not items:
                break

            all_items.extend(items)

            total_pages = data.get("PaginaTotal", 1) if isinstance(data, dict) else 1
            if page >= total_pages:
                break

            page += 1
            time.sleep(0.5)

        logging.info(f"[Sigavi] {endpoint}: {len(all_items)} registros em {page} páginas")
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS sigavi_fac_lista (
                id String,
                nome Nullable(String),
                email Nullable(String),
                telefone Nullable(String),
                status Nullable(String),
                data_cadastro Nullable(String),
                origem Nullable(String),
                empreendimento Nullable(String),
                corretor Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        if not self.token:
            self._authenticate()

        items = self._fetch_paginated_post("api/crm/fac/lista")
        df = pd.DataFrame(items) if items else pd.DataFrame()
        return {"sigavi_fac_lista": df}
