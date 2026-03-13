import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class HypnoboxConnector(BaseConnector):
    """Conector para Hypnobox (CRM imobiliário). Auth: login/password → token. Paginação: page-based."""

    ENDPOINTS = {
        "hypnobox_products": {"path": "api/products", "data_key": "Produtos", "paginated": False},
        "hypnobox_clients": {"path": "api/clients-v2.json", "data_key": "Clientes", "paginated": True},
        "hypnobox_visitas": {"path": "api/visitas", "data_key": "Visitas", "paginated": True},
        "hypnobox_propostas": {"path": "api/consultaproposta", "data_key": "Propostas", "paginated": True},
        "hypnobox_tarefas": {"path": "api/consultatarefa", "data_key": "Tarefas", "paginated": True},
    }

    def __init__(self, login: str = None, password: str = None, subdomain: str = None, api_url: str = None):
        self.login = login or settings.hypnobox_login
        self.password = password or settings.hypnobox_password
        self.subdomain = subdomain or settings.hypnobox_subdomain
        if api_url:
            self.base_url = api_url.rstrip("/")
        elif self.subdomain:
            self.base_url = f"https://{self.subdomain}.hypnobox.com.br"
        else:
            self.base_url = ""
        self.token = None

    def _authenticate(self):
        url = f"{self.base_url}/api/auth"
        params = {"login": self.login, "password": self.password, "returnType": "json"}
        resp = requests.post(url, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        self.token = data.get("token") or data.get("Token")
        logging.info("[Hypnobox] Autenticado com sucesso.")

    def _fetch_simple(self, path: str, data_key: str) -> list:
        url = f"{self.base_url}/{path}"
        params = {"token": self.token, "returnType": "json"}
        try:
            resp = requests.get(url, headers={"Content-Type": "application/json"}, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            return data.get(data_key, []) if isinstance(data, dict) else data
        except Exception as e:
            logging.error(f"[Hypnobox] Erro {path}: {e}")
            return []

    def _fetch_paginated(self, path: str, data_key: str) -> list:
        all_items = []
        page = 1
        while True:
            url = f"{self.base_url}/{path}"
            params = {"token": self.token, "pagina": page, "returnType": "json"}
            try:
                resp = requests.get(url, headers={"Content-Type": "application/json"}, params=params, timeout=120)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Hypnobox] Erro {path} pagina={page}: {e}")
                break
            data = resp.json()
            items = data.get(data_key, []) if isinstance(data, dict) else data
            if not items:
                break
            all_items.extend(items)
            paginacao = data.get("Paginacao", {}) if isinstance(data, dict) else {}
            total_pages = paginacao.get("NumerodePaginas", 1)
            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)
        logging.info(f"[Hypnobox] {path}: {len(all_items)} registros em {page} páginas")
        return all_items

    def get_tables_ddl(self) -> list:
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """ for t in self.ENDPOINTS]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        if not self.token:
            self._authenticate()
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            if cfg["paginated"]:
                items = self._fetch_paginated(cfg["path"], cfg["data_key"])
            else:
                items = self._fetch_simple(cfg["path"], cfg["data_key"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
