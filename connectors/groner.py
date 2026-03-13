import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class GronerConnector(BaseConnector):
    """Conector para Groner (IceHot API). Auth: Bearer Token. Paginação: page-based (500/page)."""

    ENDPOINTS = {
        "groner_usuario_periodo_funil_status": {
            "path": "api/Health/DadosUsoPorPeriodoPorFunilPorStatus",
            "paginated": False, "use_date_range": True,
            "extra_params": {"etapaId": 44},
        },
        "groner_usuario_periodo": {
            "path": "api/Health/DadosUsoPorPeriodo",
            "paginated": False, "use_date_range": True,
        },
        "groner_usuario_periodo_vendedor": {
            "path": "api/Health/DadosUsoPorPeriodoPorVendedor",
            "paginated": False, "use_date_range": True,
        },
        "groner_etapa_lead": {
            "path": "api/etapaLead",
            "paginated": True,
        },
        "groner_status_projeto": {
            "path": "api/statusProjeto",
            "paginated": True,
        },
        "groner_projeto_cards": {
            "path": "api/projeto/cards",
            "paginated": True,
            "extra_params": {"ordenarPor": "DataAtualizacao_DESC"},
        },
        "groner_atividades": {
            "path": "api/atividades",
            "paginated": True,
        },
        "groner_lead_cards": {
            "path": "api/lead/cards",
            "paginated": True,
        },
        "groner_loja": {
            "path": "api/loja",
            "paginated": True,
        },
        "groner_usuario": {
            "path": "api/usuario",
            "paginated": True,
            "extra_params": {"somenteAtivos": "true"},
        },
    }

    def __init__(self, api_url: str = None, token: str = None):
        self.api_url = (api_url or settings.groner_api_url or "https://icehot.api.groner.app").rstrip("/")
        self.token = token or settings.groner_token

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _fetch_paginated(self, path: str, extra_params: dict = None) -> list:
        all_items = []
        page = 1
        params = extra_params or {}
        while True:
            url = f"{self.api_url}/{path}"
            req_params = {"pageSize": 500, "pageNumber": page, **params}
            try:
                resp = requests.get(url, headers=self._headers(), params=req_params, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Groner] Erro {path} page={page}: {e}")
                break
            data = resp.json()
            content = data.get("Content", data)
            items = content.get("list", content.get("data", []))
            if isinstance(items, dict):
                items = [items]
            if not items or not isinstance(items, list):
                break
            all_items.extend(items)
            has_next = content.get("hasNextPage", False)
            if not has_next:
                break
            page += 1
            time.sleep(0.2)
        logging.info(f"[Groner] {path}: {len(all_items)} registros em {page} páginas")
        return all_items

    def _fetch_date_range(self, path: str, date_start: datetime, date_stop: datetime, extra_params: dict = None) -> list:
        url = f"{self.api_url}/{path}"
        params = {
            "dataInicial": date_start.strftime("%Y-%m-%d"),
            "dataFinal": date_stop.strftime("%Y-%m-%d"),
            **(extra_params or {}),
        }
        try:
            resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            content = data.get("Content", data)
            items = content.get("list", content.get("data", []))
            if isinstance(items, dict):
                items = [items]
            return items if isinstance(items, list) else []
        except Exception as e:
            logging.error(f"[Groner] Erro {path}: {e}")
            return []

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
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            extra = cfg.get("extra_params", {})
            if cfg.get("use_date_range"):
                items = self._fetch_date_range(cfg["path"], date_start, date_stop, extra)
            elif cfg.get("paginated"):
                items = self._fetch_paginated(cfg["path"], extra)
            else:
                items = self._fetch_date_range(cfg["path"], date_start, date_stop, extra)
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
