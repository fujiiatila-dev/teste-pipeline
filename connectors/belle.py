import requests
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from connectors.base import BaseConnector
from config.settings import settings


class BelleConnector(BaseConnector):
    """Conector para Belle Software (salões/beleza). Auth: Token header. Chunking por data."""

    VALID_ESTABLISHMENTS = ["1", "2", "3", "4", "5", "8"]

    ENDPOINTS = {
        "belle_agendamentos": {"path": "agendamentos", "use_date_range": True, "start_date": "01/01/2024"},
        "belle_agendamentos_finalizados": {"path": "agendamentos_finalizados", "use_date_range": True, "start_date": "01/01/2024"},
        "belle_clientes": {"path": "clientes", "use_date_range": False},
        "belle_servicos": {"path": "servicos", "use_date_range": False},
        "belle_contas_receber": {"path": "contas_receber", "use_date_range": True, "start_date": "01/01/2025", "max_months": 3, "param_name": "estab"},
        "belle_vendas": {"path": "vendas", "use_date_range": True, "start_date": "01/01/2025", "max_days": 2},
    }

    def __init__(self, token: str = None, api_url: str = None, establishments: str = None):
        self.token = token or settings.belle_token
        self.base_url = (api_url or settings.belle_api_url or "https://app.bellesoftware.com.br/api/release/controller/IntegracaoExterna/v1.0").rstrip("/")
        self.establishments = [e.strip() for e in (establishments or settings.belle_establishments or "1").split(",") if e.strip()]

    def _headers(self):
        return {"Authorization": self.token, "Content-Type": "application/json"}

    def _fetch(self, path: str, params: dict = None) -> list:
        url = f"{self.base_url}/{path}"
        try:
            resp = requests.get(url, headers=self._headers(), params=params or {}, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("data", data.get("items", [data]))
            return []
        except Exception as e:
            logging.error(f"[Belle] Erro {path}: {e}")
            return []

    def _generate_date_chunks(self, start_str: str, end_date: datetime, max_days: int = None, max_months: int = None):
        """Gera chunks de datas no formato DD/MM/YYYY."""
        start = datetime.strptime(start_str, "%d/%m/%Y")
        chunks = []
        current = start
        while current < end_date:
            if max_days:
                chunk_end = min(current + timedelta(days=max_days), end_date)
            elif max_months:
                chunk_end = min(current + timedelta(days=max_months * 30), end_date)
            else:
                chunk_end = end_date
            chunks.append((current.strftime("%d/%m/%Y"), chunk_end.strftime("%d/%m/%Y")))
            current = chunk_end + timedelta(days=1)
        return chunks

    def get_tables_ddl(self) -> list:
        tables = list(self.ENDPOINTS.keys())
        return [f"""
            CREATE TABLE IF NOT EXISTS {t} (
                id String,
                data String,
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """ for t in tables]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}

        for table_name, cfg in self.ENDPOINTS.items():
            all_items = []

            if cfg.get("use_date_range"):
                start_str = cfg.get("start_date", date_start.strftime("%d/%m/%Y"))
                max_days = cfg.get("max_days")
                max_months = cfg.get("max_months")

                if max_days or max_months:
                    chunks = self._generate_date_chunks(start_str, date_stop, max_days, max_months)
                else:
                    chunks = [(start_str, date_stop.strftime("%d/%m/%Y"))]

                for dt_ini, dt_fim in chunks:
                    if cfg.get("param_name") == "estab":
                        for estab in self.establishments:
                            params = {"dtInicio": dt_ini, "dtFim": dt_fim, "estab": estab}
                            items = self._fetch(cfg["path"], params)
                            for item in items:
                                item["cod_estab"] = estab
                            all_items.extend(items)
                            time.sleep(0.5)
                    else:
                        for estab in self.establishments:
                            params = {"dtInicio": dt_ini, "dtFim": dt_fim, "codEstab": estab}
                            items = self._fetch(cfg["path"], params)
                            for item in items:
                                item["cod_estab"] = estab
                            all_items.extend(items)
                            time.sleep(0.5)
            else:
                items = self._fetch(cfg["path"])
                all_items.extend(items)

            logging.info(f"[Belle] {table_name}: {len(all_items)} registros")
            results[table_name] = pd.DataFrame(all_items) if all_items else pd.DataFrame()

        return results
