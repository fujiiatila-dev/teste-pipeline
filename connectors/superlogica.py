import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class SuperlogicaConnector(BaseConnector):
    """Conector para Superlógica (gestão imobiliária). Auth: app_token + access_token headers."""

    ENDPOINTS = {
        "superlogica_contratos": {"path": "contratos", "data_key": "data"},
        "superlogica_proprietarios": {"path": "proprietarios", "data_key": "data"},
        "superlogica_locatarios": {"path": "locatarios", "data_key": "data"},
    }

    def __init__(self, app_token: str = None, access_token: str = None, api_url: str = None):
        self.app_token = app_token or settings.superlogica_app_token
        self.access_token = access_token or settings.superlogica_access_token
        self.base_url = (api_url or settings.superlogica_api_url or "http://apps.superlogica.net/imobiliaria/api").rstrip("/")

    def _headers(self):
        return {"app_token": self.app_token, "access_token": self.access_token, "Content-Type": "application/json"}

    def _fetch_all_pages(self, path: str, data_key: str) -> list:
        all_items = []
        page = 1
        while True:
            url = f"{self.base_url}/{path}"
            params = {"pagina": page}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Superlogica] Erro {path} pagina={page}: {e}")
                break
            data = resp.json()
            items = data if isinstance(data, list) else data.get(data_key, [])
            if not items:
                break
            all_items.extend(items)
            # Superlogica doesn't always return total pages; stop if less than expected
            if len(items) < 50:
                break
            page += 1
            time.sleep(1)
        logging.info(f"[Superlogica] {path}: {len(all_items)} registros")
        return all_items

    @staticmethod
    def _flatten_proprietarios(proprietarios: list) -> tuple:
        """Extrai beneficiários e contratos aninhados dos proprietários."""
        beneficiarios = []
        contratos = []
        for prop in proprietarios:
            prop_id = prop.get("id", "")
            bens = prop.pop("proprietarios_beneficiarios", []) or []
            for ben in bens:
                ben["proprietario_id"] = prop_id
                ben_contratos = ben.pop("contratos", []) or []
                for contrato in ben_contratos:
                    contrato["beneficiario_id"] = ben.get("id", "")
                    contrato["proprietario_id"] = prop_id
                    contratos.append(contrato)
                beneficiarios.append(ben)
        return beneficiarios, contratos

    def get_tables_ddl(self) -> list:
        tables = ["superlogica_contratos", "superlogica_proprietarios", "superlogica_locatarios",
                   "superlogica_proprietarios_beneficiarios", "superlogica_proprietarios_contratos"]
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
            items = self._fetch_all_pages(cfg["path"], cfg["data_key"])
            if table_name == "superlogica_proprietarios" and items:
                beneficiarios, contratos = self._flatten_proprietarios(items)
                results["superlogica_proprietarios_beneficiarios"] = pd.DataFrame(beneficiarios) if beneficiarios else pd.DataFrame()
                results["superlogica_proprietarios_contratos"] = pd.DataFrame(contratos) if contratos else pd.DataFrame()
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
