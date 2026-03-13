import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class EverflowConnector(BaseConnector):
    """Conector para Everflow (ERP). Auth: Bearer Token. Paginação configurável por endpoint."""

    ENDPOINTS = {
        "everflow_clientes": {"path": "v1/clientes", "data_key": "itens", "pagination_disabled": True},
        "everflow_contratos": {"path": "v1/contratos", "data_key": "itens", "pagination_disabled": True},
        "everflow_fornecedores": {"path": "v1/fornecedores", "data_key": "itens", "pagination_disabled": True},
        "everflow_movimento_bancarios": {"path": "v1/movimentoBancarios", "data_key": "itens", "pagination_disabled": True},
        "everflow_orcamentos": {"path": "v1/orcamentos", "data_key": "itens", "pagination_disabled": True},
        "everflow_orcamentos_propostas": {"path": "v1/orcamentos/propostas", "data_key": "itens", "pagination_disabled": True},
        "everflow_orcamentos_propostas_kanban": {"path": "v1/orcamentos/propostas/kanban", "data_key": "itens", "pagination_disabled": True},
        "everflow_pagars": {"path": "v1/pagars", "data_key": "itens", "pagination_disabled": True},
        "everflow_pedidos_venda": {"path": "v1/pedidosVenda", "data_key": "itens", "pagination_disabled": True},
        "everflow_recebers": {"path": "v1/recebers", "data_key": "itens", "pagination_disabled": True},
        "everflow_consultores": {"path": "v1/consultores", "data_key": None, "pagination_disabled": True},
        "everflow_equipamentos": {"path": "v1/equipamentos", "data_key": None, "pagination_disabled": True},
        "everflow_ordens_servico": {"path": "v1/gestaoServicos/OrdensServico", "data_key": "itens", "pagination_disabled": False},
        "everflow_ordens_servico_tarefas": {"path": "v1/gestaoServicos/OrdensServico/tarefas", "data_key": "itens", "pagination_disabled": False},
        "everflow_funcionarios": {"path": "v1/funcionarios", "data_key": "data.itens", "pagination_disabled": True},
    }

    def __init__(self, api_url: str = None, token: str = None):
        self.api_url = (api_url or settings.everflow_api_url or "").rstrip("/")
        self.token = token or settings.everflow_token

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _extract_data(self, resp_json, data_key: str):
        """Extrai dados usando data_key que pode ser nested (ex: 'data.itens')."""
        if data_key is None:
            return resp_json if isinstance(resp_json, list) else [resp_json] if isinstance(resp_json, dict) else []
        parts = data_key.split(".")
        result = resp_json
        for part in parts:
            if isinstance(result, dict):
                result = result.get(part, [])
            else:
                return []
        return result if isinstance(result, list) else [result] if result else []

    def _fetch_endpoint(self, path: str, data_key: str, pagination_disabled: bool) -> list:
        all_items = []

        if pagination_disabled:
            url = f"{self.api_url}/{path}"
            params = {"DesabilitarPaginacao": "True"}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                resp.raise_for_status()
                items = self._extract_data(resp.json(), data_key)
                all_items.extend(items)
            except Exception as e:
                logging.error(f"[Everflow] Erro {path}: {e}")
        else:
            page = 1
            while True:
                url = f"{self.api_url}/{path}"
                params = {"pageSize": 500, "page": page, "DesabilitarPaginacao": "False"}
                try:
                    resp = requests.get(url, headers=self._headers(), params=params, timeout=120)
                    resp.raise_for_status()
                except Exception as e:
                    logging.error(f"[Everflow] Erro {path} page={page}: {e}")
                    break
                items = self._extract_data(resp.json(), data_key)
                if not items:
                    break
                all_items.extend(items)
                if len(items) < 500:
                    break
                page += 1
                time.sleep(0.5)

        logging.info(f"[Everflow] {path}: {len(all_items)} registros")
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
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            items = self._fetch_endpoint(cfg["path"], cfg["data_key"], cfg["pagination_disabled"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
