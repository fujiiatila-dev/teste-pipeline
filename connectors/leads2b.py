import requests
import pandas as pd
from datetime import datetime
import logging
from connectors.base import BaseConnector
from config.settings import settings

class Leads2bConnector(BaseConnector):
    def __init__(self, token: str = None):
        self.base_url = "https://api-v2.leads2b.com/v2"
        self.token = token or settings.get("leads2b_token")

    def _get_headers(self):
        return {
            "access-token": self.token,
            "Content-Type": "application/json"
        }

    def _fetch_paginated(self, endpoint, params=None):
        """
        Busca dados de forma paginada para o Leads2b.
        A API do Leads2b costuma retornar os itens em uma chave correspondente ao endpoint (ex: 'leads').
        """
        all_items = []
        page = 1
        per_page = 100
        
        if params is None:
            params = {}
            
        params["per_page"] = per_page

        while True:
            params["page"] = page
            response = requests.get(f"{self.base_url}/{endpoint}", headers=self._get_headers(), params=params)
            
            if response.status_code == 404:
                logging.warning(f"Leads2b: Endpoint {endpoint} não encontrado.")
                break
                
            response.raise_for_status()
            data = response.json()
            
            # O Leads2b costuma retornar uma lista diretamente ou em uma chave com o nome do recurso
            items = data.get("data", []) if isinstance(data, dict) else []
            if not items and isinstance(data, list):
                items = data
            
            if not items:
                break
                
            all_items.extend(items)
            
            # Controle de paginação simples: se veio menos que o per_page, é a última página
            if len(items) < per_page:
                break
                
            page += 1
            if page > 100: # Safety break
                break
            
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS leads2b_leads (
                id String,
                name String,
                email String,
                status String,
                funnel_stage String,
                source String,
                created_at DateTime,
                updated_at DateTime,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS leads2b_negotiations (
                id String,
                lead_id String,
                title String,
                value Float64,
                status String,
                created_at DateTime,
                updated_at DateTime,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        logging.info(f"Leads2b: Extraindo leads desde {date_start.date()}")
        
        # Parâmetros de filtro (suposição baseada em padrões de API CRM)
        params_leads = {
            "start_date": date_start.strftime("%Y-%m-%d"),
            "end_date": date_stop.strftime("%Y-%m-%d")
        }
        
        # 1. Leads
        raw_leads = self._fetch_paginated("leads", params=params_leads)
        leads = []
        for l in raw_leads:
            leads.append({
                "id": str(l.get("id")),
                "name": l.get("name"),
                "email": l.get("email"),
                "status": l.get("status"),
                "funnel_stage": l.get("funnel_stage", {}).get("name") if isinstance(l.get("funnel_stage"), dict) else l.get("funnel_stage"),
                "source": l.get("source"),
                "created_at": l.get("created_at"),
                "updated_at": l.get("updated_at")
            })

        # 2. Negociações (Opportunities)
        # Nota: Muitas vezes as negociações estão vinculadas ao lead ou em endpoint próprio
        raw_negs = self._fetch_paginated("negotiations", params=params_leads)
        negotiations = []
        for n in raw_negs:
            negotiations.append({
                "id": str(n.get("id")),
                "lead_id": str(n.get("lead_id")),
                "title": n.get("title"),
                "value": float(n.get("value", 0)) if n.get("value") else 0.0,
                "status": n.get("status"),
                "created_at": n.get("created_at"),
                "updated_at": n.get("updated_at")
            })

        return {
            "leads2b_leads": pd.DataFrame(leads),
            "leads2b_negotiations": pd.DataFrame(negotiations)
        }
