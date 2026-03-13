import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class FacilitaConnector(BaseConnector):
    """Conector para AppFacilita (CRM/ERP). Dual API: BI reports + Platform funnel."""

    BI_REPORTS = {
        "facilita_negocios": [
            "bi_completo_negocios_p1", "bi_completo_negocios_p2", "bi_completo_negocios_p3",
            "bi_completo_negocios_p4", "bi_completo_negocios_p5",
        ],
        "facilita_leads": [
            "bi_relatorio_evolucao_leads_p1", "bi_relatorio_evolucao_leads_p2",
            "bi_relatorio_evolucao_leads_p3",
        ],
    }

    def __init__(self, token: str = None, instance: str = None, api_key: str = None, token_user: str = None):
        self.token = token or settings.facilita_token
        self.instance = instance or settings.facilita_instance
        self.api_key = api_key or settings.facilita_api_key
        self.token_user = token_user or settings.facilita_token_user
        self.bi_url = "https://bi.appfacilita.com/api/v1"
        self.platform_url = "https://api.facilitaapp.com/platform/v1"

    def _bi_headers(self):
        return {"facilita_token": self.token, "facilita_instance": self.instance, "Content-Type": "application/json"}

    def _platform_headers(self):
        return {"api-instance": self.instance, "api-key": self.api_key or self.token, "token-user": self.token_user or "", "Content-Type": "application/json"}

    def _fetch_bi_report(self, report_name: str) -> list:
        url = f"{self.bi_url}/analyses"
        params = {"report": report_name}
        try:
            resp = requests.get(url, headers=self._bi_headers(), params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("data", data.get("items", []))
            return []
        except Exception as e:
            logging.error(f"[Facilita] Erro BI report {report_name}: {e}")
            return []

    def _fetch_funnel(self) -> list:
        url = f"{self.platform_url}/funnel"
        try:
            resp = requests.get(url, headers=self._platform_headers(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return data.get("data", [data])
            return []
        except Exception as e:
            logging.error(f"[Facilita] Erro funnel: {e}")
            return []

    def get_tables_ddl(self) -> list:
        tables = list(self.BI_REPORTS.keys()) + ["facilita_funnel"]
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

        for table_name, reports in self.BI_REPORTS.items():
            all_items = []
            for report in reports:
                items = self._fetch_bi_report(report)
                all_items.extend(items)
                time.sleep(1)
            logging.info(f"[Facilita] {table_name}: {len(all_items)} registros de {len(reports)} relatórios")
            results[table_name] = pd.DataFrame(all_items) if all_items else pd.DataFrame()

        # Platform API: funnel
        funnel = self._fetch_funnel()
        results["facilita_funnel"] = pd.DataFrame(funnel) if funnel else pd.DataFrame()

        return results
