import io
import re
import time
import logging
import requests
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class ClickUpConnector(BaseConnector):
    """
    Conector para extração de dados do ClickUp via exportação de view.
    Auth: Bearer token + x-csrf + x-workspace-id. Sem paginação (export CSV completo).
    """

    EXPORT_URL = "https://cu-prod-prod-us-west-2-2-export-service.clickup.com/v1/exportView"

    EXPORT_PARAMS = {
        "date_type": "normal",
        "time_format": "normal",
        "name_only": "False",
        "all_columns": "True",
        "async": "True",
        "time_in_status_total": "False",
    }

    def __init__(self, bearer_token: str = None, workspace_id: str = None, view_id: str = None):
        self.bearer_token = bearer_token or settings.clickup_bearer_token
        self.workspace_id = workspace_id or settings.clickup_workspace_id
        self.view_id = view_id or settings.clickup_view_id

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.bearer_token}",
            "Content-Type": "application/json",
            "x-csrf": "1",
            "x-workspace-id": self.workspace_id,
        }

    @staticmethod
    def _clean_csv(csv_content: str) -> str:
        """Remove quebras de linha dentro de campos CSV e normaliza espacos."""
        content = csv_content.replace("\r\n", "\n").replace("\r", "\n")
        quoted_pattern = r'"([^"]*(?:""[^"]*)*)"'

        def replace_newlines(match):
            field = match.group(1)
            cleaned = re.sub(r"\n+", " ", field)
            return f'"{cleaned}"'

        content = re.sub(quoted_pattern, replace_newlines, content)
        content = re.sub(r" +", " ", content)
        return content

    def _build_payload(self) -> dict:
        """Payload simplificado para exportação de view."""
        return {
            "id": self.view_id,
            "members": [],
            "group_members": [],
            "name": "Export",
            "parent": {"id": self.workspace_id, "type": 7},
            "type": 23,
            "settings": {
                "show_task_locations": False,
                "show_subtasks": 1,
                "show_closed_subtasks": False,
                "show_assignees": True,
                "show_images": True,
            },
            "grouping": {"field": "none", "dir": 1, "collapsed": [], "ignore": False},
            "sorting": {"fields": []},
            "filters": {
                "search": "",
                "show_closed": False,
                "op": "AND",
                "fields": [],
            },
            "columns": {"fields": []},
        }

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS clickup_tasks (
                task_id Nullable(String),
                name Nullable(String),
                status Nullable(String),
                assignee Nullable(String),
                priority Nullable(String),
                due_date Nullable(String),
                date_created Nullable(String),
                date_updated Nullable(String),
                date_done Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY task_id
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        if not self.bearer_token or not self.workspace_id or not self.view_id:
            logging.warning("[ClickUp] Credenciais incompletas, pulando extracaoo")
            return {"clickup_tasks": pd.DataFrame()}

        payload = self._build_payload()

        logging.info(f"[ClickUp] Solicitando export da view {self.view_id}...")
        resp = requests.post(
            self.EXPORT_URL,
            headers=self._headers(),
            params=self.EXPORT_PARAMS,
            json=payload,
            timeout=120,
        )

        if not resp.ok:
            logging.error(f"[ClickUp] Erro no export: HTTP {resp.status_code} - {resp.text}")
            return {"clickup_tasks": pd.DataFrame()}

        data = resp.json()
        csv_url = data.get("url")
        if not csv_url:
            logging.error(f"[ClickUp] URL do CSV nao encontrada na resposta: {data}")
            return {"clickup_tasks": pd.DataFrame()}

        logging.info(f"[ClickUp] Baixando CSV de {csv_url[:60]}...")
        csv_resp = requests.get(csv_url, timeout=120)
        if not csv_resp.ok:
            logging.error(f"[ClickUp] Erro ao baixar CSV: HTTP {csv_resp.status_code}")
            return {"clickup_tasks": pd.DataFrame()}

        try:
            csv_content = csv_resp.content.decode("utf-8")
        except UnicodeDecodeError:
            csv_content = csv_resp.content.decode("latin-1")

        cleaned = self._clean_csv(csv_content)
        df = pd.read_csv(io.StringIO(cleaned))

        # Limpeza de strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.replace("\n", " ").str.replace("\r", " ").str.strip()

        logging.info(f"[ClickUp] {len(df)} tarefas extraidas com {len(df.columns)} colunas")
        return {"clickup_tasks": df}
