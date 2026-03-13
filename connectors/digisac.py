import requests
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class DigisacConnector(BaseConnector):
    """
    Conector para Digisac (atendimento ao cliente).
    Auth: Bearer Token. Paginação: page-based.
    """

    ENDPOINTS = {
        "digisac_questions": {"path": "questions", "paginated": True},
        "digisac_contacts": {"path": "contacts", "paginated": True},
        "digisac_answers_overview": {"path": "answers/overview", "paginated": False},
    }

    def __init__(self, api_url: str = None, token: str = None):
        base = (api_url or settings.digisac_api_url or "").rstrip("/")
        self.api_url = f"{base}/api/v1" if base and not base.endswith("/api/v1") else base
        self.token = token or settings.digisac_token

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _fetch_paginated(self, path: str) -> list:
        all_items = []
        page = 1
        per_page = 500

        while True:
            url = f"{self.api_url}/{path}"
            params = {"currentPage": page, "perPage": per_page}

            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logging.error(f"[Digisac] Erro {path} page={page}: {e}")
                break

            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            all_items.extend(items)

            last_page = data.get("lastPage", 1)
            if page >= last_page:
                break

            page += 1
            time.sleep(0.5)

        logging.info(f"[Digisac] {path}: {len(all_items)} registros")
        return all_items

    def _fetch_single(self, path: str) -> list:
        url = f"{self.api_url}/{path}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            return []
        except requests.exceptions.RequestException as e:
            logging.error(f"[Digisac] Erro {path}: {e}")
            return []

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS digisac_questions (
                id String,
                title Nullable(String),
                description Nullable(String),
                status Nullable(String),
                createdAt Nullable(String),
                updatedAt Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS digisac_contacts (
                id String,
                name Nullable(String),
                email Nullable(String),
                phone Nullable(String),
                createdAt Nullable(String),
                updatedAt Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            """
            CREATE TABLE IF NOT EXISTS digisac_answers_overview (
                key String,
                value Nullable(String),
                project_id String DEFAULT '',
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY key
            """,
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        results = {}
        for table_name, cfg in self.ENDPOINTS.items():
            if cfg["paginated"]:
                items = self._fetch_paginated(cfg["path"])
            else:
                items = self._fetch_single(cfg["path"])
            results[table_name] = pd.DataFrame(items) if items else pd.DataFrame()
        return results
