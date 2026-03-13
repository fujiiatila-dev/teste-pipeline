import requests
import re
import time
import logging
import pandas as pd
from datetime import datetime
from connectors.base import BaseConnector
from config.settings import settings


class ClicksignConnector(BaseConnector):
    """Conector para Clicksign (assinatura digital). Auth: Bearer Token. Nested endpoints por envelope."""

    def __init__(self, token: str = None, api_url: str = None):
        self.token = token or settings.clicksign_token
        self.api_url = (api_url or settings.clicksign_api_url or "https://app.clicksign.com/api/v3").rstrip("/")

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

    def _parse_link_header(self, link_header: str) -> dict:
        links = {}
        if not link_header:
            return links
        for part in link_header.split(","):
            match = re.match(r'<([^>]+)>;\s*rel="([^"]+)"', part.strip())
            if match:
                links[match.group(2)] = match.group(1)
        return links

    def _fetch_envelopes(self) -> list:
        all_items = []
        page = 1
        while True:
            url = f"{self.api_url}/envelopes"
            params = {"page[size]": 20, "page[number]": page}
            try:
                resp = requests.get(url, headers=self._headers(), params=params, timeout=60)
                resp.raise_for_status()
            except Exception as e:
                logging.error(f"[Clicksign] Erro envelopes page={page}: {e}")
                break
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break
            all_items.extend(items)
            links = self._parse_link_header(resp.headers.get("Link", ""))
            if "next" not in links:
                break
            page += 1
            time.sleep(0.5)
        logging.info(f"[Clicksign] envelopes: {len(all_items)} registros")
        return all_items

    def _fetch_nested(self, envelopes: list) -> tuple:
        documents = []
        signers = []
        for i, envelope in enumerate(envelopes):
            env_id = envelope.get("id", "")
            if not env_id:
                continue
            # Documents
            try:
                url = f"{self.api_url}/envelopes/{env_id}/documents"
                resp = requests.get(url, headers=self._headers(), timeout=60)
                resp.raise_for_status()
                docs = resp.json().get("data", [])
                for doc in docs:
                    doc["envelope_id"] = env_id
                documents.extend(docs)
            except Exception as e:
                logging.error(f"[Clicksign] Erro docs envelope {env_id}: {e}")
            # Signers
            try:
                url = f"{self.api_url}/envelopes/{env_id}/signers"
                resp = requests.get(url, headers=self._headers(), timeout=60)
                resp.raise_for_status()
                signs = resp.json().get("data", [])
                for s in signs:
                    s["envelope_id"] = env_id
                signers.extend(signs)
            except Exception as e:
                logging.error(f"[Clicksign] Erro signers envelope {env_id}: {e}")
            if (i + 1) % 50 == 0:
                logging.info(f"[Clicksign] Processados {i+1}/{len(envelopes)} envelopes")
            time.sleep(0.3)
        return documents, signers

    def get_tables_ddl(self) -> list:
        return [
            """CREATE TABLE IF NOT EXISTS clicksign_envelopes (
                id String, status Nullable(String), name Nullable(String),
                created_at Nullable(String), updated_at_source Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY id""",
            """CREATE TABLE IF NOT EXISTS clicksign_envelopes_documents (
                id String, envelope_id String, filename Nullable(String),
                content_type Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (envelope_id, id)""",
            """CREATE TABLE IF NOT EXISTS clicksign_envelopes_signers (
                id String, envelope_id String, name Nullable(String),
                email Nullable(String), status Nullable(String),
                project_id String DEFAULT '', updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (envelope_id, id)""",
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        envelopes = self._fetch_envelopes()
        documents, signers = self._fetch_nested(envelopes)
        return {
            "clicksign_envelopes": pd.DataFrame(envelopes) if envelopes else pd.DataFrame(),
            "clicksign_envelopes_documents": pd.DataFrame(documents) if documents else pd.DataFrame(),
            "clicksign_envelopes_signers": pd.DataFrame(signers) if signers else pd.DataFrame(),
        }
