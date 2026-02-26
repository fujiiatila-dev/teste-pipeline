import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from typing import Dict, Any, List
from connectors.base import BaseConnector
from config.settings import settings

class RDMarketingConnector(BaseConnector):
    """
    Conector para extração de dados do RD Station Marketing.
    Extrai Analytics de Emails, Conversões e Leads do Webhook.
    """
    
    def __init__(self, client_id: str = None, client_secret: str = None, refresh_token: str = None, x_api_key: str = None, alias: str = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.x_api_key = x_api_key # Para o proxy de webhook da Nalk
        self.alias = alias # Empresa no proxy
        self.base_url = "https://api.rd.services"
        self.webhook_proxy_url = "https://webhook-rd.nalk.com.br"
        self._access_token = None

    def _get_access_token(self):
        if self._access_token:
            return self._access_token
            
        logging.info("RD Marketing: Obtendo access token...")
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }
        response = requests.post(f"{self.base_url}/auth/token", data=payload)
        response.raise_for_status()
        self._access_token = response.json()["access_token"]
        return self._access_token

    def _safe_request(self, url: str, method: str = "GET", headers: dict = None, params: dict = None, json_data: dict = None):
        for attempt in range(3):
            try:
                response = requests.request(method, url, headers=headers, params=params, json=json_data, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 30))
                    logging.warning(f"RD Marketing: Rate Limit (429). Aguardando {retry_after}s...")
                    time.sleep(retry_after)
                else:
                    logging.error(f"RD Marketing Erro ({response.status_code}): {response.text}")
                    break
            except Exception as e:
                logging.error(f"RD Marketing Conexão: {e}")
                time.sleep(5)
        return None

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS rd_marketing_emails (
                extraction_date Date,
                email_id String,
                email_name String,
                sent UInt32,
                delivered UInt32,
                opened UInt32,
                clicked UInt32,
                bounced UInt32,
                unsubscribed UInt32,
                spammed UInt32,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY (email_id, extraction_date)
            """,
            """
            CREATE TABLE IF NOT EXISTS rd_marketing_conversions (
                extraction_date Date,
                conversion_identifier String,
                conversions UInt32,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY (conversion_identifier, extraction_date)
            """,
            """
            CREATE TABLE IF NOT EXISTS rd_marketing_webhook_leads (
                uuid String,
                email String,
                name String,
                company String,
                job_title String,
                bio String,
                website String,
                personal_phone String,
                mobile_phone String,
                city String,
                state String,
                country String,
                twitter String,
                facebook String,
                linkedin String,
                tags String,
                lead_status String,
                lead_stage String,
                last_conversion_date DateTime,
                created_at DateTime,
                updated_at DateTime,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY uuid
            """
        ]

    def _extract_analytics(self, endpoint: str, json_key: str, date_start: datetime, date_stop: datetime) -> pd.DataFrame:
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        all_data = []
        
        current_date = date_start
        while current_date <= date_stop:
            date_str = current_date.strftime("%Y-%m-%d")
            params = {"start_date": date_str, "end_date": date_str}
            url = f"{self.base_url}/platform/analytics/{endpoint}"
            
            data = self._safe_request(url, headers=headers, params=params)
            if data and json_key in data:
                for item in data[json_key]:
                    item["extraction_date"] = date_str
                    all_data.append(item)
            
            current_date += timedelta(days=1)
            time.sleep(0.5) # Anti-ban suave
            
        return pd.json_normalize(all_data) if all_data else pd.DataFrame()

    def _extract_webhook_leads(self) -> pd.DataFrame:
        if not self.x_api_key or not self.alias:
            return pd.DataFrame()
            
        headers = {"X-API-KEY": self.x_api_key, "accept": "application/json"}
        all_leads = []
        offset = 0
        limit = 500
        
        while True:
            url = f"{self.webhook_proxy_url}/api/v1/raw-data/by-company"
            params = {"company_alias": self.alias, "limit": limit, "offset": offset}
            
            data = self._safe_request(url, headers=headers, params=params)
            if not data or "data" not in data or not data["data"]:
                break
                
            all_leads.extend(data["data"])
            
            if len(data["data"]) < limit:
                break
            offset += limit
            
        if not all_leads:
            return pd.DataFrame()
            
        # Achata os dados conforme o legado
        flattened = [self._flatten_dict(l) for l in all_leads]
        return pd.DataFrame(flattened)

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v:
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(self._flatten_dict(item, f"{new_key}_{i + 1}", sep=sep).items())
                        else:
                            items.append((f"{new_key}_{i + 1}", item))
                else:
                    items.append((new_key, None))
            else:
                items.append((new_key, v))
        return dict(items)

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        logging.info(f"RD Marketing: Extraindo dados para {self.alias or 'Global'}")
        
        emails_df = self._extract_analytics("emails", "emails", date_start, date_stop)
        conversions_df = self._extract_analytics("conversions", "conversions", date_start, date_stop)
        leads_df = self._extract_webhook_leads()
        
        return {
            "rd_marketing_emails": emails_df,
            "rd_marketing_conversions": conversions_df,
            "rd_marketing_webhook_leads": leads_df
        }
