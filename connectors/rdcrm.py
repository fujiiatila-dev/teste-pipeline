import time
import requests
import pandas as pd
import unicodedata
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

class RDCRMConnector:
    """
    Controlador para extração de dados do RD Station CRM API.
    Refatorado para lidar com deals, pipelines, stages e contatos.
    """
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = 'https://crm.rdstation.com/api/v1'
        self.limit = 200

    def _normalize_name(self, text: str) -> str:
        """Normaliza nomes de colunas."""
        if not text: return ""
        text = unicodedata.normalize('NFKD', str(text)).encode('ASCII', 'ignore').decode('ASCII').lower()
        text = re.sub(r'[^a-z0-9_]', '_', text)
        return re.sub(r'_+', '_', text).strip('_')

    def _safe_request(self, endpoint: str, params: dict = None) -> Any:
        url = f"{self.base_url}/{endpoint}"
        params = params or {}
        params['token'] = self.token
        
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    time.sleep(60 * (attempt + 1))
                else:
                    print(f"Erro na API RDCRM ({response.status_code}): {response.text}")
                    break
            except Exception as e:
                print(f"Erro de conexão RDCRM: {e}")
                time.sleep(5)
        return None

    def _flatten_deal(self, deal: dict) -> dict:
        """Achata campos aninhados do RD CRM conforme legado."""
        flat = {}
        # Mapeamento básico
        rename_map = {
            'id': 'deal_id',
            'name': 'deal_name',
            'amount_total': 'deal_amount_total',
            'created_at': 'deal_created_at',
            'updated_at': 'deal_updated_at'
        }
        
        for k, v in deal.items():
            if k in rename_map:
                flat[rename_map[k]] = v
            elif k not in ["campaign", "contacts", "deal_custom_fields", "deal_lost_reason", "deal_products", "deal_source", "deal_stage", "organization", "user"]:
                flat[self._normalize_name(k)] = v
                
        # Extrair Relacionais
        if deal.get('deal_stage'):
            flat['deal_stage_name'] = deal['deal_stage'].get('name')
            flat['deal_stage_id'] = deal['deal_stage'].get('id')
        if deal.get('organization'):
            flat['organization_name'] = deal['organization'].get('name')
        if deal.get('user'):
            flat['user_name'] = deal['user'].get('name')
        if deal.get('deal_source'):
            flat['deal_source_name'] = deal['deal_source'].get('name')
            
        # Custom Fields
        for cf in deal.get('deal_custom_fields', []):
            label = self._normalize_name(cf.get('custom_field', {}).get('label', ''))
            if label:
                flat[label] = cf.get('value')
                
        return flat

    def extract_deals(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Coleta deals por período."""
        all_deals = []
        page = 1
        while True:
            params = {
                'created_at_period': 'true',
                'start_date': start_date,
                'end_date': end_date,
                'limit': self.limit,
                'page': page
            }
            data = self._safe_request('deals', params)
            if not data or not data.get('deals'):
                break
                
            for d in data['deals']:
                all_deals.append(self._flatten_deal(d))
                
            if len(data['deals']) < self.limit:
                break
            page += 1
            time.sleep(0.1)
            
        return pd.DataFrame(all_deals)

    def extract_stages(self) -> pd.DataFrame:
        """Coleta pipelines e etapas."""
        stages_data = []
        pipelines = self._safe_request('deal_pipelines')
        if pipelines:
            for p in pipelines:
                p_id = p.get('id')
                p_name = p.get('name')
                stages = self._safe_request('deal_stages', {'deal_pipeline_id': p_id})
                if stages and stages.get('deal_stages'):
                    for s in stages['deal_stages']:
                        stages_data.append({
                            'pipeline_id': p_id,
                            'pipeline_name': p_name,
                            'stage_id': s.get('id'),
                            'stage_name': s.get('name'),
                            'stage_order': s.get('order')
                        })
        return pd.DataFrame(stages_data)

    def extract_all(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        return {
            "rdcrm_deals": self.extract_deals(start_date, end_date),
            "rdcrm_stages": self.extract_stages()
        }
