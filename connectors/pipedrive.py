import time
import requests
import re
import unicodedata
import pandas as pd
from typing import Dict, Any, List

class PipedriveConnector:
    """
    Conector nativo do Pipedrive V2 para o pipelines de Datalake da Nalk.
    Refatorado do legado do Airflow para uso in-memory via Pandas -> Parquet.
    """
    def __init__(self, api_domain: str, api_token: str):
        self.api_domain = api_domain
        self.api_token = api_token
        self.base_url = f'https://{api_domain}.pipedrive.com/api'

    def _normalize_name(self, name: str) -> str:
        """Normaliza o nome removendo acentos, espaços e caracteres especiais"""
        if not isinstance(name, str):
            return str(name)
        name = unicodedata.normalize('NFD', name)
        name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
        name = name.lower()
        name = re.sub(r'\s+', ' ', name.strip())
        name = name.replace(' ', '_')
        name = re.sub(r'[^a-z0-9_]', '', name)
        return name

    def _clean_text_data(self, value: Any) -> Any:
        if isinstance(value, str):
            value = re.sub(r'\r?\n+', ' ', value)
            value = re.sub(r'\s+', ' ', value.strip())
        return value

    def _fetch_all(self, endpoint: str, version: str = 'v1', base_params: dict = None) -> List[Dict]:
        """Faz requests com paginação para a API (V1 e V2 do Pipedrive)"""
        all_data = []
        url = f"{self.base_url}/{version}/{endpoint}"
        params = base_params or {}
        params['api_token'] = self.api_token

        if version == 'v1':
            start = 0
            while True:
                params['start'] = start
                params['limit'] = 100
                try:
                    res = requests.get(url, params=params)
                    res.raise_for_status()
                    data = res.json()
                except Exception as e:
                    print(f"Erro no request do Pipedrive {endpoint}: {str(e)}")
                    break

                items = data.get('data') or []
                all_data.extend(items)

                pagination = data.get('additional_data', {}).get('pagination', {})
                if not pagination.get('more_items_in_collection'):
                    break
                start = pagination.get('next_start', start + 100)
                time.sleep(0.1)
        else:
            cursor = None
            while True:
                params['limit'] = 100
                if cursor:
                    params['cursor'] = cursor
                try:
                    res = requests.get(url, params=params)
                    res.raise_for_status()
                    data = res.json()
                except Exception as e:
                    print(f"Erro no request do Pipedrive {endpoint}: {str(e)}")
                    break

                items = data.get('data') or []
                all_data.extend(items)

                cursor = data.get('additional_data', {}).get('next_cursor')
                if not cursor:
                    break
                time.sleep(0.1)

        return all_data

    def _create_field_mappings(self, fields_data: List[Dict]) -> tuple:
        field_map = {}
        options_map = {}
        for field in fields_data:
            # Normalizar direto o nome do campo
            normalized_name = self._normalize_name(field.get('name', ''))
            field_map[field['key']] = normalized_name

            if field.get('field_type') == 'enum' and field.get('options'):
                field_key = field['key']
                options_map[field_key] = {
                    str(opt['id']): opt['label']
                    for opt in field['options']
                }
        return field_map, options_map

    def _flatten_custom_fields(self, data: List[Dict], field_map: dict, options_map: dict, is_lead=False) -> List[Dict]:
        """Achata o objeto removendo hashes e substituindo para os nomes reais"""
        flattened_data = []
        for item in data:
            flat_item = {}
            # Copiar os campos normais e limpar strings longas
            for k, v in item.items():
                if k != 'custom_fields' and type(v) not in (dict, list):
                    flat_item[k] = self._clean_text_data(v)
            
            # Achatar 'custom_fields' (no modelo padrao)
            if 'custom_fields' in item and isinstance(item['custom_fields'], dict):
                for key, value in item['custom_fields'].items():
                    field_name = field_map.get(key, key)
                    if key in options_map and value is not None:
                        value_str = str(value)
                        value = options_map[key].get(value_str, value)
                    flat_item[field_name] = self._clean_text_data(value)

            # Para leads, os campos customizados vem no raiz usando os hashes como key (ex: a0f8c...)
            if is_lead:
                sha1_keys = [k for k in item.keys() if len(k) == 40 and all(c in '0123456789abcdef' for c in str(k).lower())]
                for key in sha1_keys:
                    if key in field_map:
                        field_name = field_map[key]
                        value = item[key]
                        if key in options_map and value is not None:
                            value_str = str(value)
                            value = options_map[key].get(value_str, value)
                        flat_item[field_name] = self._clean_text_data(value)

            flattened_data.append(flat_item)
        return flattened_data

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """Executa o pipeline completo reproduzindo as queries do Airflow e convertendo para DataFrames limpos"""
        print("Iniciando extração do Pipedrive (Fields)")
        # 1. Obter Schemas
        org_fields = self._fetch_all('organizationFields', 'v1')
        deal_fields = self._fetch_all('dealFields', 'v1')
        person_fields = self._fetch_all('personFields', 'v1')
        activity_fields = self._fetch_all('activityFields', 'v1')
        product_fields = self._fetch_all('productFields', 'v1')
        lead_fields = self._fetch_all('leadFields', 'v1')

        # Mapas de Tradução
        org_map, org_opts = self._create_field_mappings(org_fields)
        deal_map, deal_opts = self._create_field_mappings(deal_fields)
        person_map, person_opts = self._create_field_mappings(person_fields)
        activity_map, activity_opts = self._create_field_mappings(activity_fields)
        product_map, product_opts = self._create_field_mappings(product_fields)
        lead_map, lead_opts = self._create_field_mappings(lead_fields)

        print("Iniciando extração do Pipedrive (Dados brutos de Vendas e Cadastros)")
        # 2. Obter Dados Base
        pipelines = self._fetch_all('pipelines', 'v2')
        stages = self._fetch_all('stages', 'v2')
        users = self._fetch_all('users', 'v1')
        labels = self._fetch_all('leadLabels', 'v1')

        organizations = self._fetch_all('organizations', 'v2')
        persons = self._fetch_all('persons', 'v2')
        products = self._fetch_all('products', 'v2')
        deals = self._fetch_all('deals', 'v2')
        activities = self._fetch_all('activities', 'v2')
        leads = self._fetch_all('leads', 'v1')

        print("Achatando as variaveis customizaveis HASH -> Names")
        # 3. Flatten e Normalize dos objetos difíceis
        org_flat = self._flatten_custom_fields(organizations, org_map, org_opts)
        pers_flat = self._flatten_custom_fields(persons, person_map, person_opts)
        prod_flat = self._flatten_custom_fields(products, product_map, product_opts)
        deals_flat = self._flatten_custom_fields(deals, deal_map, deal_opts)
        activ_flat = self._flatten_custom_fields(activities, activity_map, activity_opts)
        leads_flat = self._flatten_custom_fields(leads, lead_map, lead_opts, is_lead=True)

        return {
            "pipedrive_pipelines": pd.DataFrame(pipelines),
            "pipedrive_stages": pd.DataFrame(stages),
            "pipedrive_users": pd.DataFrame(users),
            "pipedrive_lead_labels": pd.DataFrame(labels),
            "pipedrive_organizations": pd.DataFrame(org_flat),
            "pipedrive_persons": pd.DataFrame(pers_flat),
            "pipedrive_products": pd.DataFrame(prod_flat),
            "pipedrive_deals": pd.DataFrame(deals_flat),
            "pipedrive_activities": pd.DataFrame(activ_flat),
            "pipedrive_leads": pd.DataFrame(leads_flat)
        }
