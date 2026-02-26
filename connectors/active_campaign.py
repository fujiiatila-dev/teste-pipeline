import time
import requests
import pandas as pd
from typing import List, Dict, Any, Optional

class ActiveCampaignConnector:
    """
    Controlador para extração de dados da API v3 do ActiveCampaign.
    Focado em deals, stages e contatos com paginação via offset.
    """
    
    def __init__(self, account_name: str, api_token: str):
        self.base_url = f"https://{account_name}.api-us1.com/api/3"
        self.headers = {"Api-Token": api_token}
        self.limit = 100

    def _fetch_all_pages(self, endpoint: str, data_key: str) -> List[Dict]:
        """Busca todas as páginas de um endpoint AC v3."""
        all_items = []
        offset = 0
        while True:
            params = {"limit": self.limit, "offset": offset}
            url = f"{self.base_url}/{endpoint}"
            
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    items = data.get(data_key, [])
                    all_items.extend(items)
                    
                    total = int(data.get('meta', {}).get('total', 0))
                    if offset + self.limit >= total or not items:
                        break
                    
                    offset += self.limit
                    # Rate limit AC v3 é de 5 req/s. 
                    time.sleep(0.25)
                elif response.status_code == 429:
                    print("Rate limit atingido. Aguardando 60s...")
                    time.sleep(60)
                else:
                    print(f"Erro AC ({response.status_code}): {response.text}")
                    break
            except Exception as e:
                print(f"Erro de conexão AC: {e}")
                time.sleep(5)
                
        return all_items

    def extract_deals(self) -> pd.DataFrame:
        deals = self._fetch_all_pages("deals", "deals")
        return pd.DataFrame(deals)

    def extract_stages(self) -> pd.DataFrame:
        stages = self._fetch_all_pages("dealStages", "dealStages")
        return pd.DataFrame(stages)

    def extract_contacts(self) -> pd.DataFrame:
        contacts = self._fetch_all_pages("contacts", "contacts")
        return pd.DataFrame(contacts)

    def extract_all(self) -> Dict[str, pd.DataFrame]:
        return {
            "ac_deals": self.extract_deals(),
            "ac_stages": self.extract_stages(),
            "ac_contacts": self.extract_contacts()
        }
