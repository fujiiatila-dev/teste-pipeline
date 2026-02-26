import time
import pandas as pd
import hubspot
from hubspot.crm.deals import PublicObjectSearchRequest, Filter, FilterGroup
from typing import List, Dict, Any, Optional
from datetime import datetime

class HubSpotConnector:
    """
    Controlador para extração de dados do HubSpot API v3.
    Implementa busca dinâmica de propriedades, mapeamento de deal stages e filtros de data.
    """
    
    def __init__(self, access_token: str):
        self.client = hubspot.Client.create(access_token=access_token)
        self.limit = 100

    def _get_all_properties(self, object_type: str) -> List[str]:
        """Busca todas as propriedades disponíveis para um objeto."""
        try:
            response = self.client.crm.properties.core_api.get_all(object_type)
            return [prop.name for prop in response.results]
        except Exception as e:
            print(f"Erro ao buscar propriedades de {object_type}: {e}")
            return []

    def _create_stage_mapping(self) -> Dict[str, Dict[str, Any]]:
        """Cria mapeamento de stage IDs para labels legíveis."""
        stage_mapping = {}
        try:
            pipelines = self.client.crm.pipelines.pipelines_api.get_all("deals")
            for pipeline in pipelines.results:
                for stage in pipeline.stages:
                    stage_mapping[stage.id] = {
                        'label': stage.label,
                        'pipeline_name': pipeline.label,
                        'probability': getattr(stage.metadata, 'probability', 0)
                    }
            return stage_mapping
        except Exception as e:
            print(f"Erro ao criar mapeamento de deals stages: {e}")
            return {}

    def extract_deals(self, start_date: Optional[str] = None) -> pd.DataFrame:
        """Extrai deals com mapeamento de stages e filtro de data opcional."""
        properties = self._get_all_properties("deals")
        stage_mapping = self._create_stage_mapping()
        
        all_deals = []
        after = None
        
        while True:
            try:
                if start_date:
                    # Filtro de data via Search API
                    start_dt = f"{start_date}T00:00:00.000Z"
                    f = Filter(property_name="createdate", operator="GTE", value=start_dt)
                    search_request = PublicObjectSearchRequest(
                        filter_groups=[FilterGroup(filters=[f])],
                        properties=properties,
                        limit=self.limit,
                        after=after
                    )
                    response = self.client.crm.deals.search_api.do_search(public_object_search_request=search_request)
                else:
                    response = self.client.crm.deals.basic_api.get_page(
                        limit=self.limit, 
                        properties=properties, 
                        after=after
                    )
                
                for deal in response.results:
                    deal_data = deal.properties
                    deal_data['hs_object_id'] = deal.id
                    
                    # Enriquecimento de Stage
                    stage_id = deal_data.get('dealstage')
                    if stage_id in stage_mapping:
                        info = stage_mapping[stage_id]
                        deal_data['dealstage_label'] = info['label']
                        deal_data['pipeline_name'] = info['pipeline_name']
                    
                    all_deals.append(deal_data)
                
                if not response.paging:
                    break
                after = response.paging.next.after
                time.sleep(0.1)
            except Exception as e:
                print(f"Erro na extração de deals: {e}")
                break
                
        return pd.DataFrame(all_deals)

    def extract_contacts(self) -> pd.DataFrame:
        """Extrai contatos com todas as propriedades."""
        properties = self._get_all_properties("contacts")
        all_contacts = []
        after = None
        
        while True:
            try:
                response = self.client.crm.contacts.basic_api.get_page(
                    limit=self.limit, 
                    properties=properties, 
                    after=after
                )
                for contact in response.results:
                    c_data = contact.properties
                    c_data['hs_object_id'] = contact.id
                    all_contacts.append(c_data)
                
                if not response.paging:
                    break
                after = response.paging.next.after
                time.sleep(0.1)
            except Exception as e:
                print(f"Erro na extração de contatos: {e}")
                break
                
        return pd.DataFrame(all_contacts)

    def extract_all(self, start_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """Helper para extrair tudo de uma vez."""
        return {
            "hubspot_deals": self.extract_deals(start_date=start_date),
            "hubspot_contacts": self.extract_contacts()
        }
