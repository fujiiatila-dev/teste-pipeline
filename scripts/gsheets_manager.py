import pandas as pd
import requests
import io

class GSheetsManager:
    """
    Gerenciador de leitura de planilhas Google Sheets via exportação CSV.
    """
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self.base_url = "https://docs.google.com/spreadsheets/d"

    def _get_csv_url(self, gid: str) -> str:
        return f"{self.base_url}/{self.sheet_id}/export?format=csv&gid={gid}"

    def get_tab_data(self, gid: str) -> list:
        url = self._get_csv_url(gid)
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            df = pd.read_csv(io.StringIO(response.text))
            
            # Remove colunas totalmente vazias e linhas nulas
            df = df.dropna(how='all').fillna('')
            
            data = df.to_dict('records')
            valid_clients = []
            
            for row in data:
                # Se não tem project_id ou company_id, tentamos gerar um a partir do nome ou campos disponíveis
                client_name = str(row.get('project_id') or row.get('company_name') or row.get('name') or 'unknown')
                
                # Geramos um slug determinístico para ser o ID interno
                generated_id = client_name.lower().replace(' ', '_').replace('-', '_').strip()
                
                if not row.get('project_id'):
                    row['project_id'] = generated_id
                
                if not row.get('company_id'):
                    row['company_id'] = generated_id
                
                # Se tivermos pelo menos um ID gerado ou vindo da planilha, consideramos válido
                if row['project_id'] != 'unknown':
                    valid_clients.append(row)
            
            return valid_clients
        except Exception as e:
            print(f"Erro ao ler tab {gid} da planilha: {e}")
            return []
