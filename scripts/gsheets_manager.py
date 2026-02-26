import pandas as pd
from typing import List, Dict

class GSheetsManager:
    """
    Controlador Mestre do Pipeline usando Google Sheets.
    Lê uma planilha pública do Google e retorna seus valores como dicionários.
    """
    
    # URL pública base para extração de CSV do Google Drive
    BASE_URL = "https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id

    def get_tab_data(self, gid: str) -> List[Dict]:
        """
        Lê uma aba específica (pelo GID) e retorna a lista de linhas válidas em formato Dict.
        Ignora linhas sem 'company_id'.
        """
        url = self.BASE_URL.format(sheet_id=self.sheet_id, gid=gid)
        try:
            # Lendo direto com o Pandas da URL pública CSV
            df = pd.read_csv(url)
            
            # Limpa colunas ou linhas totalmente vazias
            df.dropna(how='all', inplace=True)
            
            # Remove colunas totalmente vazias e linhas nulas
            df = df.dropna(how='all').fillna('')
            
            data = df.to_dict('records')
            valid_clients = []
            
            for row in data:
                # Se não tem project_id ou company_id, tentamos gerar um a partir do nome ou campos disponíveis
                # Isso permite que o pipeline rode mesmo sem o ID da API legada
                client_name = row.get('project_id') or row.get('company_name') or row.get('name') or 'unknown'
                
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

```
