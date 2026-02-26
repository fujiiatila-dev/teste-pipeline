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
            
            # Vamos padronizar o nome de algumas colunas que podem vir com espaços
            df.columns = [str(c).strip() for c in df.columns]

            # Converter para lista de dicionários
            records = df.to_dict(orient='records')
            
            # Filtro essencial: Só aceitar os que tem company_id! (exatamente como você queria)
            valid_records = []
            for r in records:
                if pd.notna(r.get('company_id')) and str(r.get('company_id')).strip() != "":
                    valid_records.append(r)
                    
            return valid_records
            
        except Exception as e:
            print(f"Erro ao ler a planilha {self.sheet_id} aba {gid}: {str(e)}")
            return []
