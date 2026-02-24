import requests
import pandas as pd
from datetime import datetime
import logging
from connectors.base import BaseConnector
from config.settings import settings

class OmieConnector(BaseConnector):
    def __init__(self):
        self.base_url = "https://app.omie.com.br/api/v1"
        self.app_key = settings.omie_app_key
        self.app_secret = settings.omie_app_secret

    def _call_api(self, endpoint, call, param=None):
        """
        Executa uma chamada POST para a API do Omie.
        """
        if param is None:
            param = [{}]
            
        payload = {
            "call": call,
            "app_key": self.app_key,
            "app_secret": self.app_secret,
            "param": param
        }
        
        url = f"{self.base_url}/{endpoint.strip('/')}/"
        response = requests.post(url, json=payload)
        
        if response.status_code != 200:
            logging.error(f"Omie API Error: {response.status_code} - {response.text}")
            response.raise_for_status()
            
        return response.json()

    def _fetch_paginated(self, endpoint, call, filter_params=None):
        """
        Busca dados de forma paginada para o Omie.
        """
        all_items = []
        page = 1
        records_per_page = 100
        
        while True:
            param = {
                "pagina": page,
                "registros_por_pagina": records_per_page,
                "apenas_importado_api": "N"
            }
            if filter_params:
                param.update(filter_params)
                
            data = self._call_api(endpoint, call, param=[param])
            
            # A API do Omie retorna os itens em uma chave específica dependendo do call
            # Ex: ListarClientes -> clientes_cadastro
            # Vamos tentar inferir ou usar chaves comuns
            items = []
            for key in data.keys():
                if isinstance(data[key], list) and key != "param":
                    items = data[key]
                    break
            
            if not items:
                break
                
            all_items.extend(items)
            
            if page >= data.get("total_de_paginas", 0):
                break
                
            page += 1
            if page > 500: # Safety break
                break
                
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS omie_clientes (
                codigo_cliente_omie Int64,
                codigo_cliente_integracao String,
                razao_social String,
                nome_fantasia String,
                cnpj_cpf String,
                email String,
                telefone1_ddd String,
                telefone1_numero String,
                contato String,
                endereco String,
                bairro String,
                cidade String,
                estado String,
                cep String,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY codigo_cliente_omie
            """,
            f"""
            CREATE TABLE IF NOT EXISTS omie_pedidos (
                numero_pedido Int64,
                codigo_pedido_omie Int64,
                codigo_cliente Int64,
                data_previsao DateTime,
                etapa String,
                valor_total Float64,
                status_pedido String,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY codigo_pedido_omie
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        logging.info(f"Omie: Extraindo dados desde {date_start.date()}")
        
        # Filtros por data costumam ser via param no Omie
        # Ex: "filtrar_por_data_de": "2023-01-01"
        filter_params = {
            "filtrar_por_data_de": date_start.strftime("%d/%m/%Y"),
            "filtrar_por_data_ate": date_stop.strftime("%d/%m/%Y")
        }

        # 1. Clientes
        # Nota: ListarClientes pode não aceitar filtro de data da mesma forma, 
        # mas muitas vezes queremos todos ou os alterados.
        raw_clientes = self._fetch_paginated("geral/clientes", "ListarClientes")
        clientes = []
        for c in raw_clientes:
            clientes.append({
                "codigo_cliente_omie": c.get("codigo_cliente_omie"),
                "codigo_cliente_integracao": c.get("codigo_cliente_integracao"),
                "razao_social": c.get("razao_social"),
                "nome_fantasia": c.get("nome_fantasia"),
                "cnpj_cpf": c.get("cnpj_cpf"),
                "email": c.get("email"),
                "telefone1_ddd": c.get("telefone1_ddd"),
                "telefone1_numero": c.get("telefone1_numero"),
                "contato": c.get("contato"),
                "endereco": c.get("endereco"),
                "bairro": c.get("bairro"),
                "cidade": c.get("cidade"),
                "estado": c.get("estado"),
                "cep": c.get("cep")
            })

        # 2. Pedidos de Venda
        raw_pedidos = self._fetch_paginated("produtos/pedido", "ListarPedidosVenda", filter_params)
        pedidos = []
        for p in raw_pedidos:
            cabecalho = p.get("cabecalho", {})
            total = p.get("total_pedido", {})
            pedidos.append({
                "numero_pedido": cabecalho.get("numero_pedido"),
                "codigo_pedido_omie": cabecalho.get("codigo_pedido_omie"),
                "codigo_cliente": cabecalho.get("codigo_cliente"),
                "data_previsao": cabecalho.get("data_previsao"),
                "etapa": cabecalho.get("etapa"),
                "valor_total": float(total.get("valor_total_pedido", 0)),
                "status_pedido": p.get("infoCadastro", {}).get("cancelado") == "S" and "Cancelado" or "Ativo"
            })

        return {
            "omie_clientes": pd.DataFrame(clientes),
            "omie_pedidos": pd.DataFrame(pedidos)
        }
