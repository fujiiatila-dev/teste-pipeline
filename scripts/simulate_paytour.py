import pandas as pd
from datetime import datetime, timedelta
import json
from unittest.mock import MagicMock, patch

# Importamos nossas classes (ajustando o path se necessário)
import sys
import os
sys.path.append(os.getcwd())

from connectors.paytour import PayTourConnector
from config.settings import settings

def run_simulation():
    print("=== SIMULAÇÃO DO PIPELINE PAYTOUR ===")
    
    # Mock de dados da API baseado na documentação
    mock_orders_response = {
        "info": {"total": 1, "pagina": 1, "total_paginas": 1},
        "itens": [
            {
                "id": "200",
                "loja_id": "468",
                "valor": "170.00",
                "desconto": "10.00",
                "cliente_nome": "João Carlos da Silva",
                "cliente_email": "joao_carlos@gmail.com",
                "data_hora_pedido": "2026-02-22 09:47:39",
                "status": "aprovado",
                "status_pagamento": "pago",
                "metodo_pagamento": "cartao",
                "utm_source": "google",
                "itens": [
                    {
                        "id": "526",
                        "valor": "160.00",
                        "produto_id": "1",
                        "produto_tipo": "passeio",
                        "data_utilizacao": "2026-02-25",
                        "quantidade": "1",
                        "nome_produto": "Mergulho de Batismo"
                    }
                ]
            }
        ]
    }

    mock_tours_response = {
        "itens": [
            {
                "id": 1,
                "nome": "Mergulho de Batismo",
                "codigo": "MERG01",
                "preco_exibicao": 160.0,
                "ativo": True
            }
        ],
        "info": {"total_paginas": 1}
    }

    # Forçamos credenciais fictícias para o teste
    settings.paytour_app_key = "mock_key"
    settings.paytour_app_secret = "mock_secret"

    with patch('requests.post') as mock_post, patch('requests.get') as mock_get:
        # Mock do Login
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"access_token": "mock_token_123"}
        
        # Mock das chamadas GET (Pedidos, Passeios, etc)
        def side_effect(url, headers, params=None):
            m = MagicMock()
            m.status_code = 200
            if "pedidos" in url:
                m.json.return_value = mock_orders_response
            elif "passeios" in url:
                m.json.return_value = mock_tours_response
            else:
                m.json.return_value = {"itens": [], "info": {"total_paginas": 1}}
            return m
            
        mock_get.side_effect = side_effect

        print("\n1. Iniciando Conector...")
        connector = PayTourConnector()
        
        print("\n2. Executando Extração (Simulando 24h)...")
        dt_start = datetime.now() - timedelta(days=1)
        dt_stop = datetime.now()
        data = connector.extract(dt_start, dt_stop)
        
        print("\n3. Resultados Preparados para o ClickHouse:")
        for table, df in data.items():
            print(f"\n--- Tabela: {table} ---")
            if not df.empty:
                print(df.head().to_string(index=False))
            else:
                print("(Tabela vazia na simulação)")

    print("\n=== SIMULAÇÃO CONCLUÍDA COM SUCESSO ===")

if __name__ == "__main__":
    run_simulation()
