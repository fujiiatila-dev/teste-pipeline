import requests
import pandas as pd
from datetime import datetime
import logging
from connectors.base import BaseConnector
from config.settings import settings

class SilbeckConnector(BaseConnector):
    def __init__(self):
        # TODO: Confirmar a URL base oficial com o suporte da Silbeck
        # Comumente segue o padrão api.silbeck.com.br ou ws.silbeck.com.br
        self.base_url = "https://api.silbeck.com.br/v1" 
        self.token = settings.silbeck_token

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def _fetch_data(self, endpoint, params=None):
        """
        Realiza a chamada para a API da Silbeck.
        """
        url = f"{self.base_url}/{endpoint.strip('/')}"
        try:
            response = requests.get(url, headers=self._get_headers(), params=params)
            
            if response.status_code == 401:
                logging.error("Silbeck: Token inválido ou expirado.")
                return []
                
            response.raise_for_status()
            data = response.json()
            
            # Ajustar conforme estrutura real (ex: data['items'] ou direto uma lista)
            if isinstance(data, list):
                return data
            return data.get("data", data.get("reservas", []))
            
        except Exception as e:
            logging.error(f"Erro ao buscar dados da Silbeck ({endpoint}): {str(e)}")
            return []

    def get_tables_ddl(self) -> list:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS silbeck_reservas (
                id String,
                codigo_reserva String,
                data_reserva DateTime,
                status String,
                valor_total Float64,
                cliente_nome String,
                cliente_email String,
                vendedor String,
                origem String,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS silbeck_clientes (
                id String,
                nome String,
                email String,
                cpf_cnpj String,
                telefone String,
                cidade String,
                estado String,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        logging.info(f"Silbeck: Extraindo dados desde {date_start.date()}")
        
        # Filtros típicos por período
        params = {
            "data_inicio": date_start.strftime("%Y-%m-%d"),
            "data_fim": date_stop.strftime("%Y-%m-%d")
        }

        # 1. Reservas
        raw_reservas = self._fetch_data("reservas", params=params)
        reservas = []
        for r in raw_reservas:
            # Mapeamento provisório baseado em campos comuns de ERP de turismo
            reservas.append({
                "id": str(r.get("id")),
                "codigo_reserva": r.get("codigo") or r.get("numero"),
                "data_reserva": r.get("data_reserva") or r.get("created_at"),
                "status": r.get("status"),
                "valor_total": float(r.get("valor_total") or 0),
                "cliente_nome": r.get("cliente", {}).get("nome") if isinstance(r.get("cliente"), dict) else r.get("cliente_nome"),
                "cliente_email": r.get("cliente", {}).get("email") if isinstance(r.get("cliente"), dict) else r.get("cliente_email"),
                "vendedor": r.get("vendedor"),
                "origem": r.get("origem")
            })

        # 2. Clientes (Geralmente endpoint separado ou extraído das reservas)
        raw_clientes = self._fetch_data("clientes", params=params)
        clientes = []
        for c in raw_clientes:
            clientes.append({
                "id": str(c.get("id")),
                "nome": c.get("nome"),
                "email": c.get("email"),
                "cpf_cnpj": c.get("cpf") or c.get("cnpj"),
                "telefone": c.get("telefone"),
                "cidade": c.get("cidade"),
                "estado": c.get("estado")
            })

        return {
            "silbeck_reservas": pd.DataFrame(reservas),
            "silbeck_clientes": pd.DataFrame(clientes)
        }
