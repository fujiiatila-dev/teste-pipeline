import requests
import pandas as pd
from datetime import datetime
import logging
import base64
from connectors.base import BaseConnector
from config.settings import settings

class PayTourConnector(BaseConnector):
    def __init__(self, email: str = None, password: str = None, loja_id: int = None, app_key: str = None, app_secret: str = None):
        self.base_url = "https://api.paytour.com.br/v2"
        self.email = email or settings.paytour_email
        self.password = password or settings.paytour_password
        self.loja_id = loja_id or settings.paytour_loja_id
        self.app_key = app_key or settings.paytour_app_key
        self.app_secret = app_secret or settings.paytour_app_secret
        self.access_token = None
        self._authenticate()

    def _authenticate(self):
        """
        Autentica na API do PayTour usando credenciais de aplicativo ou e-mail/senha.
        Prioriza credenciais de aplicativo se disponíveis.
        """
        if self.app_key and self.app_secret:
            auth_str = f"{self.app_key}:{self.app_secret}"
            auth_b64 = base64.b64encode(auth_str.encode()).decode()
            url = f"{self.base_url}/lojas/login?grant_type=application"
            headers = {"Authorization": f"Basic {auth_b64}"}
        elif self.email and self.password:
            auth_str = f"{self.email}:{self.password}"
            auth_b64 = base64.b64encode(auth_str.encode()).decode()
            loja_id = self.loja_id
            url = f"{self.base_url}/lojas/login?grant_type=password&loja_id={loja_id}"
            headers = {"Authorization": f"Basic {auth_b64}"}
        else:
            raise ValueError("Credenciais do PayTour não configuradas")

        response = requests.post(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        self.access_token = data.get("access_token")
        logging.info("PayTour: Autenticado com sucesso.")

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def _fetch_paginated(self, endpoint, params=None):
        """
        Busca dados em um endpoint paginado.
        """
        all_items = []
        page = 1
        per_page = 100
        
        if params is None:
            params = {}
            
        params["perPage"] = per_page

        while True:
            params["page"] = page
            response = requests.get(f"{self.base_url}/{endpoint}", headers=self._get_headers(), params=params)
            response.raise_for_status()
            data = response.json()
            
            items = data.get("itens", [])
            if not items:
                break
                
            all_items.extend(items)
            
            # Verificar se há mais páginas
            info = data.get("info", {})
            total_pages = info.get("total_paginas", 1)
            if page >= total_pages:
                break
                
            page += 1
            
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS paytour_orders (
                id String,
                loja_id String,
                valor Float64,
                desconto Float64,
                cliente_nome String,
                cliente_email String,
                cliente_cpf String,
                data_hora_pedido DateTime,
                data_hora_atualizacao DateTime,
                status String,
                status_pagamento String,
                metodo_pagamento String,
                utm_source String,
                utm_medium String,
                utm_campaign String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS paytour_order_items (
                id String,
                pedido_id String,
                valor Float64,
                produto_id String,
                produto_tipo String,
                data_utilizacao Date,
                quantidade UInt32,
                nome_produto String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS paytour_tours (
                id UInt64,
                nome String,
                codigo String,
                preco_exibicao Float64,
                ativo UInt8,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS paytour_combos (
                id String,
                nome String,
                valor Float64,
                ativo UInt8,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS paytour_coupons (
                id String,
                codigo String,
                tipo String,
                valor Float64,
                ativo UInt8,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        logging.info(f"PayTour: Extraindo dados de {date_start.date()} até {date_stop.date()}")
        
        # 1. Pedidos (e itens de pedidos)
        # O API do Paytour suporta updatedAtMin/Max no formato YYYY-MM-DD HH:mm:ss
        params_pedidos = {
            "updatedAtMin": date_start.strftime("%Y-%m-%d 00:00:00"),
            "updatedAtMax": date_stop.strftime("%Y-%m-%d 23:59:59"),
            "includeItems": 1
        }
        raw_pedidos = self._fetch_paginated("pedidos", params=params_pedidos)
        
        orders = []
        order_items = []
        
        for p in raw_pedidos:
            orders.append({
                "id": str(p.get("id")),
                "loja_id": str(p.get("loja_id")),
                "valor": float(p.get("valor", 0)),
                "desconto": float(p.get("desconto", 0)),
                "cliente_nome": p.get("cliente_nome"),
                "cliente_email": p.get("cliente_email"),
                "cliente_cpf": p.get("cliente_cpf"),
                "data_hora_pedido": p.get("data_hora_pedido"),
                "data_hora_atualizacao": p.get("data_hora_atualizacao"),
                "status": p.get("status"),
                "status_pagamento": p.get("status_pagamento"),
                "metodo_pagamento": p.get("metodo_pagamento"),
                "utm_source": p.get("utm_source"),
                "utm_medium": p.get("utm_medium"),
                "utm_campaign": p.get("utm_campaign")
            })
            
            for item in p.get("itens", []):
                order_items.append({
                    "id": str(item.get("id")),
                    "pedido_id": str(p.get("id")),
                    "valor": float(item.get("valor", 0)),
                    "produto_id": str(item.get("produto_id")),
                    "produto_tipo": item.get("produto_tipo"),
                    "data_utilizacao": item.get("data_utilizacao"),
                    "quantidade": int(item.get("quantidade", 1)),
                    "nome_produto": item.get("nome_produto")
                })

        # 2. Passeios (Metadata)
        raw_tours = self._fetch_paginated("passeios")
        tours = []
        for t in raw_tours:
            tours.append({
                "id": int(t.get("id")),
                "nome": t.get("nome"),
                "codigo": t.get("codigo"),
                "preco_exibicao": float(t.get("preco_exibicao", 0)),
                "ativo": 1 if t.get("ativo") else 0
            })

        # 3. Combos (Metadata)
        raw_combos = self._fetch_paginated("combos")
        combos = []
        for c in raw_combos:
            combos.append({
                "id": str(c.get("id")),
                "nome": c.get("nome"),
                "valor": float(c.get("valor", 0)),
                "ativo": 1 if c.get("ativo") else 0
            })

        # 4. Cupons (Metadata)
        raw_coupons = self._fetch_paginated("cupons")
        coupons = []
        for cp in raw_coupons:
            coupons.append({
                "id": str(cp.get("id")),
                "codigo": cp.get("code"),
                "tipo": cp.get("type"),
                "valor": float(cp.get("value", 0)),
                "ativo": 1 if cp.get("active") else 0 # Verificar se campo é active
            })

        return {
            "paytour_orders": pd.DataFrame(orders),
            "paytour_order_items": pd.DataFrame(order_items),
            "paytour_tours": pd.DataFrame(tours),
            "paytour_combos": pd.DataFrame(combos),
            "paytour_coupons": pd.DataFrame(coupons)
        }
