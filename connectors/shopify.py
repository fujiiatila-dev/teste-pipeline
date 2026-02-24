import requests
import pandas as pd
from datetime import datetime
import logging
import re
from connectors.base import BaseConnector
from config.settings import settings

class ShopifyConnector(BaseConnector):
    def __init__(self):
        self.shop_name = settings.shopify_shop_name
        self.access_token = settings.shopify_access_token
        self.api_version = settings.shopify_api_version
        self.base_url = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}"

    def _get_headers(self):
        return {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json"
        }

    def _fetch_paginated(self, endpoint, params=None):
        """
        Busca dados de forma paginada para o Shopify usando paginação baseada em cursor (Link header).
        """
        all_items = []
        url = f"{self.base_url}/{endpoint}.json"
        
        if params is None:
            params = {}
        
        # Shopify default limit is 50, max is 250
        if "limit" not in params:
            params["limit"] = 250

        while url:
            response = requests.get(url, headers=self._get_headers(), params=params)
            response.raise_for_status()
            data = response.json()
            
            # O Shopify retorna o recurso como uma chave (ex: {'orders': [...]})
            resource_key = endpoint.split("/")[-1]
            items = data.get(resource_key, [])
            all_items.extend(items)
            
            # Paginação via Link Header
            link_header = response.headers.get("Link")
            if link_header:
                # O header Link contém urls para 'next' e 'previous'
                match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                if match:
                    url = match.group(1)
                    params = None # Os parâmetros já estão na URL do 'next'
                else:
                    url = None
            else:
                url = None
            
        return all_items

    def get_tables_ddl(self) -> list:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS shopify_orders (
                id String,
                order_number Int64,
                email String,
                total_price Float64,
                subtotal_price Float64,
                total_tax Float64,
                total_discounts Float64,
                currency String,
                financial_status String,
                fulfillment_status String,
                created_at DateTime,
                updated_at DateTime,
                customer_id String,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS shopify_order_items (
                id String,
                order_id String,
                variant_id String,
                product_id String,
                title String,
                quantity Int32,
                price Float64,
                sku String,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS shopify_customers (
                id String,
                email String,
                first_name String,
                last_name String,
                orders_count Int32,
                total_spent Float64,
                created_at DateTime,
                updated_at DateTime,
                ingestion_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(ingestion_at)
            ORDER BY id
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        logging.info(f"Shopify: Extraindo dados atualizados desde {date_start.date()}")
        
        # Filtros de data para pedidos
        # status=any inclui cancelados e fechados
        params_orders = {
            "updated_at_min": date_start.isoformat(),
            "updated_at_max": date_stop.isoformat(),
            "status": "any"
        }
        
        # 1. Pedidos
        raw_orders = self._fetch_paginated("orders", params=params_orders)
        
        orders = []
        order_items = []
        
        for o in raw_orders:
            orders.append({
                "id": str(o.get("id")),
                "order_number": o.get("order_number"),
                "email": o.get("email"),
                "total_price": float(o.get("total_price", 0)),
                "subtotal_price": float(o.get("subtotal_price", 0)),
                "total_tax": float(o.get("total_tax", 0)),
                "total_discounts": float(o.get("total_discounts", 0)),
                "currency": o.get("currency"),
                "financial_status": o.get("financial_status"),
                "fulfillment_status": o.get("fulfillment_status"),
                "created_at": o.get("created_at"),
                "updated_at": o.get("updated_at"),
                "customer_id": str(o.get("customer", {}).get("id")) if o.get("customer") else None
            })
            
            for line in o.get("line_items", []):
                order_items.append({
                    "id": str(line.get("id")),
                    "order_id": str(o.get("id")),
                    "variant_id": str(line.get("variant_id")),
                    "product_id": str(line.get("product_id")),
                    "title": line.get("title"),
                    "quantity": int(line.get("quantity", 1)),
                    "price": float(line.get("price", 0)),
                    "sku": line.get("sku")
                })

        # 2. Clientes (também filtrados por data de atualização se possível)
        params_customers = {
            "updated_at_min": date_start.isoformat(),
            "updated_at_max": date_stop.isoformat()
        }
        raw_customers = self._fetch_paginated("customers", params=params_customers)
        customers = []
        for c in raw_customers:
            customers.append({
                "id": str(c.get("id")),
                "email": c.get("email"),
                "first_name": c.get("first_name"),
                "last_name": c.get("last_name"),
                "orders_count": int(c.get("orders_count", 0)),
                "total_spent": float(c.get("total_spent", 0)),
                "created_at": c.get("created_at"),
                "updated_at": c.get("updated_at")
            })

        return {
            "shopify_orders": pd.DataFrame(orders),
            "shopify_order_items": pd.DataFrame(order_items),
            "shopify_customers": pd.DataFrame(customers)
        }
