from pydantic_settings import BaseSettings
from typing import Optional
import os
import logging

class Settings(BaseSettings):
    # Meta Ads
    meta_app_id: Optional[str] = None
    meta_app_secret: Optional[str] = None
    meta_access_token: Optional[str] = None
    meta_ad_account_ids: Optional[str] = None

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "marketing"

    # PayTour
    paytour_email: Optional[str] = None
    paytour_password: Optional[str] = None
    paytour_loja_id: Optional[int] = None
    paytour_app_key: Optional[str] = None
    paytour_app_secret: Optional[str] = None

    # Leads2b
    leads2b_token: Optional[str] = None

    # Shopify
    shopify_shop_name: Optional[str] = None
    shopify_access_token: Optional[str] = None
    shopify_api_version: str = "2024-01"

    # OMIE
    omie_app_key: Optional[str] = None
    omie_app_secret: Optional[str] = None

    # Silbeck
    silbeck_token: Optional[str] = None

    class Config:
        env_file = ".env"

    def get(self, key: str):
        """
        Tenta buscar a configuração:
        1. De uma Variável do Prefect (Dinâmico via UI)
        2. Do valor carregado no objeto (via .env ou env var)
        """
        try:
            from prefect.variables import Variable
            # Tenta buscar no Prefect (Sync)
            pref_var = Variable.get(key, default=None)
            if pref_var is not None:
                return pref_var
        except Exception:
            pass
        
        # Fallback para o valor padrão do Pydantic
        return getattr(self, key, None)

settings = Settings()
