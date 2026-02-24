from pydantic_settings import BaseSettings
from typing import List, Optional

class Settings(BaseSettings):
    # Meta Ads
    meta_app_id: Optional[str] = None
    meta_app_secret: Optional[str] = None
    meta_access_token: Optional[str] = None
    meta_ad_account_ids: Optional[str] = None # CSV string: act_1,act_2

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

settings = Settings()
