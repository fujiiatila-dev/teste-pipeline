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

    # Google Ads
    google_ads_developer_token: Optional[str] = None
    google_ads_client_id: Optional[str] = None
    google_ads_client_secret: Optional[str] = None
    google_ads_refresh_token: Optional[str] = None
    google_ads_login_customer_id: Optional[str] = None
    google_ads_customer_id: Optional[str] = None

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "nalk_worker"
    clickhouse_password: str = "@Datanalk!2025DN"
    clickhouse_database: str = "marketing"

    # Data Lake / MinIO / S3
    minio_endpoint: str = "http://localhost:9000"
    minio_external_endpoint: str = "http://minio:9000" # Nome do container da rede docker (para o clickhouse acessar)
    minio_access_key: str = "admin"
    minio_secret_key: str = "miniopassword123"

    # PayTour
    paytour_email: Optional[str] = None
    paytour_password: Optional[str] = None
    paytour_loja_id: Optional[str] = None
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

    # Asaas
    asaas_api_url: Optional[str] = None
    asaas_access_token: Optional[str] = None

    # Eduzz
    eduzz_api_url: Optional[str] = None
    eduzz_auth_token: Optional[str] = None

    # Piperun
    piperun_api_url: Optional[str] = None
    piperun_api_token: Optional[str] = None

    # Ploomes
    ploomes_user_key: Optional[str] = None

    # Hotmart
    hotmart_basic_auth: Optional[str] = None
    hotmart_days_to_fetch: int = 365

    # ClickUp
    clickup_bearer_token: Optional[str] = None
    clickup_workspace_id: Optional[str] = None
    clickup_view_id: Optional[str] = None

    # Native (ExpoConecta)
    native_api_url: Optional[str] = None
    native_username: Optional[str] = None
    native_password: Optional[str] = None
    native_report_ids: Optional[str] = None

    # Brevo
    brevo_api_key: Optional[str] = None

    # Sigavi
    sigavi_api_url: Optional[str] = None
    sigavi_username: Optional[str] = None
    sigavi_password: Optional[str] = None

    # Digisac
    digisac_api_url: Optional[str] = None
    digisac_token: Optional[str] = None

    # Arbo
    arbo_token_leads: Optional[str] = None
    arbo_token_imoveis: Optional[str] = None
    arbo_api_url_leads: Optional[str] = None
    arbo_api_url_imoveis: Optional[str] = None

    # CVCRM (CVDW + CVIO)
    cvcrm_api_dominio: Optional[str] = None
    cvcrm_email: Optional[str] = None
    cvcrm_token: Optional[str] = None

    # Superlogica
    superlogica_app_token: Optional[str] = None
    superlogica_access_token: Optional[str] = None
    superlogica_api_url: Optional[str] = None

    # Contact2Sale (C2S)
    c2s_api_url: Optional[str] = None
    c2s_token: Optional[str] = None

    # Groner
    groner_api_url: Optional[str] = None
    groner_token: Optional[str] = None

    # Acert
    acert_token: Optional[str] = None
    acert_api_url: Optional[str] = None
    acert_store_ids: Optional[str] = None

    # Belle
    belle_token: Optional[str] = None
    belle_api_url: Optional[str] = None
    belle_establishments: Optional[str] = None

    # Facilita
    facilita_token: Optional[str] = None
    facilita_instance: Optional[str] = None
    facilita_api_key: Optional[str] = None
    facilita_token_user: Optional[str] = None

    # Everflow
    everflow_api_url: Optional[str] = None
    everflow_token: Optional[str] = None

    # Evo (W12App)
    evo_username: Optional[str] = None
    evo_password: Optional[str] = None
    evo_api_url: Optional[str] = None

    # Hypnobox
    hypnobox_login: Optional[str] = None
    hypnobox_password: Optional[str] = None
    hypnobox_subdomain: Optional[str] = None

    # Imobzi
    imobzi_api_secret: Optional[str] = None
    imobzi_api_url: Optional[str] = None

    # Clicksign
    clicksign_token: Optional[str] = None
    clicksign_api_url: Optional[str] = None

    # Vindi
    vindi_api_token: Optional[str] = None
    vindi_api_url: Optional[str] = None

    # Moskit
    moskit_api_key: Optional[str] = None
    moskit_api_url: Optional[str] = None

    # Mautic
    mautic_base_url: Optional[str] = None
    mautic_client_id: Optional[str] = None
    mautic_client_secret: Optional[str] = None

    # Learn Words (MevBrasil)
    learn_words_base_url: Optional[str] = None
    learn_words_client_id: Optional[str] = None
    learn_words_client_secret: Optional[str] = None

    # Alertas & Notificações
    alert_email_to: str = "suporte@nalk.com.br"
    alert_email_from: str = "pipeline@nalk.com.br"
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None

    # Backoffice Webhook
    backoffice_endpoint: Optional[str] = None
    backoffice_auth_token: Optional[str] = None

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
