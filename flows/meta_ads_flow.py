from prefect import flow, task
from connectors.meta_ads import MetaAdsConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
from datetime import datetime, timedelta
import pandas as pd
import logging

@task(retries=3, retry_delay_seconds=60)
def extract_meta_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = MetaAdsConnector(
        app_id=credentials.get("meta_app_id") or credentials.get("app_id") or "1617411648834739", # ID padrão do projeto Meta
        app_secret=credentials.get("meta_app_secret") or credentials.get("app_secret"),
        access_token=credentials.get("access_token") or credentials.get("meta_access_token"),
        account_ids=credentials.get("accounts_ids") or credentials.get("meta_ad_account_ids") or credentials.get("account_ids")
    )
    data = connector.extract(date_start, date_stop)
    
    # Injetando infos adicionais de negócio
    for df in data.values():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "unknown-client")
             
    return data

@task
def create_tables():
    ch = ClickHouseClient()
    connector = MetaAdsConnector()
    ch.create_database()
    ch.run_ddl(connector.get_tables_ddl())

@task
def load_to_clickhouse(data_dict: dict, credentials: dict, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    company_name = credentials.get("project_id", "unknown-client")
    date_path = dt_stop.strftime('%Y%m%d')

    for table_name, df in data_dict.items():
        if not df.empty:
            # Converter colunas numéricas
            if table_name == "meta_ad_insights":
                numeric_cols = ['impressions', 'clicks', 'spend', 'reach', 'ctr', 'cpc', 'cpm']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Datalake
            bucket = "raw-data"
            s3_key = f"meta_ads/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Upload Meta Ads MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            
            # ClickHouse Ingestion
            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)

@flow(name="Meta Ads to ClickHouse")
def meta_ads_pipeline(date_start: str = None, date_stop: str = None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_META_ADS = "1615208458" # Atualizado conforme URL do usuário

    print(f"Buscando configurações na Planilha ID: {SHEET_ID}")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients_to_run = manager.get_tab_data(gid=GID_META_ADS)
    
    if not clients_to_run:
        print("Nenhum cliente válido encontrado para Meta Ads.")
        return

    if not date_start:
        date_start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_tables()

    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando Meta Ads: {company_name}")
        
        try:
            data = extract_meta_data(dt_start, dt_stop, credentials=client)
            load_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar Meta Ads para {company_name}: {e}")

if __name__ == "__main__":
    meta_ads_pipeline()
