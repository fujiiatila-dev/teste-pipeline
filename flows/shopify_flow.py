from prefect import flow, task
from datetime import datetime, timedelta
import logging
import pandas as pd
from connectors.shopify import ShopifyConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager

@task(retries=3, retry_delay_seconds=60)
def extract_shopify_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = ShopifyConnector(
        shop_name=credentials.get("shopify_shop_name"),
        access_token=credentials.get("shopify_access_token"),
        api_version=credentials.get("shopify_api_version")
    )
    data = connector.extract(date_start, date_stop)
    
    # Injetando infos adicionais de negócio
    for df in data.values():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "unknown-client")
             
    return data

@task
def create_shopify_tables():
    connector = ShopifyConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_shopify_to_clickhouse(data: dict, credentials: dict, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    company_name = credentials.get("project_id", "unknown-client")
    date_path = dt_stop.strftime('%Y%m%d')

    for table_name, df in data.items():
        if not df.empty:
            # Datalake
            bucket = "raw-data"
            s3_key = f"shopify/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Upload Shopify MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            
            # ClickHouse Ingestion
            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)

@flow(name="Shopify to ClickHouse")
def shopify_pipeline(date_start: str = None, date_stop: str = None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_SHOPIFY = "912384756" # Exemplo, o usuário deve confirmar ou ajustar

    print(f"Buscando configurações na Planilha ID: {SHEET_ID}")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients_to_run = manager.get_tab_data(gid=GID_SHOPIFY)
    
    if not clients_to_run:
        print("Nenhum cliente válido encontrado para Shopify.")
        return

    # Default: last 2 days
    if not date_start:
        date_start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_shopify_tables()

    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando Shopify: {company_name}")
        
        try:
            data = extract_shopify_data(dt_start, dt_stop, credentials=client)
            load_shopify_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar Shopify para {company_name}: {e}")

if __name__ == "__main__":
    shopify_pipeline()
