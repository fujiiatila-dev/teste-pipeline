from prefect import flow, task
from datetime import datetime, timedelta
import logging
import pandas as pd
from connectors.paytour import PayTourConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager

@task(retries=3, retry_delay_seconds=60)
def extract_paytour_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = PayTourConnector(
        email=credentials.get("paytour_email"),
        password=credentials.get("paytour_password"),
        loja_id=credentials.get("paytour_loja_id"),
        app_key=credentials.get("paytour_app_key"),
        app_secret=credentials.get("paytour_app_secret")
    )
    data = connector.extract(date_start, date_stop)
    
    # Injetando infos adicionais de negócio
    for df in data.values():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "unknown-client")
             
    return data

@task
def create_paytour_tables():
    connector = PayTourConnector() # Usa settings padrão para DDL se necessário, ou mock
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_paytour_to_clickhouse(data: dict, credentials: dict, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    company_name = credentials.get("project_id", "unknown-client")
    date_path = dt_stop.strftime('%Y%m%d')

    for table_name, df in data.items():
        if not df.empty:
            # Datalake
            bucket = "raw-data"
            s3_key = f"paytour/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Upload PayTour MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            
            # ClickHouse Ingestion
            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)

@flow(name="PayTour to ClickHouse")
def paytour_pipeline(date_start: str = None, date_stop: str = None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_PAYTOUR = "319485726" # Exemplo, o usuário deve confirmar ou ajustar

    print(f"Buscando configurações na Planilha ID: {SHEET_ID}")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients_to_run = manager.get_tab_data(gid=GID_PAYTOUR)
    
    if not clients_to_run:
        print("Nenhum cliente válido encontrado para PayTour.")
        return

    # Default: yesterday
    if not date_start:
        date_start = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = date_start

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_paytour_tables()

    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando PayTour: {company_name}")
        
        try:
            data = extract_paytour_data(dt_start, dt_stop, credentials=client)
            load_paytour_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar PayTour para {company_name}: {e}")

if __name__ == "__main__":
    paytour_pipeline()
