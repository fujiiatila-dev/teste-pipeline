from prefect import flow, task
from datetime import datetime, timedelta
import logging
import pandas as pd
from connectors.leads2b import Leads2bConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager

@task(retries=3, retry_delay_seconds=60)
def extract_leads2b_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = Leads2bConnector(token=credentials.get("leads2b_token"))
    data = connector.extract(date_start, date_stop)
    
    # Injetando infos adicionais de negócio
    for df in data.values():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "unknown-client")
             
    return data

@task
def create_leads2b_tables():
    connector = Leads2bConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_leads2b_to_clickhouse(data: dict, credentials: dict, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    company_name = credentials.get("project_id", "unknown-client")
    date_path = dt_stop.strftime('%Y%m%d')

    for table_name, df in data.items():
        if not df.empty:
            # Datalake
            bucket = "raw-data"
            s3_key = f"leads2b/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Upload Leads2b MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            
            # ClickHouse Ingestion
            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)

@flow(name="Leads2b to ClickHouse")
def leads2b_pipeline(date_start: str = None, date_stop: str = None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_LEADS2B = "682134590" # Exemplo, o usuário deve confirmar ou ajustar

    print(f"Buscando configurações na Planilha ID: {SHEET_ID}")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients_to_run = manager.get_tab_data(gid=GID_LEADS2B)
    
    if not clients_to_run:
        print("Nenhum cliente válido encontrado para Leads2b.")
        return

    # Default: last 7 days for CRM to catch updates
    if not date_start:
        date_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_leads2b_tables()

    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando Leads2b: {company_name}")
        
        try:
            data = extract_leads2b_data(dt_start, dt_stop, credentials=client)
            load_leads2b_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar Leads2b para {company_name}: {e}")

if __name__ == "__main__":
    leads2b_pipeline()
