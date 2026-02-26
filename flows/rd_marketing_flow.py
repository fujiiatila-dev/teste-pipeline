from prefect import flow, task
from datetime import datetime, timedelta
import logging
import pandas as pd
from connectors.rd_marketing import RDMarketingConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager

@task(retries=3, retry_delay_seconds=60)
def extract_rdmkt_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = RDMarketingConnector(
        client_id=credentials.get("rd_client_id") or credentials.get("client_id"),
        client_secret=credentials.get("rd_client_secret") or credentials.get("client_secret"),
        refresh_token=credentials.get("rd_refresh_token") or credentials.get("refresh_token") or credentials.get("token"),
        x_api_key=credentials.get("rd_x_api_key") or credentials.get("x_api_key"),
        alias=credentials.get("rd_company_alias") or credentials.get("alias")
    )
    data = connector.extract(date_start, date_stop)
    
    # Injetando infos adicionais de negócio
    for table_name, df in data.items():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "unknown-client")
             # Cast todas as colunas object para string pra evitar erro no pyarrow/clickhouse
             for col in df.select_dtypes(include=['object']).columns:
                 df[col] = df[col].astype(str)
             
    return data

@task
def create_rdmkt_tables():
    connector = RDMarketingConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_rdmkt_to_clickhouse(data: dict, credentials: dict, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    company_name = credentials.get("project_id", "unknown-client")
    date_path = dt_stop.strftime('%Y%m%d')

    for table_name, df in data.items():
        if not df.empty:
            # Datalake
            bucket = "raw-data"
            s3_key = f"rd_marketing/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Upload RD Marketing MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            
            # ClickHouse Ingestion
            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)

@flow(name="RD Marketing to ClickHouse")
def rd_marketing_pipeline(date_start: str = None, date_stop: str = None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_RDMKT = "1479848665" # Atualizado conforme URL do usuário

    print(f"Buscando configurações na Planilha ID: {SHEET_ID}")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients_to_run = manager.get_tab_data(gid=GID_RDMKT)
    
    if not clients_to_run:
        print("Nenhum cliente válido encontrado para RD Marketing.")
        return

    # Default: last 45 days for analytics (as per legacy 43 days)
    if not date_start:
        date_start = (datetime.now() - timedelta(days=45)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_rdmkt_tables()

    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando RD Marketing: {company_name}")
        
        try:
            data = extract_rdmkt_data(dt_start, dt_stop, credentials=client)
            load_rdmkt_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar RD Marketing para {company_name}: {e}")

if __name__ == "__main__":
    rd_marketing_pipeline()
