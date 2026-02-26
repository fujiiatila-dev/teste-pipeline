from prefect import flow, task
from connectors.google_ads import GoogleAdsConnector
from connectors.clickhouse_client import ClickHouseClient
from datetime import datetime, timedelta
import pandas as pd
from scripts.gsheets_manager import GSheetsManager
from connectors.datalake import DatalakeConnector

@task(retries=3, retry_delay_seconds=60)
def extract_google_ads_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = GoogleAdsConnector(
        developer_token=credentials.get("developer_token"),
        client_id=credentials.get("client_id"),
        client_secret=credentials.get("client_secret"),
        refresh_token=credentials.get("refresh_token"),
        login_customer_id=str(credentials.get("login_customer_id")),
        customer_ids=str(credentials.get("subaccount_ids"))
    )
    data = connector.extract(date_start, date_stop)
    
    # Injetando infos adicionais de negócio
    for df in data.values():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "vinicola-thera-nalk")
             
    return data

@task
def create_tables():
    ch = ClickHouseClient()
    connector = GoogleAdsConnector()
    ch.create_database()
    ch.run_ddl(connector.get_tables_ddl())

@task
def load_to_clickhouse(data_dict: dict, credentials: dict, dt_start: datetime, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    # ID Cliente/Projeto 
    company_name = credentials.get("project_id", "unknown-client")
    
    # Range de datas como parte da key para identificar os jobs
    date_path = dt_stop.strftime('%Y%m%d')

    for table_name, df in data_dict.items():
        if not df.empty:
            # Converter colunas numéricas (Isso salva muitas exceções do Clickhouse)
            if table_name == "google_ads_insights":
                numeric_cols = ['impressions', 'clicks', 'cost', 'conversions']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        
            # Converter data -> Parquet exige compatibilidade total
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.date
                
            # PASSO 1 - Datalake
            bucket = "raw-data"
            # O path do arquivo (Organizado por empresa e tabela)
            s3_key = f"google_ads/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Fazendo upload no MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            # Gera url: s3://raw-data/google_ads/cliente/run_2026.parquet
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            
            # PASSO 2 - Cloud -> Warehouse Ingestion (Native Fast read)
            if s3_url:
                 try:
                     print(f"[{table_name}] Comandando ClickHouse para injetar via S3/MinIO Engine")
                     ch.insert_from_s3(table_name, s3_url)
                 except Exception as err:
                     # Fallback nativo do Python (Dataframe local) 
                     print(f"[{table_name}] Erro ao usar Engine S3 DWH: {err}, injetando DataFrame via python diretamente como Fail-Over safe")
                     ch.insert_dataframe(table_name, df)

@flow(name="Google Ads to ClickHouse")
def google_ads_pipeline(date_start: str = "", date_stop: str = ""):
    # ID da planilha e o novo GID
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_GOOGLE_ADS = "380179982"

    print(f"Buscando configurações na Planilha ID: {SHEET_ID}")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients_to_run = manager.get_tab_data(gid=GID_GOOGLE_ADS)
    
    if not clients_to_run:
        print("Nenhum cliente válido (com company_id) encontrado na planilha. Verifique a permissão do link.")
        return

    print(f"Total de clientes a rodar Google Ads: {len(clients_to_run)}")

    # Default: do dia 1 do mês atual até hoje
    if not date_start:
        date_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    print(f"Extraindo dados do Google Ads de {dt_start.date()} a {dt_stop.date()}")
    
    create_tables()

    # Loop para processar para cada Cliente da Planilha!
    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando cliente: {company_name}")
        
        try:
            data = extract_google_ads_data(dt_start, dt_stop, credentials=client)
            load_to_clickhouse(data, client, dt_start, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar cliente {company_name}: {e}")
    
    print("Pipeline Multi-Client do Google Ads concluído com sucesso!")

if __name__ == "__main__":
    google_ads_pipeline()
