from prefect import flow, task
from connectors.hubspot import HubSpotConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
import pandas as pd
from datetime import datetime
import os

@task(retries=3, retry_delay_seconds=60)
def extract_hubspot_task(credentials: dict):
    access_token = credentials.get("api_token") or credentials.get("access_token")
    if not access_token:
        raise ValueError("HubSpot Access Token não encontrado nas credenciais.")
        
    connector = HubSpotConnector(access_token=access_token)
    
    # Busca dinamicamente a partir de uma data se disponível
    start_date = credentials.get("date_start") or "2024-01-01"
    data = connector.extract_all(start_date=start_date)
    
    # Injetando ID do projeto
    company_id = credentials.get("project_id", "unknown")
    for df in data.values():
        if not df.empty:
            df['project_id'] = company_id
            # Limpeza de tipos object (JSON/List) para String (Clickhouse S3 exige consistencia)
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)
                
    return data

@task
def load_hubspot_to_datalake(data_dict: dict, credentials: dict):
    lake = DatalakeConnector()
    ch = ClickHouseClient()
    
    company = credentials.get("project_id", "unknown")
    run_date = datetime.now().strftime('%Y%m%d')
    
    for table_name, df in data_dict.items():
        if df.empty:
            continue
            
        bucket = "raw-data"
        s3_key = f"hubspot/{company}/{table_name}_{run_date}.parquet"
        
        print(f"[{table_name}] Subindo para Datalake -> s3://{bucket}/{s3_key}")
        s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
        
        if s3_url:
            print(f"[{table_name}] Ingerindo no ClickHouse...")
            # Como HubSpot tem muitas propriedades dinâmicas, o ideal é o insert_dataframe (fail-safe) 
            # ou garantir que a tabela existe. 
            # Tentamos via S3 nativo primeiro
            try:
                ch.insert_from_s3(table_name, s3_url)
            except Exception:
                print(f"[{table_name}] Failover: Ingerindo via DataFrame diretamente.")
                ch.insert_dataframe(table_name, df)

@flow(name="HubSpot to Datalake")
def hubspot_pipeline():
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    # Adicione o GID da aba HubSpot se souber, senão o manager pode filtrar por conteúdo
    # Por padrão, usaremos o manager para pegar os clientes
    manager = GSheetsManager(sheet_id=SHEET_ID)
    
    # GID sugerido para HubSpot (exemplo) ou 0 e filtrar
    clients = manager.get_tab_data(gid="0") 
    
    # Filtrar apenas clientes que tem hubspot marcado ou campos de hubspot
    hubspot_clients = [c for c in clients if c.get('api_token') or 'hubspot' in str(c.values()).lower()]
    
    if not hubspot_clients:
        print("Nenhum cliente HubSpot encontrado na planilha.")
        return

    for client in hubspot_clients:
        print(f"🚀 Iniciando HubSpot para: {client.get('project_id')}")
        try:
            data = extract_hubspot_task(client)
            load_hubspot_to_datalake(data, client)
        except Exception as e:
            print(f"❌ Erro no cliente {client.get('project_id')}: {e}")

if __name__ == "__main__":
    hubspot_pipeline()
