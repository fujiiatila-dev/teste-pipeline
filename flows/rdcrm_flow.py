from prefect import flow, task
from connectors.rdcrm import RDCRMConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
import pandas as pd
from datetime import datetime, timedelta

@task(retries=3, retry_delay_seconds=60)
def extract_rdcrm_task(credentials: dict):
    token = credentials.get("api_token") or credentials.get("token")
    if not token:
        raise ValueError("Token RDCRM não encontrado.")
        
    connector = RDCRMConnector(token=token)
    
    # Range de datas (Padrão: últimos 30 dias para testes)
    # Na planilha você deve ter date_start e date_stop
    date_start = credentials.get("date_start") or (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    date_stop = credentials.get("date_stop") or datetime.now().strftime('%Y-%m-%d')
    
    data = connector.extract_all(date_start, date_stop)
    
    company_id = credentials.get("project_id", "unknown")
    for name, df in data.items():
        if not df.empty:
            df['project_id'] = company_id
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)
                
    return data

@task
def load_rdcrm_to_datalake(data_dict: dict, credentials: dict):
    lake = DatalakeConnector()
    ch = ClickHouseClient()
    company = credentials.get("project_id", "unknown")
    run_date = datetime.now().strftime('%Y%m%d')
    
    for table_name, df in data_dict.items():
        if df.empty:
            continue
            
        bucket = "raw-data"
        s3_key = f"rdcrm/{company}/{table_name}_{run_date}.parquet"
        s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
        
        if s3_url:
            try:
                ch.insert_from_s3(table_name, s3_url)
            except Exception:
                ch.insert_dataframe(table_name, df)

@flow(name="RD Station CRM to Datalake")
def rdcrm_pipeline():
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients = manager.get_tab_data(gid="0") 
    
    # Filtro para RD CRM (ajuste conforme o nome real da coluna ou conteúdo)
    rd_clients = [c for c in clients if c.get('token') and 'rd' in str(c.values()).lower()]
    
    if not rd_clients:
        print("Nenhum cliente RD CRM encontrado.")
        return

    for client in rd_clients:
        print(f"🚀 Iniciando RDCRM para: {client.get('project_id')}")
        try:
            results = extract_rdcrm_task(client)
            load_rdcrm_to_datalake(results, client)
        except Exception as e:
            print(f"❌ Erro no cliente {client.get('project_id')}: {e}")

if __name__ == "__main__":
    rdcrm_pipeline()
