from prefect import flow, task
from connectors.active_campaign import ActiveCampaignConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
import pandas as pd
from datetime import datetime

@task(retries=3, retry_delay_seconds=60)
def extract_active_campaign_task(credentials: dict):
    # ActiveCampaign requer account_name e api_token
    account = credentials.get("account_name")
    token = credentials.get("api_token") or credentials.get("token")
    
    if not account or not token:
        raise ValueError("Credenciais ActiveCampaign incompletas (account_name/api_token).")
        
    connector = ActiveCampaignConnector(account_name=account, api_token=token)
    data = connector.extract_all()
    
    company_id = credentials.get("project_id", "unknown")
    for df in data.values():
        if not df.empty:
            df['project_id'] = company_id
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str)
                
    return data

@task
def load_ac_to_datalake(data_dict: dict, credentials: dict):
    lake = DatalakeConnector()
    ch = ClickHouseClient()
    company = credentials.get("project_id", "unknown")
    run_date = datetime.now().strftime('%Y%m%d')
    
    for table_name, df in data_dict.items():
        if df.empty:
            continue
            
        bucket = "raw-data"
        s3_key = f"active_campaign/{company}/{table_name}_{run_date}.parquet"
        s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
        
        if s3_url:
            try:
                ch.insert_from_s3(table_name, s3_url)
            except Exception:
                ch.insert_dataframe(table_name, df)

@flow(name="ActiveCampaign to Datalake")
def active_campaign_pipeline():
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients = manager.get_tab_data(gid="0") 
    
    ac_clients = [c for c in clients if c.get('account_name') and c.get('api_token')]
    
    if not ac_clients:
        print("Nenhum cliente ActiveCampaign encontrado.")
        return

    for client in ac_clients:
        print(f"🚀 Iniciando ActiveCampaign para: {client.get('project_id')}")
        try:
            results = extract_active_campaign_task(client)
            load_ac_to_datalake(results, client)
        except Exception as e:
            print(f"❌ Erro no cliente {client.get('project_id')}: {e}")

if __name__ == "__main__":
    active_campaign_pipeline()
