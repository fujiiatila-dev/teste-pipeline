from prefect import flow, task
from connectors.clicksign import ClicksignConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
from datetime import datetime, timedelta
import pandas as pd
import logging

@task(retries=3, retry_delay_seconds=60)
def extract_clicksign_data(date_start, date_stop, credentials):
    connector = ClicksignConnector(
        token=credentials.get("token") or credentials.get("clicksign_token"),
        api_url=credentials.get("api_url") or credentials.get("clicksign_api_url"),
    )
    data = connector.extract(date_start, date_stop)
    company_id = credentials.get("project_id", "unknown")
    for df in data.values():
        if not df.empty:
            df["project_id"] = company_id
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str)
    return data

@task
def create_tables():
    ch = ClickHouseClient()
    connector = ClicksignConnector()
    ch.create_database()
    ch.run_ddl(connector.get_tables_ddl())

@task
def load_to_clickhouse(data_dict, credentials, dt_stop):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    company_name = credentials.get("project_id", "unknown")
    date_path = dt_stop.strftime("%Y%m%d")
    for table_name, df in data_dict.items():
        if not df.empty:
            bucket = "raw-data"
            s3_key = f"clicksign/{company_name}/{table_name}_run_{date_path}.parquet"
            print(f"[{table_name}] Upload Clicksign MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)

@flow(name="Clicksign to ClickHouse")
def clicksign_pipeline(date_start=None, date_stop=None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_CLICKSIGN = "0"
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients = manager.get_tab_data(gid=GID_CLICKSIGN)
    clicksign_clients = [c for c in clients if c.get("clicksign_token") or c.get("token")]
    if not clicksign_clients:
        print("Nenhum cliente Clicksign encontrado.")
        return
    if not date_start:
        date_start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if not date_stop:
        date_stop = datetime.now().strftime("%Y-%m-%d")
    dt_start = datetime.strptime(date_start, "%Y-%m-%d")
    dt_stop = datetime.strptime(date_stop, "%Y-%m-%d")
    create_tables()
    for client in clicksign_clients:
        company = client.get("project_id", "Unknown")
        print(f"--> Processando Clicksign: {company}")
        try:
            data = extract_clicksign_data(dt_start, dt_stop, credentials=client)
            load_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar Clicksign para {company}: {e}")

if __name__ == "__main__":
    clicksign_pipeline()
