from prefect import flow, task
from connectors.eduzz import EduzzConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
from datetime import datetime, timedelta
import pandas as pd


@task(retries=3, retry_delay_seconds=60)
def extract_eduzz_data(date_start: datetime, date_stop: datetime, credentials: dict):
    connector = EduzzConnector(
        api_url=credentials.get("api_url") or credentials.get("eduzz_api_url"),
        auth_token=credentials.get("auth_token") or credentials.get("eduzz_auth_token"),
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
    connector = EduzzConnector()
    ch.create_database()
    ch.run_ddl(connector.get_tables_ddl())


@task
def load_to_clickhouse(data_dict: dict, credentials: dict, dt_stop: datetime):
    ch = ClickHouseClient()
    lake = DatalakeConnector()

    company_name = credentials.get("project_id", "unknown")
    date_path = dt_stop.strftime("%Y%m%d")

    for table_name, df in data_dict.items():
        if not df.empty:
            bucket = "raw-data"
            s3_key = f"eduzz/{company_name}/{table_name}_run_{date_path}.parquet"

            print(f"[{table_name}] Upload Eduzz MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)

            if s3_url:
                try:
                    ch.insert_from_s3(table_name, s3_url)
                except Exception as err:
                    print(f"[{table_name}] Fallback upload: {err}")
                    ch.insert_dataframe(table_name, df)


@flow(name="Eduzz to ClickHouse")
def eduzz_pipeline(date_start: str = None, date_stop: str = None):
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    GID_EDUZZ = "0"

    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients = manager.get_tab_data(gid=GID_EDUZZ)

    eduzz_clients = [c for c in clients if c.get("auth_token") or c.get("eduzz_auth_token")]

    if not eduzz_clients:
        print("Nenhum cliente Eduzz encontrado na planilha.")
        return

    if not date_start:
        date_start = "2020-01-01"
    if not date_stop:
        date_stop = datetime.now().strftime("%Y-%m-%d")

    dt_start = datetime.strptime(date_start, "%Y-%m-%d")
    dt_stop = datetime.strptime(date_stop, "%Y-%m-%d")

    create_tables()

    for client in eduzz_clients:
        company = client.get("project_id", "Unknown")
        print(f"--> Processando Eduzz: {company}")

        try:
            data = extract_eduzz_data(dt_start, dt_stop, credentials=client)
            load_to_clickhouse(data, client, dt_stop)
        except Exception as e:
            print(f"Falha ao rodar Eduzz para {company}: {e}")


if __name__ == "__main__":
    eduzz_pipeline()
