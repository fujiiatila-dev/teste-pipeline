from prefect import flow, task
from datetime import datetime, timedelta
import logging
from connectors.omie import OmieConnector
from connectors.clickhouse_client import ClickHouseClient

@task(retries=3, retry_delay_seconds=60)
def extract_omie_data(date_start: datetime, date_stop: datetime):
    connector = OmieConnector()
    return connector.extract(date_start, date_stop)

@task
def create_omie_tables():
    connector = OmieConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_omie_to_clickhouse(data: dict):
    client = ClickHouseClient()
    for table_name, df in data.items():
        client.insert_dataframe(table_name, df)

@flow(name="OMIE to ClickHouse")
def omie_pipeline(date_start: str = None, date_stop: str = None):
    # Default: last 30 days for ERP
    if not date_start:
        date_start = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_omie_tables()
    data = extract_omie_data(dt_start, dt_stop)
    load_omie_to_clickhouse(data)

if __name__ == "__main__":
    omie_pipeline()
