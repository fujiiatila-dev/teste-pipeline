from prefect import flow, task
from datetime import datetime, timedelta
import logging
from connectors.leads2b import Leads2bConnector
from connectors.clickhouse_client import ClickHouseClient

@task(retries=3, retry_delay_seconds=60)
def extract_leads2b_data(date_start: datetime, date_stop: datetime):
    connector = Leads2bConnector()
    return connector.extract(date_start, date_stop)

@task
def create_leads2b_tables():
    connector = Leads2bConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_leads2b_to_clickhouse(data: dict):
    client = ClickHouseClient()
    for table_name, df in data.items():
        client.insert_dataframe(table_name, df)

@flow(name="Leads2b to ClickHouse")
def leads2b_pipeline(date_start: str = None, date_stop: str = None):
    # Default: last 7 days for CRM to catch updates
    if not date_start:
        date_start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_leads2b_tables()
    data = extract_leads2b_data(dt_start, dt_stop)
    load_leads2b_to_clickhouse(data)

if __name__ == "__main__":
    leads2b_pipeline()
