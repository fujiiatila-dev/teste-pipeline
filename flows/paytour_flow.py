from prefect import flow, task
from datetime import datetime, timedelta
import logging
import pandas as pd
from connectors.paytour import PayTourConnector
from connectors.clickhouse_client import ClickHouseClient

@task(retries=3, retry_delay_seconds=60)
def extract_paytour_data(date_start: datetime, date_stop: datetime):
    connector = PayTourConnector()
    return connector.extract(date_start, date_stop)

@task
def create_paytour_tables():
    connector = PayTourConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_paytour_to_clickhouse(data: dict):
    client = ClickHouseClient()
    for table_name, df in data.items():
        client.insert_dataframe(table_name, df)

@flow(name="PayTour to ClickHouse")
def paytour_pipeline(date_start: str = None, date_stop: str = None):
    # Default: yesterday
    if not date_start:
        date_start = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = date_start

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_paytour_tables()
    data = extract_paytour_data(dt_start, dt_stop)
    load_paytour_to_clickhouse(data)

if __name__ == "__main__":
    paytour_pipeline()
