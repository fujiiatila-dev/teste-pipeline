from prefect import flow, task
from datetime import datetime, timedelta
import logging
from connectors.shopify import ShopifyConnector
from connectors.clickhouse_client import ClickHouseClient

@task(retries=3, retry_delay_seconds=60)
def extract_shopify_data(date_start: datetime, date_stop: datetime):
    connector = ShopifyConnector()
    return connector.extract(date_start, date_stop)

@task
def create_shopify_tables():
    connector = ShopifyConnector()
    client = ClickHouseClient()
    client.run_ddl(connector.get_tables_ddl())

@task
def load_shopify_to_clickhouse(data: dict):
    client = ClickHouseClient()
    for table_name, df in data.items():
        client.insert_dataframe(table_name, df)

@flow(name="Shopify to ClickHouse")
def shopify_pipeline(date_start: str = None, date_stop: str = None):
    # Default: last 2 days
    if not date_start:
        date_start = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = datetime.now().strftime('%Y-%m-%d')

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_shopify_tables()
    data = extract_shopify_data(dt_start, dt_stop)
    load_shopify_to_clickhouse(data)

if __name__ == "__main__":
    shopify_pipeline()
