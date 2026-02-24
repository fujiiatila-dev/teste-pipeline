from prefect import flow, task
from connectors.meta_ads import MetaAdsConnector
from connectors.clickhouse_client import ClickHouseClient
from datetime import datetime, timedelta
import pandas as pd

@task(retries=3, retry_delay_seconds=60)
def extract_meta_data(date_start: datetime, date_stop: datetime):
    connector = MetaAdsConnector()
    return connector.extract(date_start, date_stop)

@task
def create_tables():
    ch = ClickHouseClient()
    connector = MetaAdsConnector()
    ch.create_database()
    ch.run_ddl(connector.get_tables_ddl())

@task
def load_to_clickhouse(data_dict: dict):
    ch = ClickHouseClient()
    for table_name, df in data_dict.items():
        if not df.empty:
            # Converter colunas numéricas que vêm como string da API
            if table_name == "meta_ad_insights":
                numeric_cols = ['impressions', 'clicks', 'spend', 'reach', 'ctr', 'cpc', 'cpm']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            ch.insert_dataframe(table_name, df)

@flow(name="Meta Ads to ClickHouse")
def meta_ads_pipeline(date_start: str = None, date_stop: str = None):
    # Default: ontem
    if not date_start:
        date_start = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    if not date_stop:
        date_stop = date_start

    dt_start = datetime.strptime(date_start, '%Y-%m-%d')
    dt_stop = datetime.strptime(date_stop, '%Y-%m-%d')

    create_tables()
    data = extract_meta_data(dt_start, dt_stop)
    load_to_clickhouse(data)

if __name__ == "__main__":
    meta_ads_pipeline()
