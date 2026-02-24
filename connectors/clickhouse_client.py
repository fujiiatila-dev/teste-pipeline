import clickhouse_connect
import pandas as pd
from config.settings import settings
import logging

class ClickHouseClient:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database
        )

    def ping(self):
        return self.client.ping()

    def create_database(self):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}")

    def run_ddl(self, ddl_list: list):
        for sql in ddl_list:
            self.client.command(sql)

    def insert_dataframe(self, table_name: str, df: pd.DataFrame):
        if df.empty:
            logging.warning(f"DataFrame for {table_name} is empty. Skipping.")
            return
        
        # O clickhouse-connect insere DataFrames diretamente
        self.client.insert_df(table_name, df)
        logging.info(f"Inserted {len(df)} rows into {table_name}")
