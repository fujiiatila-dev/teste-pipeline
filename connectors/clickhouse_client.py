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

    def insert_from_s3(self, table_name: str, s3_url_path: str):
        """
        Dada uma URL s3 virtual (ex: s3://raw-data/file.parquet) gerada pelo DatalakeConnector,
        faz com que a Engine de banco de dados do ClickHouse puxe esses dados diretamente do MinIO 
        via rede C2C (Container to Container), de forma insanamente rapida.
        """
        # Formatando a URL para o protocolo HTTP do S3 compativel. 
        # Como o s3_url_path deve ser 's3://raw-data/...', removemos o 's3://' inicial.
        s3_path_cleaned = s3_url_path.replace("s3://", "")
        
        # Monta a URL C2C (usando minio_external_endpoint p/ containers se acharem)
        full_endpoint = f"{settings.minio_external_endpoint}/{s3_path_cleaned}"

        query = f"""
            INSERT INTO {table_name}
            SELECT * FROM s3(
                '{full_endpoint}',
                '{settings.minio_access_key}',
                '{settings.minio_secret_key}',
                'Parquet'
            )
        """
        
        try:
            self.client.command(query)
            logging.info(f"Dados inseridos com sucesso do Datalake {s3_url_path} para a tabela {table_name}")
        except Exception as e:
            logging.error(f"Erro ao ingerir S3 {s3_url_path} para o ClickHouse: {str(e)}")
            raise e
