import pandas as pd
import tempfile
import boto3
import os
from config.settings import settings

class DatalakeConnector:
    """
    Controlador para envio de dados brutos (Raw Data) para o MinIO (Data Lake)
    usando a interface S3 (boto3).
    """

    def __init__(self):
        # Acesso ao MinIO usando s3 boto3
        self.s3_client = boto3.client(
            's3',
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            # Se for MinIO local/HTTP precisa apontar ssl para false (nas config avancadas)
            # Para S3 API compatibilidade, usamos as chaves virtuais e estilo path configs
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4')
        )
        
    def ensure_bucket_exists(self, bucket_name: str):
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            # Bucket nao existe ou sem permissao, tentamos criar
            try:
                self.s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket {bucket_name} criado no MinIO!")
            except Exception as e:
                print(f"Falha ao criar bucket {bucket_name}. Motivo: {str(e)}")

    def push_dataframe_to_parquet(self, df: pd.DataFrame, bucket_name: str, s3_key: str) -> str:
        """
        Salva o dataframe para arquivo Parquet e envia para o bucket MinIO via API S3.
        Retorna a URL/path_virtual para consumo posterior do Clickhouse.
        """
        if df.empty:
            return ""

        self.ensure_bucket_exists(bucket_name)

        # Usar tempfile local antes de upload
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name

        try:
            # Convert para parquet (Mais leve e otimizado para Data Lake)
            df.to_parquet(temp_path, index=False, engine='pyarrow')
            
            # Executar Upload
            self.s3_client.upload_file(temp_path, bucket_name, s3_key)
            print(f"Arquivo enviado para o Datalake: s3://{bucket_name}/{s3_key}")
            
            # Retorna caminho padrão S3 pro Clickhouse puxar depois
            return f"s3://{bucket_name}/{s3_key}"

        except Exception as e:
            print(f"Erro ao salvar DataFrame em parquet no bucket {bucket_name}: {str(e)}")
            raise e
        finally:
            # Apagar arquivo temporario do worker
            if os.path.exists(temp_path):
                os.remove(temp_path)
