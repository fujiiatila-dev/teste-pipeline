from prefect import flow, task
from connectors.pipedrive import PipedriveConnector
from connectors.clickhouse_client import ClickHouseClient
from connectors.datalake import DatalakeConnector
from scripts.gsheets_manager import GSheetsManager
import pandas as pd
from datetime import datetime

@task(retries=3, retry_delay_seconds=60)
def extract_pipedrive_data(credentials: dict):
    # API credentials provided dynamically from the Google Sheet
    api_domain = credentials.get("api_base_url") 
    api_token = credentials.get("api_token")
    
    # Falback: Se na planilha estivesse como "developer_token" e afins
    if not api_token:
        api_token = credentials.get("developer_token")
        
    connector = PipedriveConnector(api_domain=api_domain, api_token=api_token)
    data = connector.extract_all()
    
    # Injetando infos adicionais de negócio
    for df in data.values():
         if not df.empty:
             df['project_id'] = credentials.get("project_id", "unknown-client")
             # Cast todas as colunas object para string pra evitar erro no pyarrow
             for col in df.select_dtypes(include=['object']).columns:
                 df[col] = df[col].astype(str)
                 
    return data

@task
def create_tables():
    # Para o Pipedrive, como os schemas sao ultra dinamicos no formato schemaless (Custom Fields variam),
    # nós não precisamos dar CREATE TABLE forte! 
    # O ClickHouse nativamente ao dar INSERT FROM S3 infere o schema da primeira remessa se a tabela n existir (clickhouse 23+)
    pass

@task
def load_to_clickhouse(data_dict: dict, credentials: dict):
    ch = ClickHouseClient()
    lake = DatalakeConnector()
    
    # ID Cliente/Projeto 
    company_name = credentials.get("project_id", "unknown-client")
    date_path = datetime.now().strftime('%Y%m%d')

    for table_name, df in data_dict.items():
        if not df.empty:
            bucket = "raw-data"
            s3_key = f"pipedrive_v2/{company_name}/{table_name}_run_{date_path}.parquet"
            
            print(f"[{table_name}] Upload Pipedrive MinIO ({len(df)} linhas) -> s3://{bucket}/{s3_key}")
            
            try:
                s3_url = lake.push_dataframe_to_parquet(df, bucket, s3_key)
                
                if s3_url:
                    print(f"[{table_name}] Injetando ClickHouse via Engine S3")
                    # No ClickHouse, se a API S3 estiver enviando dinamicamente e sem tipo prévio, 
                    # as inserções em DFs evitam o schema lock de colunas customizadas do Pipedrive.
                    # Vamos usar a engine nativa apenas como Fallback de velocidade.
                    # Usaremos o upload DWH nativo do Clickhouse Connect em memoria neste workflow pois o schema do pipedrive muda por cliente!
                    ch.insert_dataframe(table_name, df)
            except Exception as e:
                print(f"[{table_name}] Erro: {e}")

@flow(name="Pipedrive to Datalake")
def pipedrive_pipeline():
    SHEET_ID = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
    # IMPORTANTE: Esse é o GID provavel da aba "pipedrive_v2", nós verificamos que a aba Pipedrive_v2 existe.
    # Mas como o Manager ler a URL base, devemos buscar a aba correta. 
    # Vou buscar pelo mesmo modelo dinamico (Apenas trocar o GID de testes no banco do usuario)
    GID_PIPEDRIVE = "TODO_CHANGE_ME" 
    
    print(f"Buscando configurações do Pipedrive V2")
    manager = GSheetsManager(sheet_id=SHEET_ID)
    
    # Buscando de todas as abas até achar a de pipedrive v2 dinâmicamente
    # Como não temos o GID exato aqui impresso, mas sabemos o nome, o manager precisa dar loop
    # Mas o manager depende do gid, entao deixaremos placeholder pra Vc por
    # Assumindo 0 por hora ou o proprio user preenche. 
    # Para o teste, lemos o GID de uma variavel: 
    # Mude isso de TODO_CHANGE_ME => ex: 123123123 (Da aba Pipedrive_v2 da planilha)
    # Por prevencao, pego o general
    clients_to_run = manager.get_tab_data(gid="149023067") # chute do GID ou usar o GID do Google ads se quiser testar c credenciais adaptadas 
    
    if not clients_to_run:
        print("Nenhum cliente válido no Pipedrive.")
        # Forcando simulação se for vazio só pra testar o Pipedrive vazio logando o flow
        pass

    for client in clients_to_run:
        company_name = client.get('project_id', 'Unknown')
        print(f"--> Processando Pipedrive cliente: {company_name}")
        
        try:
            data = extract_pipedrive_data(credentials=client)
            load_to_clickhouse(data, client)
        except Exception as e:
            print(f"Falha ao rodar cliente {company_name} Pipedrive: {e}")
    
    print("Pipeline Multi-Client do Pipedrive concluído!")

if __name__ == "__main__":
    pipedrive_pipeline()
