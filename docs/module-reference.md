# Referencia de Modulos

> **Referencia tecnica de todos os modulos, classes e funcoes do pipeline.**

---

## Estrutura do Projeto

```
teste-pipeline/
├── config/
│   └── settings.py          # Configuracoes centralizadas (Pydantic)
├── connectors/
│   ├── base.py              # Classe abstrata BaseConnector
│   ├── clickhouse_client.py # Cliente ClickHouse
│   ├── datalake.py          # Cliente MinIO (S3)
│   └── *.py                 # 40 conectores de integracao
├── flows/
│   ├── health_check_flow.py # Health check da infraestrutura
│   └── *_flow.py            # 41 flows de integracao
├── scripts/
│   ├── gsheets_manager.py   # Leitor de config multi-cliente
│   ├── alerting.py          # Notificacoes por email
│   ├── monitor.py           # Monitor de recursos do sistema
│   └── webhook_notifier.py  # Webhook para backoffice
├── .github/workflows/
│   ├── ci.yml               # Pipeline de CI (lint, compile)
│   └── deploy.yml           # Pipeline de deploy (Docker, Prefect)
├── Dockerfile               # Imagem do worker
├── docker-compose.yml       # Servicos (Prefect, MinIO)
├── prefect.yaml             # 42 deployments
├── requirements.txt         # Dependencias Python
└── .env.example             # Template de variaveis de ambiente
```

---

## config/settings.py

### Classe `Settings`

Herda de `pydantic_settings.BaseSettings`. Carrega configuracoes de `.env`, variaveis de ambiente e Prefect Variables.

```python
class Settings(BaseSettings):
    class Config:
        env_file = ".env"
```

**Metodo principal:**

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `get(key)` | `key: str` | `Any` | Busca valor em: 1) Prefect Variable, 2) .env/env var, 3) default |

**Grupos de configuracao (170+ campos):**

| Grupo | Campos principais | Obrigatorio |
|-------|------------------|-------------|
| ClickHouse | `clickhouse_host`, `_port`, `_user`, `_password`, `_database` | Sim |
| MinIO | `minio_endpoint`, `_external_endpoint`, `_access_key`, `_secret_key` | Sim |
| Alertas | `alert_email_to`, `_from`, `smtp_host`, `_port`, `_user`, `_password` | Parcial |
| Webhook | `backoffice_endpoint`, `backoffice_auth_token` | Nao |
| Integracoes | 36 grupos de credenciais (1 por plataforma) | Nao |

**Instancia global:**
```python
settings = Settings()  # Disponivel via: from config.settings import settings
```

---

## connectors/base.py

### Classe `BaseConnector` (ABC)

Classe abstrata que define a interface de todos os conectores.

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `extract()` | `date_start: datetime`, `date_stop: datetime` | `dict[str, DataFrame]` | Extrai dados da API e retorna dicionario de DataFrames |
| `get_tables_ddl()` | - | `list[str]` | Retorna lista de SQL CREATE TABLE para ClickHouse |

**Exemplo de retorno do `extract()`:**
```python
{
    "meta_ad_insights": DataFrame(columns=["date_start", "campaign_id", ...]),
    "meta_campaigns": DataFrame(columns=["id", "name", "status", ...]),
}
```

**Exemplo de retorno do `get_tables_ddl()`:**
```python
[
    """CREATE TABLE IF NOT EXISTS marketing.meta_ad_insights (
        date_start Date,
        campaign_id String,
        ...
        updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY (date_start, campaign_id, project_id)""",
]
```

---

## connectors/clickhouse_client.py

### Classe `ClickHouseClient`

Wrapper para operacoes no ClickHouse usando `clickhouse-connect`.

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `__init__()` | - | - | Conecta usando settings (host, port, user, password, database) |
| `ping()` | - | `bool` | Testa conexao |
| `create_database()` | - | - | `CREATE DATABASE IF NOT EXISTS {database}` |
| `run_ddl(ddl_list)` | `ddl_list: list[str]` | - | Executa lista de comandos SQL DDL |
| `insert_dataframe(table, df)` | `table: str`, `df: DataFrame` | - | Insere DataFrame diretamente via `client.insert_df()` |
| `insert_from_s3(table, s3_url)` | `table: str`, `s3_url: str` | - | Carrega dados do MinIO via `s3()` virtual table (C2C) |

**Fluxo do `insert_from_s3()`:**
```
s3://raw-data/meta_ads/file.parquet
    → http://minio:9000/raw-data/meta_ads/file.parquet
    → INSERT INTO table SELECT * FROM s3(url, key, secret, 'Parquet')
```

---

## connectors/datalake.py

### Classe `DatalakeConnector`

Cliente MinIO (S3-compatible) para persistencia de dados brutos em Parquet.

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `__init__()` | - | - | Inicializa boto3 S3 client com credenciais MinIO |
| `ensure_bucket_exists(bucket)` | `bucket: str` | - | Cria bucket se nao existir |
| `push_dataframe_to_parquet(df, bucket, key)` | `df: DataFrame`, `bucket: str`, `key: str` | `str` | Salva DF como Parquet, faz upload, retorna `s3://path` |

**Fluxo interno:**
```
DataFrame → temp_file.parquet → s3.upload_file() → MinIO → retorna s3://bucket/key
```

---

## scripts/gsheets_manager.py

### Classe `GSheetsManager`

Leitor de credenciais multi-cliente via Google Sheets (exportacao CSV publica).

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `__init__(sheet_id)` | `sheet_id: str` | - | ID da planilha Google Sheets |
| `get_tab_data(gid)` | `gid: str` | `list[dict]` | Busca dados de uma aba especifica |

**Comportamento do `get_tab_data()`:**
1. Faz GET no CSV export: `https://docs.google.com/spreadsheets/d/{id}/export?format=csv&gid={gid}`
2. Parseia com `pandas.read_csv()`
3. Remove colunas/linhas vazias
4. Valida `project_id` e `company_id` (gera fallback se ausentes)
5. Retorna lista de dicionarios com clientes validos

---

## scripts/alerting.py

### Funcoes de alerta por email

| Funcao | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `send_email_alert(subject, body, html)` | `subject: str`, `body: str`, `html: bool` | `bool` | Envia email via SMTP com TLS |
| `alert_flow_failure(flow, error, client)` | `flow_name: str`, `error: str`, `client_name: str` | `bool` | Alerta de falha de flow (HTML) |
| `alert_health_check(failures)` | `failures: list[dict]` | `bool` | Alerta de servicos indisponiveis |
| `alert_data_freshness(stale)` | `stale_tables: list[dict]` | `bool` | Alerta de dados desatualizados |

**Configuracao:**
- De: `pipeline@nalk.com.br`
- Para: `suporte@nalk.com.br`
- SMTP: `smtp.gmail.com:587` (TLS)

---

## scripts/monitor.py

### Classe `ResourceMonitor`

Monitor de recursos do sistema (CPU, memoria, disco) usando `psutil`.

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `get_metrics()` | - | `dict` | Coleta metricas atuais do sistema |
| `check_thresholds()` | - | `list[dict]` | Retorna alertas se recurso ultrapassou threshold |
| `log_metrics()` | - | `tuple` | Loga metricas e retorna (metrics, alerts) |

**Thresholds:**

| Recurso | Threshold |
|---------|-----------|
| Memoria | 85% |
| CPU | 90% |
| Disco | 90% |

---

## scripts/webhook_notifier.py

### Classe `WebhookNotifier`

Notificador de status de flows para backoffice via webhook HTTP.

| Metodo | Parametros | Retorno | Descricao |
|--------|-----------|---------|-----------|
| `__init__(endpoint, auth_token)` | `endpoint: str`, `auth_token: str` | - | Configura endpoint e token |
| `send(payload)` | `payload: dict` | `bool` | Envia payload com retry exponencial (5 tentativas) |
| `notify_flow_start(flow, company)` | `flow_name: str`, `company_id: str` | `bool` | Notifica inicio |
| `notify_flow_success(flow, company, tables, rows)` | ... | `bool` | Notifica sucesso |
| `notify_flow_failure(flow, error, company)` | ... | `bool` | Notifica falha |

**Retry:** 5 tentativas com backoff exponencial (10s, 20s, 40s, 80s, 160s)

---

## Conectores de Integracao (40 arquivos)

Todos herdam `BaseConnector` e implementam `extract()` + `get_tables_ddl()`.

### Ads & Marketing

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `meta_ads.py` | `MetaAdsConnector` | `meta_ad_insights`, `meta_campaigns` |
| `google_ads.py` | `GoogleAdsConnector` | `google_ads_campaigns`, `google_ads_ad_groups`, `google_ads_ads`, `google_ads_keywords` |
| `rd_marketing.py` | `RDMarketingConnector` | `rd_marketing_emails`, `rd_marketing_conversions`, `rd_marketing_funnels` |
| `brevo.py` | `BrevoConnector` | `brevo_email_campaigns`, `brevo_sms_campaigns` |
| `mautic.py` | `MauticConnector` | `mautic_contacts`, `mautic_segments`, `mautic_campaigns` |
| `active_campaign.py` | `ActiveCampaignConnector` | `active_campaign_contacts`, `active_campaign_deals`, etc. |

### CRM

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `hubspot.py` | `HubSpotConnector` | `hubspot_contacts`, `hubspot_companies`, `hubspot_deals` |
| `pipedrive.py` | `PipedriveConnector` | `pipedrive_deals`, `pipedrive_persons`, `pipedrive_organizations` |
| `ploomes.py` | `PloomesConnector` | `ploomes_deals`, `ploomes_contacts`, `ploomes_tables` |
| `piperun.py` | `PiperunConnector` | `piperun_deals`, `piperun_contacts`, `piperun_companies` |
| `moskit.py` | `MoskitConnector` | `moskit_deals`, `moskit_custom_fields`, `moskit_stages`, etc. |
| `rdcrm.py` | `RDCRMConnector` | `rdcrm_deals`, `rdcrm_contacts`, `rdcrm_companies` |
| `c2s.py` | `C2sConnector` | `c2s_companies`, `c2s_leads`, `c2s_sellers`, `c2s_tags` |
| `leads2b.py` | `Leads2bConnector` | `leads2b_companies`, `leads2b_deals` |

### CRM Imobiliario

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `cvcrm_cvdw.py` | `CvcrmCvdwConnector` | `cvcrm_cvdw_vendas`, `_reservas`, `_leads`, `_leads_visitas`, `_leads_historico_situacoes` |
| `cvcrm_cvio.py` | `CvcrmCvioConnector` | `cvcrm_cvio_leads` |
| `arbo.py` | `ArboConnector` | `arbo_leads`, `arbo_imoveis` |
| `hypnobox.py` | `HypnoboxConnector` | `hypnobox_products`, `_clients`, `_visitas`, `_propostas`, `_tarefas` |
| `imobzi.py` | `ImobziConnector` | `imobzi_deals`, `_contacts`, `_pipelines`, etc. |
| `facilita.py` | `FacilitaConnector` | `facilita_negocios`, `_leads`, `_funnel` |
| `sigavi.py` | `SigaviConnector` | `sigavi_fac_lista` |
| `groner.py` | `GronerConnector` | `groner_usuario_periodo_funil_status`, `_lead_cards`, etc. |

### E-commerce & Produtos Digitais

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `shopify.py` | `ShopifyConnector` | `shopify_orders`, `shopify_products`, `shopify_customers` |
| `hotmart.py` | `HotmartConnector` | `hotmart_sales`, `hotmart_products`, `hotmart_subscriptions` |
| `eduzz.py` | `EduzzConnector` | `eduzz_sales`, `eduzz_products` |

### Financeiro & Billing

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `asaas.py` | `AsaasConnector` | `asaas_payments`, `asaas_customers`, `asaas_subscriptions` |
| `superlogica.py` | `SuperlogicaConnector` | `superlogica_contratos`, `_proprietarios`, `_locatarios`, etc. |
| `vindi.py` | `VindiConnector` | `vindi_bills`, `_subscriptions`, `_customers` |
| `acert.py` | `AcertConnector` | `acert_customers`, `_cash_flow`, `_products`, `_sales`, etc. |
| `clicksign.py` | `ClicksignConnector` | `clicksign_envelopes`, `_documents`, `_signers` |

### ERP

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `omie.py` | `OmieConnector` | `omie_clientes`, `omie_pedidos`, `omie_nfes` |
| `everflow.py` | `EverflowConnector` | `everflow_clientes`, `_contratos`, `_pagars`, etc. (16 tabelas) |

### Outros

| Conector | Classe | Tabelas |
|----------|--------|---------|
| `clickup.py` | `ClickUpConnector` | `clickup_tasks`, `clickup_time_entries` |
| `digisac.py` | `DigisacConnector` | `digisac_questions`, `_contacts`, `_answers_overview` |
| `native.py` | `NativeConnector` | `native_report_{id}` (dinamico) |
| `evo.py` | `EvoConnector` | `evo_activities`, `_members`, `_sales`, etc. |
| `belle.py` | `BelleConnector` | `belle_agendamentos`, `_clientes`, `_vendas`, etc. |
| `learn_words.py` | `LearnWordsConnector` | `learn_words_users`, `_courses`, `_user_progress`, etc. |
| `paytour.py` | `PayTourConnector` | `paytour_orders`, `paytour_products` |
| `silbeck.py` | `SilbeckConnector` | `silbeck_deals`, `silbeck_contacts` |

---

## Flows de Integracao (41 arquivos)

Todos seguem o mesmo padrao:

```python
@flow(name="Nome do Flow")
def nome_pipeline(date_start: str = None, date_stop: str = None):
    # 1. Ler config
    manager = GSheetsManager(sheet_id=SHEET_ID)
    clients = manager.get_tab_data(gid=GID)

    # 2. Criar tabelas
    create_tables()

    # 3. Loop por cliente
    for client in clients:
        data = extract_data(dt_start, dt_stop, credentials=client)
        load_to_clickhouse(data, client, dt_stop)
```

**Tasks com retry:**
- `extract_*()`: `retries=3, retry_delay_seconds=60`
- `create_tables()`: sem retry
- `load_to_clickhouse()`: sem retry (fallback interno de S3 → insert direto)

---

## Dependencias (requirements.txt)

| Pacote | Versao | Uso |
|--------|--------|-----|
| `facebook-business` | >=19.0.0 | Meta Ads SDK |
| `clickhouse-connect` | >=0.7.0 | Cliente ClickHouse |
| `pandas` | >=2.0.0 | Manipulacao de dados |
| `python-dotenv` | >=1.0.0 | Carga de .env |
| `pydantic-settings` | >=2.0.0 | Configuracoes tipadas |
| `requests` | >=2.31.0 | HTTP client |
| `google-ads` | >=23.1.0 | Google Ads SDK |
| `hubspot-api-client` | >=8.0.0 | HubSpot SDK |
| `aiohttp` | >=3.9.0 | HTTP async |
| `boto3` | latest | S3/MinIO client |
| `s3fs` | latest | S3 filesystem |
| `pyarrow` | latest | Engine Parquet |
| `psutil` | >=5.9.0 | Monitor de recursos |

---

*Documentacao atualizada em Marco 2026.*
