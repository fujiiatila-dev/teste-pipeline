# Fluxos do Pipeline - Diagramas e Etapas

> **Diagramas detalhados de cada etapa do pipeline de dados.**

---

## Visao Geral do Fluxo

```mermaid
%%{init: {'theme': 'dark'}}%%
graph TB
    START(("Trigger<br/>(Schedule/Manual)")) --> READ_CONFIG
    READ_CONFIG["1. Ler Configuracao<br/>GSheetsManager"] --> CREATE_TABLES
    CREATE_TABLES["2. Criar Tabelas<br/>ClickHouseClient.run_ddl()"] --> LOOP

    subgraph LOOP["3. Loop por Cliente"]
        EXTRACT["extract()<br/>API → DataFrame"] --> TRANSFORM
        TRANSFORM["Transformar<br/>project_id, tipos"] --> LAKE
        LAKE["push_to_parquet()<br/>DataFrame → MinIO"] --> LOAD
        LOAD["insert_from_s3()<br/>MinIO → ClickHouse"]
    end

    LOOP --> SUCCESS{"Sucesso?"}
    SUCCESS -->|"Sim"| DONE(("Concluido"))
    SUCCESS -->|"Nao"| ALERT["Alerta Email<br/>suporte@nalk.com.br"]
    ALERT --> DONE
```

---

## Etapa 1: Leitura de Configuracao

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
    participant Flow
    participant GSM as GSheetsManager
    participant GS as Google Sheets

    Flow->>GSM: GSheetsManager(sheet_id)
    Flow->>GSM: get_tab_data(gid="1615208458")
    GSM->>GS: GET /export?format=csv&gid=...
    GS-->>GSM: CSV response
    GSM->>GSM: pandas.read_csv()
    GSM->>GSM: Valida project_id / company_id
    GSM->>GSM: Gera IDs fallback se necessario
    GSM-->>Flow: List[dict] - clientes validos
```

**Detalhes:**
- A planilha Google Sheets e publica (somente leitura)
- Cada aba corresponde a uma integracao (Meta Ads, HubSpot, etc.)
- Cada linha = 1 cliente com suas credenciais
- Se `project_id` ou `company_id` estiverem vazios, sao gerados automaticamente a partir do nome do cliente

---

## Etapa 2: Criacao de Tabelas

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
    participant Flow
    participant Conn as Connector
    participant CH as ClickHouse

    Flow->>CH: create_database()
    Note over CH: CREATE DATABASE IF NOT EXISTS marketing

    Flow->>Conn: get_tables_ddl()
    Conn-->>Flow: List[str] - SQLs de CREATE TABLE

    Flow->>CH: run_ddl(ddl_list)
    loop Para cada DDL
        CH->>CH: CREATE TABLE IF NOT EXISTS ...
    end

    Note over CH: Engine: ReplacingMergeTree<br/>ORDER BY: campos de identidade<br/>updated_at: controle de versao
```

**Padrao DDL (ClickHouse):**
```sql
CREATE TABLE IF NOT EXISTS marketing.meta_ad_insights (
    date_start Date,
    campaign_id String,
    campaign_name String,
    impressions Float64,
    clicks Float64,
    spend Float64,
    project_id String DEFAULT 'unknown',
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (date_start, campaign_id, project_id)
```

---

## Etapa 3: Extracao (Extract)

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
    participant Flow
    participant Conn as Connector
    participant API as API Externa

    Flow->>Conn: extract(date_start, date_stop)

    alt Paginacao offset-based
        loop Enquanto tem dados
            Conn->>API: GET /endpoint?offset=N&limit=100
            API-->>Conn: JSON response
            Conn->>Conn: Acumula em lista
        end
    else Paginacao page-based
        loop Enquanto pagina < total
            Conn->>API: GET /endpoint?page=N&per_page=100
            API-->>Conn: JSON response
        end
    else Paginacao cursor-based
        loop Enquanto cursor != null
            Conn->>API: GET /endpoint?cursor=ABC
            API-->>Conn: JSON + next_cursor
        end
    else Sem paginacao
        Conn->>API: GET /endpoint
        API-->>Conn: JSON completo
    end

    Conn->>Conn: pd.DataFrame(dados)
    Conn-->>Flow: {"tabela_1": df1, "tabela_2": df2}
```

**Tipos de autenticacao usados pelos conectores:**

| Tipo | Conectores | Exemplo |
|------|-----------|---------|
| Bearer Token | Arbo, C2S, Clicksign, Groner, etc. | `Authorization: Bearer {token}` |
| API Key (header) | Brevo, Moskit, Ploomes | `api-key: {key}` ou `apikey: {key}` |
| Basic Auth | Vindi, Evo | `Authorization: Basic base64(user:pass)` |
| OAuth2 | Mautic, Learn Words | `client_credentials → Bearer` |
| Login + Token | Hypnobox, Native, Sigavi | POST login → token → Bearer |
| Headers customizados | CVCRM, Facilita, Superlogica | `email`, `token`, `app_token` |
| SDK nativo | Meta Ads, Google Ads, HubSpot | Facebook Business SDK, Google Ads API |

---

## Etapa 4: Carga (Load)

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
    participant Flow
    participant Lake as DatalakeConnector
    participant MN as MinIO
    participant CH as ClickHouse

    loop Para cada tabela no dict
        Flow->>Flow: Injeta project_id no DataFrame
        Flow->>Flow: Converte tipos numericos

        Flow->>Lake: push_dataframe_to_parquet(df, bucket, s3_key)
        Lake->>Lake: df.to_parquet(temp_file)
        Lake->>MN: s3.upload_file(temp_file, bucket, key)
        MN-->>Lake: OK
        Lake->>Lake: Remove temp_file
        Lake-->>Flow: s3_url

        Flow->>CH: insert_from_s3(table, s3_url)
        Note over CH: INSERT INTO table<br/>SELECT * FROM s3(url, 'Parquet')

        alt Falha no S3 load
            Flow->>CH: insert_dataframe(table, df)
            Note over CH: Fallback: insert direto<br/>via clickhouse-connect
        end
    end
```

**Organizacao no MinIO:**
```
raw-data/
├── meta_ads/
│   ├── cliente_a/
│   │   ├── meta_ad_insights_run_20260313.parquet
│   │   └── meta_campaigns_run_20260313.parquet
│   └── cliente_b/
│       └── ...
├── hubspot/
│   ├── cliente_x/
│   │   ├── hubspot_contacts_run_20260313.parquet
│   │   └── ...
│   └── ...
└── ...
```

---

## Fluxo de Health Check

```mermaid
%%{init: {'theme': 'dark'}}%%
graph TB
    CRON(("Cron: 0 */6 * * *<br/>America/Sao_Paulo")) --> HC

    subgraph HC["health_check_pipeline()"]
        C1["check_clickhouse()<br/>ping()"]
        C2["check_minio()<br/>list_buckets()"]
        C3["check_prefect_api()<br/>GET /api/health"]
        C4["check_google_sheets()<br/>GET CSV export"]
        C5["check_data_freshness()<br/>updated_at > 48h"]
    end

    HC --> EVAL{"Falhas?"}
    EVAL -->|"Sim"| EMAIL["Email para<br/>suporte@nalk.com.br"]
    EVAL -->|"Nao"| OK(("Tudo OK"))

    C5 --> STALE{"Tabelas<br/>desatualizadas?"}
    STALE -->|"Sim"| EMAIL_STALE["Email: Dados<br/>Desatualizados"]
    STALE -->|"Nao"| OK
```

---

## Fluxo de Alertas

```mermaid
%%{init: {'theme': 'dark'}}%%
graph LR
    subgraph TRIGGERS["Gatilhos"]
        FF["Flow falhou"]
        HCF["Health check falhou"]
        DF["Dados desatualizados >48h"]
    end

    subgraph CHANNELS["Canais"]
        EMAIL["SMTP Email<br/>smtp.gmail.com:587"]
        WH["Webhook HTTP<br/>backoffice"]
    end

    FF --> EMAIL
    FF --> WH
    HCF --> EMAIL
    DF --> EMAIL
```

**Configuracao de alertas:**
- **De:** pipeline@nalk.com.br
- **Para:** suporte@nalk.com.br
- **SMTP:** smtp.gmail.com:587 (TLS)
- **Webhook:** POST para `{backoffice_endpoint}/airflow/webhook` com retry exponencial (5 tentativas)

---

## Ciclo de Vida de uma Execucao

```mermaid
%%{init: {'theme': 'dark'}}%%
stateDiagram-v2
    [*] --> Scheduled: Prefect Schedule
    [*] --> Manual: Prefect UI / CLI

    Scheduled --> Running: Worker pega a execucao
    Manual --> Running

    Running --> Extracting: Conecta a API
    Extracting --> Loading: Dados extraidos
    Loading --> Completed: Carga finalizada

    Extracting --> Failed: Erro na API
    Loading --> Failed: Erro no ClickHouse/MinIO

    Failed --> Retrying: retry (max 3x, delay 60s)
    Retrying --> Extracting
    Retrying --> Failed: Esgotou retries

    Failed --> [*]: Alerta enviado
    Completed --> [*]
```

---

## Prefect Deployments

Todos os deployments estao configurados em `prefect.yaml`:

| Deployment | Flow | Work Pool | Parametros |
|------------|------|-----------|-----------|
| 42 integracoes | `{nome}_pipeline()` | default | `date_start`, `date_stop` |
| health-check | `health_check_pipeline()` | default | nenhum |

**Schedule ativo:** Apenas `health-check` (cron: `0 */6 * * *`)

Os demais flows sao executados manualmente via Prefect UI ou API, ou podem ter schedules adicionados no `prefect.yaml`.

---

*Documentacao atualizada em Marco 2026.*
