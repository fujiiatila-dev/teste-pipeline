# Marketing Pipeline (POC Meta Ads & PayTour)

Este projeto realiza a extração de dados do Meta Ads e PayTour, e a ingestão em um banco de dados ClickHouse externo, orquestrado pelo Prefect.

## Tecnologias
- **Prefect 2.x**: Orquestração e monitoramento.
- **ClickHouse**: Data Warehouse (externo ao Docker).
- **Docker & Docker Compose**: Gerenciamento do Prefect Server e Worker.
- **Python 3.11**: Facebook SDK e ClickHouse Connect.

## Como Iniciar

1. **Configurar Variáveis de Ambiente**:
   - Copie o `.env.example` para `.env`:
     ```bash
     cp .env.example .env
     ```
   - Preencha as credenciais do **Meta Ads**, **PayTour** e do seu **ClickHouse**.

2. **Subir o ambiente com Docker**:
   ```bash
   docker-compose up -d
   ```

3. **Acessar a UI do Prefect**:
   - Abra [http://localhost:4200](http://localhost:4200) no seu navegador.

4. **Registrar o Deployment (Opcional)**:
   - Para agendar execuções, entre no container do worker e registre o flow:
     ```bash
     docker exec -it prefect-worker bash
     prefect deployment build flows/meta_ads_flow.py:meta_ads_pipeline -n "Daily Sync" --cron "0 2 * * *"
     prefect deployment apply meta_ads_pipeline-deployment.yaml
     ```

## Estrutura de Connectors
Para adicionar novas plataformas (Leads2b, Shopify, etc), basta:
1. Criar um novo arquivo em `connectors/` herdando de `BaseConnector`.
2. Implementar os métodos `extract` e `get_tables_ddl`.
3. Criar um novo flow em `flows/` seguindo o modelo do `meta_ads_flow.py`.

## Próximos Passos (Backlog)
- [x] Implementar conector Leads2b
- [x] Implementar conector Shopify
- [x] Implementar conector OMIE
- [x] Implementar conector Pay Tour
- [x] Implementar conector Silbeck
