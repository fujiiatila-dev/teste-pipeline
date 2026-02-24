# Multi-Platform Marketing Data Pipeline (POC)

Este projeto é um pipeline de dados robusto e extensível projetado para extrair métricas de marketing, vendas e leads de múltiplas plataformas (Meta Ads, Shopify, Leads2b, OMIE, PayTour e Silbeck) e consolidá-las em um Data Warehouse **ClickHouse**.

A orquestração é gerenciada pelo **Prefect 2.x**, garantindo resiliência, monitoramento em tempo real e facilidade de agendamento.

## 🚀 Integrações Implementadas

| Plataforma | Tipo | Fluxo | Status |
| :--- | :--- | :--- | :--- |
| **Meta Ads** | Performance | Campanhas e Insights (Ad-level) | ✅ Operacional |
| **Shopify** | E-commerce | Pedidos, Itens e Clientes | ✅ Operacional |
| **Leads2b** | CRM | Leads e Negociações | ✅ Operacional |
| **OMIE** | ERP | Clientes e Pedidos de Venda | ✅ Operacional |
| **PayTour** | Turismo/Vendas | Pedidos, Itens, Passeios, Combos e Cupons | ✅ Operacional |
| **Silbeck** | Turismo/Reservas | Reservas e Clientes | ✅ Operacional |

## 🛠️ Stack Tecnológica

- **Orquestrador**: [Prefect 2.x](https://www.prefect.io/)
- **Banco de Dados**: [ClickHouse](https://clickhouse.com/) (Externo)
- **Infraestrutura**: Docker & Docker Compose
- **Linguagem**: Python 3.11+
- **Bibliotecas Chave**: `pandas`, `clickhouse-connect`, `facebook-business`, `pydantic-settings`.

## 📂 Estrutura do Projeto

```text
├── config/              # Configurações centralizadas (Pydantic)
├── connectors/          # Lógica de extração por plataforma (BaseConnector)
├── flows/               # Definição dos workflows do Prefect
├── scripts/             # Utilitários e simuladores
├── .env.example         # Modelo de variáveis de ambiente
├── docker-compose.yml   # Orquestração de containers (Server + Worker)
└── Dockerfile           # Imagem customizada do Worker
```

## 🏁 Como Iniciar

### 1. Preparar Ambiente
Copie o arquivo de exemplo e preencha suas credenciais:
```bash
cp .env.example .env
```
> **Nota**: Certifique-se de configurar o host do ClickHouse corretamente (ex: IP da máquina host ou endereço remoto).

### 2. Subir Infraestrutura
Execute o comando para provisionar o Prefect Server e o Worker:
```bash
docker-compose up -d
```
Acesse a UI do Prefect em: [http://localhost:4200](http://localhost:4200)

### 3. Registrar e Executar Flows (Deployments)
Para registrar um pipeline e torná-lo agendável via UI:
```bash
docker exec -it prefect-worker prefect deployment build flows/meta_ads_flow.py:meta_ads_pipeline -n "Meta Ads Daily" --pool default
docker exec -it prefect-worker prefect deployment apply meta_ads_pipeline-deployment.yaml
```

## 🧩 Adicionando Novos Conectores

O projeto segue o **SOLID**. Para adicionar uma nova plataforma:
1. Crie um arquivo em `connectors/` herdando de `BaseConnector`.
2. Implemente `extract(date_start, date_stop)` e `get_tables_ddl()`.
3. Adicione as credenciais em `config/settings.py` e `.env`.
4. Crie o arquivo de fluxo em `flows/` seguindo o padrão.

---
Desenvolvido com foco em alta performance e escalabilidade de dados.
