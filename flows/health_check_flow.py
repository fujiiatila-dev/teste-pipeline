from prefect import flow, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@task
def check_clickhouse():
    """Verifica conectividade com o ClickHouse."""
    from connectors.clickhouse_client import ClickHouseClient
    try:
        ch = ClickHouseClient()
        ch.ping()
        return {"service": "ClickHouse", "status": "ok", "error": ""}
    except Exception as e:
        return {"service": "ClickHouse", "status": "fail", "error": str(e)}


@task
def check_minio():
    """Verifica conectividade com o MinIO."""
    from connectors.datalake import DatalakeConnector
    try:
        lake = DatalakeConnector()
        lake.s3_client.list_buckets()
        return {"service": "MinIO", "status": "ok", "error": ""}
    except Exception as e:
        return {"service": "MinIO", "status": "fail", "error": str(e)}


@task
def check_prefect_api():
    """Verifica conectividade com o Prefect API."""
    import requests
    import os
    try:
        api_url = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")
        resp = requests.get(f"{api_url}/health", timeout=10)
        resp.raise_for_status()
        return {"service": "Prefect API", "status": "ok", "error": ""}
    except Exception as e:
        return {"service": "Prefect API", "status": "fail", "error": str(e)}


@task
def check_google_sheets():
    """Verifica acesso à planilha de configuração."""
    import requests
    try:
        sheet_id = "1ZA4rVPpHqDNvdw7t1gajgoCeV1uAaIM_sdI90BUCKIE"
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return {"service": "Google Sheets", "status": "ok", "error": ""}
    except Exception as e:
        return {"service": "Google Sheets", "status": "fail", "error": str(e)}


@task
def check_data_freshness():
    """Verifica tabelas com dados desatualizados (>48h sem updated_at recente)."""
    from connectors.clickhouse_client import ClickHouseClient
    stale_tables = []
    try:
        ch = ClickHouseClient()
        tables_result = ch.client.query(
            f"SELECT name FROM system.tables WHERE database = '{ch.client.database}'"
        )
        tables = [row[0] for row in tables_result.result_rows]

        for table in tables:
            try:
                result = ch.client.query(
                    f"SELECT max(updated_at) as last_update FROM {table}"
                )
                if result.result_rows and result.result_rows[0][0]:
                    last_update = result.result_rows[0][0]
                    if isinstance(last_update, datetime):
                        hours_stale = (datetime.now() - last_update).total_seconds() / 3600
                        if hours_stale > 48:
                            stale_tables.append({
                                "table": table,
                                "last_update": last_update.strftime("%Y-%m-%d %H:%M"),
                                "hours_stale": round(hours_stale, 1),
                            })
            except Exception:
                pass
    except Exception as e:
        logger.warning("Não foi possível verificar freshness: %s", e)

    return stale_tables


@task
def send_alerts(results: list, stale_tables: list):
    """Envia alertas por email se houver falhas."""
    from scripts.alerting import alert_health_check, alert_data_freshness

    failures = [r for r in results if r["status"] == "fail"]

    if failures:
        logger.error("Health check: %d serviço(s) com falha", len(failures))
        alert_health_check(failures)
    else:
        logger.info("Health check: todos os serviços OK")

    if stale_tables:
        logger.warning("Data freshness: %d tabela(s) desatualizada(s)", len(stale_tables))
        alert_data_freshness(stale_tables)

    return {
        "services_ok": len([r for r in results if r["status"] == "ok"]),
        "services_fail": len(failures),
        "stale_tables": len(stale_tables),
    }


@flow(name="Infrastructure Health Check")
def health_check_pipeline():
    """
    Verifica a saúde de toda a infraestrutura do pipeline:
    - ClickHouse (warehouse)
    - MinIO (data lake)
    - Prefect API (orquestrador)
    - Google Sheets (configuração de clientes)
    - Data Freshness (tabelas desatualizadas)

    Envia alerta por email para suporte@nalk.com.br em caso de falha.
    """
    results = [
        check_clickhouse(),
        check_minio(),
        check_prefect_api(),
        check_google_sheets(),
    ]

    stale_tables = check_data_freshness()

    for r in results:
        status_icon = "OK" if r["status"] == "ok" else "FAIL"
        logger.info("[%s] %s %s", status_icon, r["service"],
                     f'- {r["error"]}' if r["error"] else "")

    summary = send_alerts(results, stale_tables)
    print(f"\nHealth Check Summary: {summary['services_ok']} OK, "
          f"{summary['services_fail']} FAIL, "
          f"{summary['stale_tables']} tabelas desatualizadas")

    return summary


if __name__ == "__main__":
    health_check_pipeline()
