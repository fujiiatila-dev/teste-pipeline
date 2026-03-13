import requests
import time
import logging
from datetime import datetime
from config.settings import settings

logger = logging.getLogger(__name__)


class WebhookNotifier:
    """
    Envia notificações de status de flows para o Backoffice via webhook.
    Implementa retry com backoff exponencial.
    """

    MAX_ATTEMPTS = 5
    BASE_DELAY_SECONDS = 10

    def __init__(self, endpoint: str = None, auth_token: str = None):
        self.endpoint = endpoint or settings.backoffice_endpoint
        self.auth_token = auth_token or settings.backoffice_auth_token

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "x-custom-token": self.auth_token,
        }

    def send(self, payload: dict) -> bool:
        """Envia payload com retry e backoff exponencial."""
        if not self.endpoint or not self.auth_token:
            logger.debug("Webhook não configurado. Pulando notificação.")
            return False

        url = f"{self.endpoint.rstrip('/')}/airflow/webhook"

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            try:
                resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
                resp.raise_for_status()
                logger.info("Webhook enviado com sucesso (tentativa %d)", attempt)
                return True
            except Exception as e:
                delay = self.BASE_DELAY_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "Webhook falhou (tentativa %d/%d): %s. Retry em %ds",
                    attempt, self.MAX_ATTEMPTS, e, delay,
                )
                if attempt < self.MAX_ATTEMPTS:
                    time.sleep(delay)

        logger.error("Webhook falhou após %d tentativas", self.MAX_ATTEMPTS)
        return False

    def notify_flow_start(self, flow_name: str, company_id: str = None):
        """Notifica início de execução de flow."""
        return self.send({
            "type": "flow_status",
            "status": "running",
            "flow_name": flow_name,
            "company_id": company_id,
            "timestamp": datetime.now().isoformat(),
        })

    def notify_flow_success(self, flow_name: str, company_id: str = None,
                            tables_loaded: int = 0, rows_total: int = 0):
        """Notifica conclusão com sucesso de flow."""
        return self.send({
            "type": "flow_status",
            "status": "completed",
            "flow_name": flow_name,
            "company_id": company_id,
            "tables_loaded": tables_loaded,
            "rows_total": rows_total,
            "timestamp": datetime.now().isoformat(),
        })

    def notify_flow_failure(self, flow_name: str, error: str, company_id: str = None):
        """Notifica falha de flow."""
        return self.send({
            "type": "flow_status",
            "status": "failed",
            "flow_name": flow_name,
            "company_id": company_id,
            "error": str(error)[:500],
            "timestamp": datetime.now().isoformat(),
        })
