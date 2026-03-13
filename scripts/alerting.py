import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from config.settings import settings

logger = logging.getLogger(__name__)


def send_email_alert(subject: str, body: str, html: bool = False):
    """
    Envia alerta por email via SMTP.
    Requer SMTP_USER e SMTP_PASSWORD configurados.
    """
    if not settings.smtp_user or not settings.smtp_password:
        logger.warning("SMTP não configurado. Alerta não enviado: %s", subject)
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Pipeline Alert] {subject}"
    msg["From"] = settings.alert_email_from
    msg["To"] = settings.alert_email_to

    content_type = "html" if html else "plain"
    msg.attach(MIMEText(body, content_type))

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_password)
            server.send_message(msg)
        logger.info("Alerta enviado para %s: %s", settings.alert_email_to, subject)
        return True
    except Exception as e:
        logger.error("Falha ao enviar email: %s", e)
        return False


def alert_flow_failure(flow_name: str, error: str, client_name: str = None):
    """Alerta de falha de flow formatado em HTML."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    client_info = f"<tr><td><b>Cliente</b></td><td>{client_name}</td></tr>" if client_name else ""

    body = f"""
    <html><body>
    <h2 style="color:#d32f2f;">Flow Falhou</h2>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
        <tr><td><b>Flow</b></td><td>{flow_name}</td></tr>
        {client_info}
        <tr><td><b>Erro</b></td><td><pre>{error}</pre></td></tr>
        <tr><td><b>Horário</b></td><td>{timestamp}</td></tr>
    </table>
    </body></html>
    """
    return send_email_alert(f"FALHA - {flow_name}", body, html=True)


def alert_health_check(failures: list):
    """Alerta de health check com lista de serviços com problema."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = ""
    for f in failures:
        rows += f'<tr><td>{f["service"]}</td><td style="color:red;">{f["status"]}</td><td>{f["error"]}</td></tr>'

    body = f"""
    <html><body>
    <h2 style="color:#e65100;">Health Check - Serviços Indisponíveis</h2>
    <p>Verificação em: {timestamp}</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
        <tr style="background:#f5f5f5;"><th>Serviço</th><th>Status</th><th>Erro</th></tr>
        {rows}
    </table>
    </body></html>
    """
    return send_email_alert("HEALTH CHECK - Serviços Indisponíveis", body, html=True)


def alert_data_freshness(stale_tables: list):
    """Alerta de tabelas com dados desatualizados."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = ""
    for t in stale_tables:
        rows += f'<tr><td>{t["table"]}</td><td>{t["last_update"]}</td><td>{t["hours_stale"]}h</td></tr>'

    body = f"""
    <html><body>
    <h2 style="color:#f57c00;">Dados Desatualizados</h2>
    <p>Verificação em: {timestamp}</p>
    <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;">
        <tr style="background:#f5f5f5;"><th>Tabela</th><th>Última Atualização</th><th>Atraso</th></tr>
        {rows}
    </table>
    </body></html>
    """
    return send_email_alert("ALERTA - Dados Desatualizados", body, html=True)
