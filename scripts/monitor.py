import os
import logging
import psutil
from datetime import datetime

logger = logging.getLogger(__name__)


class ResourceMonitor:
    """
    Monitor de recursos do sistema (CPU, memória, disco).
    Usado para detectar problemas de performance nos workers.
    """

    MEMORY_THRESHOLD_PERCENT = 85
    CPU_THRESHOLD_PERCENT = 90
    DISK_THRESHOLD_PERCENT = 90

    @staticmethod
    def get_metrics() -> dict:
        """Coleta métricas atuais do sistema."""
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        cpu_percent = psutil.cpu_percent(interval=1)

        return {
            "timestamp": datetime.now().isoformat(),
            "memory": {
                "total_gb": round(mem.total / (1024**3), 2),
                "used_gb": round(mem.used / (1024**3), 2),
                "percent": mem.percent,
            },
            "cpu": {
                "percent": cpu_percent,
                "count": psutil.cpu_count(),
            },
            "disk": {
                "total_gb": round(disk.total / (1024**3), 2),
                "used_gb": round(disk.used / (1024**3), 2),
                "percent": round(disk.used / disk.total * 100, 1),
            },
        }

    @classmethod
    def check_thresholds(cls) -> list:
        """Retorna lista de alertas se algum recurso ultrapassou o threshold."""
        metrics = cls.get_metrics()
        alerts = []

        if metrics["memory"]["percent"] > cls.MEMORY_THRESHOLD_PERCENT:
            alerts.append({
                "resource": "Memória",
                "value": f'{metrics["memory"]["percent"]}%',
                "threshold": f'{cls.MEMORY_THRESHOLD_PERCENT}%',
                "detail": f'{metrics["memory"]["used_gb"]}GB / {metrics["memory"]["total_gb"]}GB',
            })

        if metrics["cpu"]["percent"] > cls.CPU_THRESHOLD_PERCENT:
            alerts.append({
                "resource": "CPU",
                "value": f'{metrics["cpu"]["percent"]}%',
                "threshold": f'{cls.CPU_THRESHOLD_PERCENT}%',
                "detail": f'{metrics["cpu"]["count"]} cores',
            })

        if metrics["disk"]["percent"] > cls.DISK_THRESHOLD_PERCENT:
            alerts.append({
                "resource": "Disco",
                "value": f'{metrics["disk"]["percent"]}%',
                "threshold": f'{cls.DISK_THRESHOLD_PERCENT}%',
                "detail": f'{metrics["disk"]["used_gb"]}GB / {metrics["disk"]["total_gb"]}GB',
            })

        return alerts

    @classmethod
    def log_metrics(cls):
        """Loga métricas atuais."""
        metrics = cls.get_metrics()
        logger.info(
            "Resources: MEM=%s%% CPU=%s%% DISK=%s%%",
            metrics["memory"]["percent"],
            metrics["cpu"]["percent"],
            metrics["disk"]["percent"],
        )

        alerts = cls.check_thresholds()
        for alert in alerts:
            logger.warning(
                "RESOURCE ALERT: %s em %s (threshold: %s) - %s",
                alert["resource"], alert["value"], alert["threshold"], alert["detail"],
            )
        return metrics, alerts
