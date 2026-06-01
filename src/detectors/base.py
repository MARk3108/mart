"""
Базовый детектор аномалий.
Логика: считаем baseline (mean + std) за окно N дней,
сравниваем текущее значение, если отклонение > anomaly_sigma → алерт.
"""
import logging
import math
import uuid
from datetime import datetime
from typing import Optional

from config import config

log = logging.getLogger(__name__)


def calc_sigma(value: float, mean: float, std: float) -> Optional[float]:
    """Отклонение в единицах σ. None если std=0 (нет вариации — нет аномалии)."""
    if std is None or std < 1e-9:
        return None
    return (float(value) - float(mean)) / float(std)


def severity(sigma: float) -> str:
    """Маппинг отклонения в уровень критичности."""
    abs_sigma = abs(sigma)
    if abs_sigma >= 4.0:
        return "critical"
    if abs_sigma >= 3.0:
        return "high"
    if abs_sigma >= 2.5:
        return "medium"
    return "low"


def make_alert(
    anomaly_type: str,
    entity_type: str,
    entity_id: str,
    metric_name: str,
    metric_value: float,
    mean: float,
    std: float,
    sigma: float,
    details: str = "",
) -> dict:
    return {
        "alert_id":        str(uuid.uuid4()),
        "detected_at":     datetime.utcnow(),
        "anomaly_type":    anomaly_type,
        "entity_type":     entity_type,
        "entity_id":       entity_id,
        "metric_name":     metric_name,
        "metric_value":    round(float(metric_value), 4),
        "baseline_mean":   round(float(mean), 4),
        "baseline_std":    round(float(std), 4),
        "deviation_sigma": round(float(sigma), 4),
        "severity":        severity(sigma),
        "details":         details,
        "is_resolved":     0,
    }
