"""
Детектор падения использования сервисов экосистемы.
Читает: bank_marts.daily_service_usage
Логика: baseline по session_count и unique_services_used за 30 дней,
        сравниваем вчерашний день. Аномалия только в сторону падения (sigma < 0).
"""
import logging
from datetime import datetime, timedelta

from db.clickhouse import db
from detectors.base import calc_sigma, make_alert
from config import config

log = logging.getLogger(__name__)


def run() -> list[dict]:
    log.info("[detector:service_usage] Running...")
    alerts = []

    yesterday    = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    window_start = (datetime.utcnow() - timedelta(days=config.baseline_short_days + 1)).strftime("%Y-%m-%d")

    # Baseline: суммируем по клиенту (не по каждому сервису отдельно)
    # unique_services_used — глубина вовлечённости
    baseline_rows = db.mart_query(f"""
        SELECT
            client_id,
            avg(total_sessions)         AS mean_sessions,
            stddevPop(total_sessions)   AS std_sessions,
            avg(unique_services)        AS mean_services,
            stddevPop(unique_services)  AS std_services,
            count()                     AS days_count
        FROM (
            SELECT
                date,
                client_id,
                sum(session_count)      AS total_sessions,
                uniq(service_id)        AS unique_services
            FROM daily_service_usage
            WHERE date >= '{window_start}'
              AND date <  '{yesterday}'
            GROUP BY date, client_id
        )
        GROUP BY client_id
        HAVING days_count >= 5
    """)

    if not baseline_rows:
        log.info("[detector:service_usage] Not enough history yet.")
        return alerts

    baseline = {str(r["client_id"]): r for r in baseline_rows}

    # Факт: вчера по клиенту
    fact_rows = db.mart_query(f"""
        SELECT
            client_id,
            sum(session_count)  AS total_sessions,
            uniq(service_id)    AS unique_services
        FROM daily_service_usage
        WHERE date = '{yesterday}'
        GROUP BY client_id
    """)

    for row in fact_rows:
        cid = str(row["client_id"])
        b = baseline.get(cid)
        if not b:
            continue

        # session_count — аномалия только вниз
        sigma = calc_sigma(row["total_sessions"], b["mean_sessions"], b["std_sessions"])
        if sigma is not None and sigma <= -config.anomaly_sigma:
            alerts.append(make_alert(
                anomaly_type="service_usage_drop",
                entity_type="client",
                entity_id=cid,
                metric_name="session_count",
                metric_value=row["total_sessions"],
                mean=b["mean_sessions"],
                std=b["std_sessions"],
                sigma=sigma,
                details=f"Падение сессий: {row['total_sessions']:.0f} "
                        f"(норма {b['mean_sessions']:.1f} ± {b['std_sessions']:.1f})",
            ))

        # unique_services_used — аномалия только вниз
        sigma = calc_sigma(row["unique_services"], b["mean_services"], b["std_services"])
        if sigma is not None and sigma <= -config.anomaly_sigma:
            alerts.append(make_alert(
                anomaly_type="service_usage_drop",
                entity_type="client",
                entity_id=cid,
                metric_name="unique_services_used",
                metric_value=row["unique_services"],
                mean=b["mean_services"],
                std=b["std_services"],
                sigma=sigma,
                details=f"Сокращение числа используемых сервисов: {row['unique_services']:.0f} "
                        f"(норма {b['mean_services']:.1f} ± {b['std_services']:.1f})",
            ))

    log.info("[detector:service_usage] Found %d alerts.", len(alerts))
    return alerts
