"""
Детектор всплеска UX-затруднений.
Читает: bank_marts.daily_friction_stats
Логика: baseline по friction_event_count и funnel_success_rate за 30 дней.
        Аномалия:
          - friction_event_count вверх (sigma > +threshold)
          - funnel_success_rate  вниз  (sigma < -threshold)
"""
import logging
from datetime import datetime, timedelta

from db.clickhouse import db
from detectors.base import calc_sigma, make_alert
from config import config

log = logging.getLogger(__name__)


def run() -> list[dict]:
    log.info("[detector:friction] Running...")
    alerts = []

    yesterday    = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    window_start = (datetime.utcnow() - timedelta(days=config.baseline_short_days + 1)).strftime("%Y-%m-%d")

    # Baseline по клиенту (суммируем по всем воронкам)
    baseline_rows = db.mart_query(f"""
        SELECT
            client_id,
            avg(total_friction)          AS mean_friction,
            stddevPop(total_friction)    AS std_friction,
            avg(avg_success_rate)        AS mean_success_rate,
            stddevPop(avg_success_rate)  AS std_success_rate,
            avg(avg_duration)            AS mean_duration,
            stddevPop(avg_duration)      AS std_duration,
            count()                      AS days_count
        FROM (
            SELECT
                date,
                client_id,
                sum(friction_event_count)       AS total_friction,
                avg(funnel_success_rate)        AS avg_success_rate,
                avg(avg_task_duration_sec)      AS avg_duration
            FROM daily_friction_stats
            WHERE date >= '{window_start}'
              AND date <  '{yesterday}'
            GROUP BY date, client_id
        )
        GROUP BY client_id
        HAVING days_count >= 5
    """)

    if not baseline_rows:
        log.info("[detector:friction] Not enough history yet.")
        return alerts

    baseline = {str(r["client_id"]): r for r in baseline_rows}

    # Факт: вчера по клиенту
    fact_rows = db.mart_query(f"""
        SELECT
            client_id,
            sum(friction_event_count)       AS total_friction,
            avg(funnel_success_rate)        AS avg_success_rate,
            avg(avg_task_duration_sec)      AS avg_duration
        FROM daily_friction_stats
        WHERE date = '{yesterday}'
        GROUP BY client_id
    """)

    for row in fact_rows:
        cid = str(row["client_id"])
        b = baseline.get(cid)
        if not b:
            continue

        # friction_event_count — аномалия только вверх
        sigma = calc_sigma(row["total_friction"], b["mean_friction"], b["std_friction"])
        if sigma is not None and sigma >= config.anomaly_sigma:
            alerts.append(make_alert(
                anomaly_type="ux_friction_spike",
                entity_type="client",
                entity_id=cid,
                metric_name="friction_event_count",
                metric_value=row["total_friction"],
                mean=b["mean_friction"],
                std=b["std_friction"],
                sigma=sigma,
                details=f"Всплеск friction-событий: {row['total_friction']:.0f} "
                        f"(норма {b['mean_friction']:.1f} ± {b['std_friction']:.1f})",
            ))

        # funnel_success_rate — аномалия только вниз
        sigma = calc_sigma(row["avg_success_rate"], b["mean_success_rate"], b["std_success_rate"])
        if sigma is not None and sigma <= -config.anomaly_sigma:
            alerts.append(make_alert(
                anomaly_type="ux_friction_spike",
                entity_type="client",
                entity_id=cid,
                metric_name="funnel_success_rate",
                metric_value=row["avg_success_rate"],
                mean=b["mean_success_rate"],
                std=b["std_success_rate"],
                sigma=sigma,
                details=f"Падение успешности воронок: {row['avg_success_rate']:.2%} "
                        f"(норма {b['mean_success_rate']:.2%} ± {b['std_success_rate']:.2%})",
            ))

        # avg_task_duration_sec — аномалия только вверх
        sigma = calc_sigma(row["avg_duration"], b["mean_duration"], b["std_duration"])
        if sigma is not None and sigma >= config.anomaly_sigma:
            alerts.append(make_alert(
                anomaly_type="ux_friction_spike",
                entity_type="client",
                entity_id=cid,
                metric_name="avg_task_duration_sec",
                metric_value=row["avg_duration"],
                mean=b["mean_duration"],
                std=b["std_duration"],
                sigma=sigma,
                details=f"Рост времени выполнения задач: {row['avg_duration']:.0f}с "
                        f"(норма {b['mean_duration']:.0f} ± {b['std_duration']:.0f})",
            ))

    log.info("[detector:friction] Found %d alerts.", len(alerts))
    return alerts
