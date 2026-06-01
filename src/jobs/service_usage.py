"""
Job: агрегация использования сервисов экосистемы.
Читает: bank_raw.service_events
Пишет: bank_marts.daily_service_usage, bank_marts.monthly_service_usage
"""
import logging
from datetime import datetime, timedelta

from db.clickhouse import db

log = logging.getLogger(__name__)


def run() -> None:
    log.info("[service_usage] Starting aggregation...")

    since = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")

    # ── Daily service usage ─────────────────────────────────────
    # ifNull(amount, 0) — чтобы избежать None в Decimal колонке
    rows = db.query(f"""
        SELECT
            toDate(event_time)                                      AS date,
            client_id,
            service_id,
            countIf(event_type = 'session_start')                  AS session_count,
            count()                                                 AS event_count,
            toFloat64(sumIf(
                ifNull(amount, 0),
                event_type = 'purchase' AND amount IS NOT NULL
            ))                                                      AS tx_sum,
            countIf(event_type = 'purchase')                       AS tx_count,
            countIf(event_type = 'cancel')                         AS cancel_count
        FROM service_events
        WHERE toDate(event_time) >= '{since}'
        GROUP BY date, client_id, service_id
    """)

    # Приводим tx_sum к float на случай если CH вернул Decimal
    for r in rows:
        r["tx_sum"] = float(r["tx_sum"] or 0)

    if rows:
        db.insert("daily_service_usage", rows)
        log.info("[service_usage] daily_service_usage: upserted %d rows", len(rows))

    # ── Monthly service usage ───────────────────────────────────
    since_month = (datetime.utcnow() - timedelta(days=32)).strftime("%Y-%m-01")

    rows = db.query(f"""
        SELECT
            toStartOfMonth(event_time)                             AS month,
            client_id,
            service_id,
            uniq(toDate(event_time))                               AS active_days,
            countIf(event_type = 'session_start')                  AS session_count,
            toFloat64(sumIf(
                ifNull(amount, 0),
                event_type = 'purchase' AND amount IS NOT NULL
            ))                                                      AS tx_sum,
            uniq(service_id)                                       AS unique_services_used
        FROM service_events
        WHERE toStartOfMonth(event_time) >= '{since_month}'
        GROUP BY month, client_id, service_id
    """)

    for r in rows:
        r["tx_sum"] = float(r["tx_sum"] or 0)

    if rows:
        db.insert("monthly_service_usage", rows)
        log.info("[service_usage] monthly_service_usage: upserted %d rows", len(rows))
