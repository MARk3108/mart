"""
Job: агрегация оборотов МСБ.
Читает: bank_raw.transactions
Пишет: bank_marts.daily_turnover, bank_marts.monthly_turnover
"""
import logging
from datetime import datetime, timedelta

from db.clickhouse import db

log = logging.getLogger(__name__)


def run() -> None:
    log.info("[turnover] Starting aggregation...")

    # Агрегируем последние 2 дня с запасом (вчера мог не досчитаться)
    since = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")

    # ── Daily turnover ──────────────────────────────────────────
    rows = db.query(f"""
        SELECT
            toDate(operation_time)                          AS date,
            business_id,
            sumIf(amount, direction = 'приток')             AS inflow_sum,
            sumIf(amount, direction = 'отток')              AS outflow_sum,
            countIf(direction = 'приток')                   AS inflow_count,
            countIf(direction = 'отток')                    AS outflow_count,
            count()                                         AS tx_count,
            avg(amount)                                     AS avg_tx_amount,
            uniq(counterparty_id)                           AS unique_counterparties,
            toUInt8(count() > 0)                            AS active_day,
            sumIf(amount, is_cash_withdrawal = 1)           AS cash_withdrawal_sum,
            avg(balance_after)                              AS balance_avg,
            min(balance_after)                              AS balance_min,
            stddevPop(balance_after)                        AS balance_volatility
        FROM transactions
        WHERE toDate(operation_time) >= '{since}'
          AND status = 'успех'
        GROUP BY date, business_id
    """)

    if rows:
        db.insert("daily_turnover", rows)
        log.info("[turnover] daily_turnover: upserted %d rows", len(rows))

    # ── Monthly turnover ────────────────────────────────────────
    since_month = (datetime.utcnow() - timedelta(days=32)).strftime("%Y-%m-01")

    rows = db.query(f"""
        SELECT
            toStartOfMonth(operation_time)      AS month,
            business_id,
            sumIf(amount, direction = 'приток') AS inflow_sum,
            sumIf(amount, direction = 'отток')  AS outflow_sum,
            count()                             AS tx_count,
            avg(balance_after)                  AS avg_balance,
            uniq(counterparty_id)               AS unique_counterparties
        FROM transactions
        WHERE toStartOfMonth(operation_time) >= '{since_month}'
          AND status = 'успех'
        GROUP BY month, business_id
    """)

    if rows:
        db.insert("monthly_turnover", rows)
        log.info("[turnover] monthly_turnover: upserted %d rows", len(rows))
