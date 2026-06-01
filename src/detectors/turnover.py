"""
Детектор аномалий оборотов МСБ.
Читает: bank_marts.daily_turnover
Логика: baseline по inflow_sum и tx_count за 30 дней,
        сравниваем вчерашний день.
"""
import logging
from datetime import datetime, timedelta

from db.clickhouse import db
from detectors.base import calc_sigma, make_alert
from config import config

log = logging.getLogger(__name__)


def run() -> list[dict]:
    log.info("[detector:turnover] Running...")
    alerts = []

    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    window_start = (datetime.utcnow() - timedelta(days=config.baseline_short_days + 1)).strftime("%Y-%m-%d")
    min_history  = (datetime.utcnow() - timedelta(days=config.min_history_days)).strftime("%Y-%m-%d")

    # Baseline: mean + std за 30 дней до вчера, по каждому бизнесу
    baseline_rows = db.mart_query(f"""
        SELECT
            business_id,
            avg(inflow_sum)      AS mean_inflow,
            stddevPop(inflow_sum) AS std_inflow,
            avg(tx_count)        AS mean_tx,
            stddevPop(tx_count)  AS std_tx,
            count()              AS days_count
        FROM daily_turnover
        WHERE date >= '{window_start}'
          AND date <  '{yesterday}'
        GROUP BY business_id
        HAVING days_count >= 5
    """)

    if not baseline_rows:
        log.info("[detector:turnover] Not enough history yet.")
        return alerts

    baseline = {str(r["business_id"]): r for r in baseline_rows}

    # Факт: вчерашние значения
    fact_rows = db.mart_query(f"""
        SELECT business_id, inflow_sum, tx_count
        FROM daily_turnover
        WHERE date = '{yesterday}'
    """)

    for row in fact_rows:
        bid = str(row["business_id"])
        b = baseline.get(bid)
        if not b:
            continue

        # Проверяем inflow_sum
        sigma = calc_sigma(row["inflow_sum"], b["mean_inflow"], b["std_inflow"])
        if sigma is not None and abs(sigma) >= config.anomaly_sigma:
            direction = "падение" if sigma < 0 else "всплеск"
            alerts.append(make_alert(
                anomaly_type="turnover_anomaly",
                entity_type="business",
                entity_id=bid,
                metric_name="inflow_sum",
                metric_value=row["inflow_sum"],
                mean=b["mean_inflow"],
                std=b["std_inflow"],
                sigma=sigma,
                details=f"{direction} входящих оборотов: {row['inflow_sum']:.0f} руб "
                        f"(норма {b['mean_inflow']:.0f} ± {b['std_inflow']:.0f})",
            ))

        # Проверяем tx_count
        sigma = calc_sigma(row["tx_count"], b["mean_tx"], b["std_tx"])
        if sigma is not None and abs(sigma) >= config.anomaly_sigma:
            direction = "падение" if sigma < 0 else "всплеск"
            alerts.append(make_alert(
                anomaly_type="turnover_anomaly",
                entity_type="business",
                entity_id=bid,
                metric_name="tx_count",
                metric_value=row["tx_count"],
                mean=b["mean_tx"],
                std=b["std_tx"],
                sigma=sigma,
                details=f"{direction} числа транзакций: {row['tx_count']} "
                        f"(норма {b['mean_tx']:.1f} ± {b['std_tx']:.1f})",
            ))

    log.info("[detector:turnover] Found %d alerts.", len(alerts))
    return alerts
