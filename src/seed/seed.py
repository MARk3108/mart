"""
Сидер исторических данных для mart.

Генерирует 7 дней истории (15–21 мая 2026) прямо в bank_marts:
  - daily_turnover        (по каждому business_id из bank_raw)
  - daily_service_usage   (по каждому client_id + service_id из bank_raw)
  - daily_friction_stats  (по каждому client_id из bank_raw)

95% дней — нормальные значения на основе текущих агрегатов из bank_raw.
5% дней — аномальные выбросы (чтобы baseline сразу имел вариацию).

После запуска сидера детекторы начнут находить аномалии в текущих данных.
"""
import logging
import random
import uuid
from datetime import date, timedelta

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from config import config

log = logging.getLogger(__name__)
rng = random.Random(42)   # фиксированный seed для воспроизводимости

# Даты: 7 дней до сегодня (15–21 мая 2026)
TODAY = date(2026, 5, 22)
SEED_DAYS = [TODAY - timedelta(days=i) for i in range(1, 8)]  # вчера и 6 дней назад


def _source() -> Client:
    return Client(
        host=config.source_host, port=config.source_port,
        user=config.source_user, password=config.source_password,
        database=config.source_db, connect_timeout=10,
    )


def _mart() -> Client:
    return Client(
        host=config.mart_host, port=config.mart_port,
        user=config.mart_user, password=config.mart_password,
        database=config.mart_db, connect_timeout=10,
    )


def _query(client: Client, sql: str) -> list[dict]:
    rows, cols = client.execute(sql, with_column_types=True)
    col_names = [c[0] for c in cols]
    return [dict(zip(col_names, row)) for row in rows]


def _insert(client: Client, table: str, rows: list[dict]) -> None:
    if rows:
        client.execute(f"INSERT INTO {table} VALUES", rows)
        log.info("  → %s: inserted %d rows", table, len(rows))


def _noisy(value: float, noise: float = 0.25) -> float:
    """Добавляем случайный шум ±noise*100% к значению."""
    return max(0.0, value * (1 + rng.uniform(-noise, noise)))


def _anomaly_multiplier() -> float:
    """5% шанс аномального значения — в 3–5 раз выше или ниже нормы."""
    if rng.random() < 0.05:
        return rng.choice([
            rng.uniform(0.05, 0.15),   # резкое падение
            rng.uniform(3.0, 5.0),     # резкий всплеск
        ])
    return 1.0


# ── Сидер 1: daily_turnover ────────────────────────────────────────────────

def seed_daily_turnover(src: Client, mart: Client) -> None:
    log.info("[seed] daily_turnover...")

    # Берём текущие агрегаты из bank_raw как базу для нормы
    base_rows = _query(src, """
        SELECT
            business_id,
            avg(amount)                                     AS avg_amount,
            countIf(direction = 'приток')                   AS inflow_count,
            countIf(direction = 'отток')                    AS outflow_count,
            sumIf(amount, direction = 'приток')             AS inflow_sum,
            sumIf(amount, direction = 'отток')              AS outflow_sum,
            uniq(counterparty_id)                           AS unique_counterparties,
            sumIf(amount, is_cash_withdrawal = 1)           AS cash_withdrawal_sum,
            avg(balance_after)                              AS balance_avg
        FROM transactions
        WHERE status = 'успех'
        GROUP BY business_id
        LIMIT 200
    """)

    if not base_rows:
        log.warning("[seed] No transactions in bank_raw yet, skipping daily_turnover seed.")
        return

    rows = []
    for d in SEED_DAYS:
        for b in base_rows:
            mult = _anomaly_multiplier()
            inflow  = round(_noisy(float(b["inflow_sum"]  or 0)) * mult, 2)
            outflow = round(_noisy(float(b["outflow_sum"] or 0)) * mult, 2)
            tx      = max(1, int(_noisy(float((b["inflow_count"] or 0) + (b["outflow_count"] or 0))) * mult))
            bal_avg = round(_noisy(float(b["balance_avg"] or 10000)), 2)

            rows.append({
                "date":                  d,
                "business_id":           str(b["business_id"]),
                "inflow_sum":            inflow,
                "outflow_sum":           outflow,
                "inflow_count":          max(0, int(tx * 0.55)),
                "outflow_count":         max(0, int(tx * 0.45)),
                "tx_count":              tx,
                "avg_tx_amount":         round(inflow / max(tx, 1), 2),
                "unique_counterparties": max(1, int(_noisy(float(b["unique_counterparties"] or 1)))),
                "active_day":            1,
                "cash_withdrawal_sum":   round(_noisy(float(b["cash_withdrawal_sum"] or 0)), 2),
                "balance_avg":           bal_avg,
                "balance_min":           round(bal_avg * rng.uniform(0.5, 0.9), 2),
                "balance_volatility":    round(bal_avg * rng.uniform(0.01, 0.1), 2),
            })

    _insert(mart, "daily_turnover", rows)


# ── Сидер 2: daily_service_usage ───────────────────────────────────────────

def seed_daily_service_usage(src: Client, mart: Client) -> None:
    log.info("[seed] daily_service_usage...")

    base_rows = _query(src, """
        SELECT
            client_id,
            service_id,
            countIf(event_type = 'session_start')   AS session_count,
            count()                                  AS event_count,
            countIf(event_type = 'purchase')         AS tx_count,
            countIf(event_type = 'cancel')           AS cancel_count
        FROM service_events
        GROUP BY client_id, service_id
        LIMIT 1000
    """)

    if not base_rows:
        log.warning("[seed] No service_events in bank_raw yet, skipping.")
        return

    rows = []
    for d in SEED_DAYS:
        for b in base_rows:
            mult = _anomaly_multiplier()
            sess = max(0, int(_noisy(float(b["session_count"] or 1)) * mult))
            rows.append({
                "date":          d,
                "client_id":     str(b["client_id"]),
                "service_id":    str(b["service_id"]),
                "session_count": sess,
                "event_count":   max(sess, int(_noisy(float(b["event_count"] or 1)) * mult)),
                "tx_sum":        round(rng.uniform(0, 50000) * mult, 2),
                "tx_count":      max(0, int(_noisy(float(b["tx_count"] or 0)) * mult)),
                "cancel_count":  max(0, int(_noisy(float(b["cancel_count"] or 0)))),
            })

    _insert(mart, "daily_service_usage", rows)


# ── Сидер 3: daily_friction_stats ──────────────────────────────────────────

def seed_daily_friction_stats(src: Client, mart: Client) -> None:
    log.info("[seed] daily_friction_stats...")

    base_rows = _query(src, """
        SELECT
            client_id,
            count()                                         AS friction_count,
            countIf(event_type = 'rage_click')             AS rage_click,
            countIf(event_type = 'idle')                   AS idle,
            countIf(event_type = 'ui_error')               AS ui_error,
            countIf(event_type = 'exit_without_action')    AS exit_without
        FROM ux_events
        GROUP BY client_id
        LIMIT 500
    """)

    session_rows = _query(src, """
        SELECT
            client_id,
            count()                   AS session_count,
            countIf(is_completed = 1) AS completed,
            avg(duration_sec)         AS avg_duration
        FROM sessions
        GROUP BY client_id
        LIMIT 500
    """)

    sess_index = {str(r["client_id"]): r for r in session_rows}

    if not base_rows:
        log.warning("[seed] No ux_events in bank_raw yet, skipping.")
        return

    null_uuid = "00000000-0000-0000-0000-000000000000"

    rows = []
    for d in SEED_DAYS:
        for b in base_rows:
            cid  = str(b["client_id"])
            mult = _anomaly_multiplier()
            sess = sess_index.get(cid, {})

            session_count   = max(1, int(_noisy(float(sess.get("session_count") or 1))))
            completed_count = max(0, int(session_count * rng.uniform(0.7, 0.95)))
            success_rate    = round(completed_count / session_count, 4)
            friction        = max(0, int(_noisy(float(b["friction_count"] or 1)) * mult))

            rows.append({
                "date":                      d,
                "client_id":                 cid,
                "funnel_id":                 null_uuid,
                "friction_event_count":      friction,
                "rage_click_count":          max(0, int(_noisy(float(b["rage_click"] or 0)) * mult)),
                "idle_count":                max(0, int(_noisy(float(b["idle"] or 0)) * mult)),
                "ui_error_count":            max(0, int(_noisy(float(b["ui_error"] or 0)) * mult)),
                "exit_without_action_count": max(0, int(_noisy(float(b["exit_without"] or 0)))),
                "session_count":             session_count,
                "completed_session_count":   completed_count,
                "funnel_success_rate":       success_rate,
                "avg_task_duration_sec":     round(_noisy(float(sess.get("avg_duration") or 120)), 2),
                "ux_tickets_count":          max(0, int(rng.uniform(0, 3))),
                "is_active_day":             1,
            })

    _insert(mart, "daily_friction_stats", rows)


# ── Точка входа ─────────────────────────────────────────────────────────────

def run_seed() -> None:
    log.info("=== Starting historical seed (days: %s) ===",
             [str(d) for d in SEED_DAYS])

    src  = _source()
    mart = _mart()

    # Проверяем — может уже засеяно
    existing_dt = mart.execute("SELECT count() FROM daily_turnover WHERE date < today()")[0][0]
    existing_su = mart.execute("SELECT count() FROM daily_service_usage WHERE date < today()")[0][0]
    existing_fr = mart.execute("SELECT count() FROM daily_friction_stats WHERE date < today()")[0][0]

    if existing_dt > 0 and existing_su > 0 and existing_fr > 0:
        log.info("Historical data already exists, skipping seed.")
        return

    log.info("Existing counts: turnover=%d service_usage=%d friction=%d",
        existing_dt, existing_su, existing_fr)

    seed_daily_turnover(src, mart)
    seed_daily_service_usage(src, mart)
    seed_daily_friction_stats(src, mart)

    log.info("=== Historical seed complete ===")
