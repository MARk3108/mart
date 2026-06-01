"""
Job: агрегация UX-затруднений.
Читает: bank_raw.ux_events, bank_raw.sessions, bank_raw.support_tickets
Пишет: bank_marts.daily_friction_stats
"""
import logging
from datetime import datetime, timedelta

from db.clickhouse import db

log = logging.getLogger(__name__)


def run() -> None:
    log.info("[friction] Starting aggregation...")

    since = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")

    # ── UX events агрегат ───────────────────────────────────────
    ux_rows = db.query(f"""
        SELECT
            toDate(event_time)                                          AS date,
            client_id,
            ifNull(funnel_id, toUUID('00000000-0000-0000-0000-000000000000'))
                                                                        AS funnel_id,
            count()                                                     AS friction_event_count,
            countIf(event_type = 'rage_click')                         AS rage_click_count,
            countIf(event_type = 'idle')                               AS idle_count,
            countIf(event_type = 'ui_error')                           AS ui_error_count,
            countIf(event_type = 'exit_without_action')                AS exit_without_action_count
        FROM ux_events
        WHERE toDate(event_time) >= '{since}'
        GROUP BY date, client_id, funnel_id
    """)

    # ── Sessions агрегат ────────────────────────────────────────
    sess_rows = db.query(f"""
        SELECT
            toDate(start_time)          AS date,
            client_id,
            ifNull(funnel_id, toUUID('00000000-0000-0000-0000-000000000000'))
                                        AS funnel_id,
            count()                     AS session_count,
            countIf(is_completed = 1)   AS completed_session_count,
            avg(duration_sec)           AS avg_task_duration_sec
        FROM sessions
        WHERE toDate(start_time) >= '{since}'
        GROUP BY date, client_id, funnel_id
    """)

    # ── Support tickets (UX-related) ────────────────────────────
    ticket_rows = db.query(f"""
        SELECT
            toDate(created_at)  AS date,
            client_id,
            count()             AS ux_tickets_count
        FROM support_tickets
        WHERE toDate(created_at) >= '{since}'
          AND is_ux_related = 1
        GROUP BY date, client_id
    """)

    # ── Объединяем в памяти ─────────────────────────────────────
    # Индексируем по (date, client_id, funnel_id)
    ux_index = {
        (r["date"], str(r["client_id"]), str(r["funnel_id"])): r
        for r in ux_rows
    }
    sess_index = {
        (r["date"], str(r["client_id"]), str(r["funnel_id"])): r
        for r in sess_rows
    }
    ticket_index = {
        (r["date"], str(r["client_id"])): r["ux_tickets_count"]
        for r in ticket_rows
    }

    # Все уникальные ключи
    all_keys = set(ux_index.keys()) | set(sess_index.keys())

    result = []
    for key in all_keys:
        date, client_id, funnel_id = key
        ux  = ux_index.get(key, {})
        ses = sess_index.get(key, {})

        session_count   = ses.get("session_count", 0)
        completed_count = ses.get("completed_session_count", 0)
        success_rate    = (completed_count / session_count) if session_count > 0 else 0.0

        result.append({
            "date":                      date,
            "client_id":                 client_id,
            "funnel_id":                 funnel_id,
            "friction_event_count":      ux.get("friction_event_count", 0),
            "rage_click_count":          ux.get("rage_click_count", 0),
            "idle_count":                ux.get("idle_count", 0),
            "ui_error_count":            ux.get("ui_error_count", 0),
            "exit_without_action_count": ux.get("exit_without_action_count", 0),
            "session_count":             session_count,
            "completed_session_count":   completed_count,
            "funnel_success_rate":       round(success_rate, 4),
            "avg_task_duration_sec":     round(ses.get("avg_task_duration_sec") or 0.0, 2),
            "ux_tickets_count":          ticket_index.get((date, client_id), 0),
            "is_active_day":             1,
        })

    if result:
        db.insert("daily_friction_stats", result)
        log.info("[friction] daily_friction_stats: upserted %d rows", len(result))
