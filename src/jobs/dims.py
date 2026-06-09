"""
Job: синхронизация справочников из PostgreSQL (processor) в mart ClickHouse.

Запускается:
  - при старте mart сервиса
  - раз в сутки в основном цикле

Источник: PostgreSQL processor (bank_registry)
Назначение: bank_marts.dim_businesses / dim_clients / dim_services / dim_funnels

Используется в Superset для JOIN с витринами —
чтобы видеть названия бизнесов, клиентов, сервисов вместо UUID.
"""
import logging
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

from db.clickhouse import db

log = logging.getLogger(__name__)

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://bank:bank_pass@processor-postgres:5432/bank_registry"
)


def _pg_query(conn, sql: str) -> list[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(r) for r in cur.fetchall()]


def run() -> None:
    log.info("[dims] Starting dimension sync from PostgreSQL...")

    try:
        conn = psycopg2.connect(POSTGRES_DSN)
    except Exception as e:
        log.error("[dims] Cannot connect to PostgreSQL: %s", e)
        return

    try:
        synced_at = datetime.utcnow()

        # ── dim_businesses ──────────────────────────────────────
        rows = _pg_query(conn, """
            SELECT
                business_id,
                company_name,
                COALESCE(inn, '')           AS inn,
                COALESCE(industry, '')      AS industry,
                COALESCE(segment, '')       AS segment,
                COALESCE(region, '')        AS region,
                COALESCE(tax_regime, '')    AS tax_regime,
                COALESCE(current_tariff, '') AS current_tariff,
                CASE WHEN is_active THEN 1 ELSE 0 END AS is_active
            FROM business_clients
        """)
        if rows:
            for r in rows:
                r["business_id"] = str(r["business_id"])
                r["synced_at"] = synced_at
            db.insert("dim_businesses", rows)
            log.info("[dims] dim_businesses: synced %d rows", len(rows))

        # ── dim_clients ─────────────────────────────────────────
        rows = _pg_query(conn, """
            SELECT
                client_id,
                full_name,
                COALESCE(segment, '')           AS segment,
                COALESCE(region, '')            AS region,
                COALESCE(primary_product, '')   AS primary_product,
                COALESCE(subscription_plan, '') AS subscription_plan,
                CASE WHEN is_active THEN 1 ELSE 0 END AS is_active
            FROM clients
        """)
        if rows:
            for r in rows:
                r["client_id"] = str(r["client_id"])
                r["synced_at"] = synced_at
            db.insert("dim_clients", rows)
            log.info("[dims] dim_clients: synced %d rows", len(rows))

        # ── dim_services ────────────────────────────────────────
        rows = _pg_query(conn, """
            SELECT
                service_id,
                service_name,
                COALESCE(service_type, '') AS service_type,
                CASE WHEN is_active THEN 1 ELSE 0 END AS is_active
            FROM ecosystem_services
        """)
        if rows:
            for r in rows:
                r["service_id"] = str(r["service_id"])
                r["synced_at"] = synced_at
            db.insert("dim_services", rows)
            log.info("[dims] dim_services: synced %d rows", len(rows))

        # ── dim_funnels ─────────────────────────────────────────
        rows = _pg_query(conn, """
            SELECT
                funnel_id,
                funnel_name,
                COALESCE(service_id::text, '00000000-0000-0000-0000-000000000000') AS service_id,
                COALESCE(target_event, '')              AS target_event,
                COALESCE(benchmark_duration_sec, 0)     AS benchmark_duration_sec,
                CASE WHEN is_active THEN 1 ELSE 0 END   AS is_active
            FROM funnel_definitions
        """)
        if rows:
            for r in rows:
                r["funnel_id"] = str(r["funnel_id"])
                r["synced_at"] = synced_at
            db.insert("dim_funnels", rows)
            log.info("[dims] dim_funnels: synced %d rows", len(rows))

    finally:
        conn.close()

    log.info("[dims] Dimension sync complete.")
