"""
Mart Service — точка входа.

Цикл каждые MART_SCHEDULE_MINUTES минут:
  1. Jobs    — агрегируем витрины из bank_raw → bank_marts
  2. Detectors — считаем baseline, детектируем аномалии
  3. Alerts  — пишем найденные аномалии в anomaly_alerts
"""
import logging
import signal
import time
from datetime import datetime

from config import config
from db.clickhouse import db
from db.init import ensure_schema
from seed.seed import run_seed
from jobs import turnover, service_usage, friction
from detectors import turnover as det_turnover
from detectors import service_usage as det_service_usage
from detectors import friction as det_friction

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("mart")

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, stopping...")
    _shutdown = True


def run_cycle() -> None:
    started = datetime.utcnow()
    log.info("=== Mart cycle started at %s ===", started.isoformat())

    # ── 1. Агрегация витрин ─────────────────────────────────────
    for job, name in [
        (turnover.run,      "turnover"),
        (service_usage.run, "service_usage"),
        (friction.run,      "friction"),
    ]:
        try:
            job()
        except Exception as e:
            log.error("Job [%s] failed: %s", name, e, exc_info=True)

    # ── 2. Детекция аномалий ────────────────────────────────────
    all_alerts = []
    for detector, name in [
        (det_turnover.run,       "turnover"),
        (det_service_usage.run,  "service_usage"),
        (det_friction.run,       "friction"),
    ]:
        try:
            alerts = detector()
            all_alerts.extend(alerts)
        except Exception as e:
            log.error("Detector [%s] failed: %s", name, e, exc_info=True)

    # ── 3. Запись алертов ───────────────────────────────────────
    if all_alerts:
        try:
            db.insert("anomaly_alerts", all_alerts)
            log.info("Wrote %d anomaly alerts.", len(all_alerts))

            by_type: dict[str, int] = {}
            by_severity: dict[str, int] = {}
            for a in all_alerts:
                by_type[a["anomaly_type"]] = by_type.get(a["anomaly_type"], 0) + 1
                by_severity[a["severity"]] = by_severity.get(a["severity"], 0) + 1
            log.info("  by type:     %s", by_type)
            log.info("  by severity: %s", by_severity)
        except Exception as e:
            log.error("Failed to write alerts: %s", e, exc_info=True)
    else:
        log.info("No anomalies detected this cycle.")

    elapsed = (datetime.utcnow() - started).total_seconds()
    log.info("=== Mart cycle finished in %.1fs ===", elapsed)


def wait_for_clickhouse(retries: int = 20, delay: int = 5) -> None:
    log.info("Waiting for ClickHouse connections...")
    for attempt in range(1, retries + 1):
        try:
            db.query("SELECT 1")
            db.mart_query("SELECT 1")
            log.info("ClickHouse connections OK.")
            return
        except Exception as e:
            log.warning("ClickHouse not ready (attempt %d/%d): %s", attempt, retries, e)
            time.sleep(delay)
    raise RuntimeError("Could not connect to ClickHouse after %d attempts" % retries)


def main() -> None:
    global _shutdown

    log.info("=== Mart Service starting ===")
    log.info("Schedule: every %d minutes", config.schedule_minutes)
    log.info("Baseline window: %d days, anomaly threshold: %.1fσ",
             config.baseline_short_days, config.anomaly_sigma)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    ensure_schema()
    run_seed()
    wait_for_clickhouse()

    interval_sec = config.schedule_minutes * 60

    while not _shutdown:
        try:
            run_cycle()
        except Exception as e:
            log.error("Mart cycle crashed: %s", e, exc_info=True)

        for _ in range(interval_sec):
            if _shutdown:
                break
            time.sleep(1)

    log.info("=== Mart Service stopped ===")


if __name__ == "__main__":
    main()