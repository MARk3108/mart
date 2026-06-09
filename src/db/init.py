"""
Автоматическое создание БД и таблиц при старте mart.
Идемпотентно — CREATE DATABASE/TABLE IF NOT EXISTS.
"""
import logging
import time
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from config import config

log = logging.getLogger(__name__)

DDL = """
CREATE DATABASE IF NOT EXISTS bank_marts;

CREATE TABLE IF NOT EXISTS bank_marts.daily_turnover
(
    date                  Date,
    business_id           UUID,
    inflow_sum            Decimal(18,2),
    outflow_sum           Decimal(18,2),
    inflow_count          UInt32,
    outflow_count         UInt32,
    tx_count              UInt32,
    avg_tx_amount         Decimal(18,2),
    unique_counterparties UInt32,
    active_day            UInt8,
    cash_withdrawal_sum   Decimal(18,2),
    balance_avg           Decimal(18,2),
    balance_min           Decimal(18,2),
    balance_volatility    Decimal(18,2)
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (business_id, date);

CREATE TABLE IF NOT EXISTS bank_marts.monthly_turnover
(
    month                 Date,
    business_id           UUID,
    inflow_sum            Decimal(18,2),
    outflow_sum           Decimal(18,2),
    tx_count              UInt32,
    avg_balance           Decimal(18,2),
    unique_counterparties UInt32
) ENGINE = ReplacingMergeTree()
PARTITION BY toYear(month)
ORDER BY (business_id, month);

CREATE TABLE IF NOT EXISTS bank_marts.business_baseline
(
    business_id      UUID,
    baseline_period  String,
    metric           String,
    mean_value       Float64,
    std_deviation    Float64,
    p25              Float64,
    p75              Float64,
    calculated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (business_id, metric, baseline_period);

CREATE TABLE IF NOT EXISTS bank_marts.daily_service_usage
(
    date          Date,
    client_id     UUID,
    service_id    UUID,
    session_count UInt32,
    event_count   UInt32,
    tx_sum        Float64,
    tx_count      UInt32,
    cancel_count  UInt32
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (client_id, service_id, date);

CREATE TABLE IF NOT EXISTS bank_marts.monthly_service_usage
(
    month                Date,
    client_id            UUID,
    service_id           UUID,
    active_days          UInt32,
    session_count        UInt32,
    tx_sum               Float64,
    unique_services_used UInt32
) ENGINE = ReplacingMergeTree()
PARTITION BY toYear(month)
ORDER BY (client_id, service_id, month);

CREATE TABLE IF NOT EXISTS bank_marts.client_service_baseline
(
    client_id       UUID,
    baseline_period String,
    metric          String,
    mean_value      Float64,
    std_deviation   Float64,
    p25             Float64,
    p75             Float64,
    calculated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (client_id, metric, baseline_period);

CREATE TABLE IF NOT EXISTS bank_marts.daily_friction_stats
(
    date                      Date,
    client_id                 UUID,
    funnel_id                 UUID,
    friction_event_count      UInt32,
    rage_click_count          UInt32,
    idle_count                UInt32,
    ui_error_count            UInt32,
    exit_without_action_count UInt32,
    session_count             UInt32,
    completed_session_count   UInt32,
    funnel_success_rate       Float64,
    avg_task_duration_sec     Float64,
    ux_tickets_count          UInt32,
    is_active_day             UInt8
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (client_id, date);

CREATE TABLE IF NOT EXISTS bank_marts.client_friction_baseline
(
    client_id       UUID,
    baseline_period String,
    metric          String,
    mean_value      Float64,
    std_deviation   Float64,
    p25             Float64,
    p75             Float64,
    calculated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (client_id, metric, baseline_period);

CREATE TABLE IF NOT EXISTS bank_marts.anomaly_alerts
(
    alert_id        UUID DEFAULT generateUUIDv4(),
    detected_at     DateTime DEFAULT now(),
    anomaly_type    LowCardinality(String),
    entity_type     LowCardinality(String),
    entity_id       UUID,
    metric_name     String,
    metric_value    Float64,
    baseline_mean   Float64,
    baseline_std    Float64,
    deviation_sigma Float64,
    severity        LowCardinality(String),
    details         String,
    is_resolved     UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, anomaly_type, entity_id);

CREATE TABLE IF NOT EXISTS bank_marts.dim_businesses
(
    business_id   UUID,
    company_name  String,
    inn           String,
    industry      String,
    segment       String,
    region        String,
    tax_regime    String,
    current_tariff String,
    is_active     UInt8,
    synced_at     DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(synced_at)
ORDER BY business_id;

CREATE TABLE IF NOT EXISTS bank_marts.dim_clients
(
    client_id         UUID,
    full_name         String,
    segment           String,
    region            String,
    primary_product   String,
    subscription_plan String,
    is_active         UInt8,
    synced_at         DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(synced_at)
ORDER BY client_id;

CREATE TABLE IF NOT EXISTS bank_marts.dim_services
(
    service_id   UUID,
    service_name String,
    service_type String,
    is_active    UInt8,
    synced_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(synced_at)
ORDER BY service_id;

CREATE TABLE IF NOT EXISTS bank_marts.dim_funnels
(
    funnel_id              UUID,
    funnel_name            String,
    service_id             UUID,
    target_event           String,
    benchmark_duration_sec UInt32,
    is_active              UInt8,
    synced_at              DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(synced_at)
ORDER BY funnel_id;
"""


def ensure_schema(retries: int = 10, delay: int = 5) -> None:
    log.info("Ensuring ClickHouse schema (bank_marts)...")

    for attempt in range(1, retries + 1):
        try:
            client = Client(
                host=config.mart_host,
                port=config.mart_port,
                user=config.mart_user,
                password=config.mart_password,
                connect_timeout=10,
            )
            for statement in DDL.strip().split(";"):
                statement = statement.strip()

                if not statement:
                    continue

                try:
                    log.info("Executing:\n%s", statement)
                    client.execute(statement)
                except Exception as e:
                    log.error("FAILED STATEMENT:\n%s\nERROR: %s", statement, e)
                    raise

            log.info("ClickHouse schema (bank_marts) OK.")
            return

        except ClickHouseError as e:
            log.warning("Schema init failed (attempt %d/%d): %s", attempt, retries, e)
            if attempt < retries:
                time.sleep(delay)

    raise RuntimeError("Could not initialize ClickHouse schema after %d attempts" % retries)