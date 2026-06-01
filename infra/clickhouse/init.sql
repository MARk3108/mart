-- ============================================================
-- ВИТРИНЫ (ClickHouse / mart) — база bank_marts
-- ============================================================

CREATE DATABASE IF NOT EXISTS bank_marts;

-- ─── Аномалия 1: Обороты МСБ ───────────────────────────────

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
    baseline_period  String,       -- 30d / 90d
    metric           String,       -- inflow_sum / outflow_sum / tx_count
    mean_value       Float64,
    std_deviation    Float64,
    p25              Float64,
    p75              Float64,
    calculated_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (business_id, metric, baseline_period);

-- ─── Аномалия 2: Использование сервисов ────────────────────

CREATE TABLE IF NOT EXISTS bank_marts.daily_service_usage
(
    date          Date,
    client_id     UUID,
    service_id    UUID,
    session_count UInt32,
    event_count   UInt32,
    tx_sum        Decimal(18,2),
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
    tx_sum               Decimal(18,2),
    unique_services_used UInt32
) ENGINE = ReplacingMergeTree()
PARTITION BY toYear(month)
ORDER BY (client_id, service_id, month);

CREATE TABLE IF NOT EXISTS bank_marts.client_service_baseline
(
    client_id       UUID,
    baseline_period String,
    metric          String,   -- session_count / tx_sum / unique_services_used
    mean_value      Float64,
    std_deviation   Float64,
    p25             Float64,
    p75             Float64,
    calculated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (client_id, metric, baseline_period);

-- ─── Аномалия 3: UX-затруднения ────────────────────────────

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
    metric          String,   -- friction_event_count / funnel_success_rate / avg_task_duration_sec
    mean_value      Float64,
    std_deviation   Float64,
    p25             Float64,
    p75             Float64,
    calculated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(calculated_at)
ORDER BY (client_id, metric, baseline_period);

-- ─── Витрина алертов ───────────────────────────────────────

CREATE TABLE IF NOT EXISTS bank_marts.anomaly_alerts
(
    alert_id        UUID DEFAULT generateUUIDv4(),
    detected_at     DateTime DEFAULT now(),
    anomaly_type    LowCardinality(String), -- turnover_anomaly / service_usage_drop / ux_friction_spike
    entity_type     LowCardinality(String), -- business / client
    entity_id       UUID,
    metric_name     String,
    metric_value    Float64,
    baseline_mean   Float64,
    baseline_std    Float64,
    deviation_sigma Float64,
    severity        LowCardinality(String), -- low / medium / high / critical
    details         String,
    is_resolved     UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(detected_at)
ORDER BY (detected_at, anomaly_type, entity_id);
