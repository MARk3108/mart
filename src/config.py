import os
from dataclasses import dataclass


@dataclass
class Config:
    # Источник — processor ClickHouse (bank_raw)
    source_host:     str = os.getenv("SOURCE_CLICKHOUSE_HOST",     "processor-clickhouse")
    source_port:     int = int(os.getenv("SOURCE_CLICKHOUSE_PORT", "9000"))
    source_user:     str = os.getenv("SOURCE_CLICKHOUSE_USER",     "bank")
    source_password: str = os.getenv("SOURCE_CLICKHOUSE_PASSWORD", "bank_pass")
    source_db:       str = os.getenv("SOURCE_CLICKHOUSE_DB",       "bank_raw")

    # Назначение — mart ClickHouse (bank_marts)
    mart_host:     str = os.getenv("MART_CLICKHOUSE_HOST",     "clickhouse")
    mart_port:     int = int(os.getenv("MART_CLICKHOUSE_PORT", "9000"))
    mart_user:     str = os.getenv("MART_CLICKHOUSE_USER",     "bank")
    mart_password: str = os.getenv("MART_CLICKHOUSE_PASSWORD", "bank_pass")
    mart_db:       str = os.getenv("MART_CLICKHOUSE_DB",       "bank_marts")

    # Расписание
    schedule_minutes: int = int(os.getenv("MART_SCHEDULE_MINUTES", "5"))

    # Baseline — окна и порог аномалии
    baseline_short_days: int = 30
    baseline_long_days:  int = 90
    min_history_days:    int = 7    # меньше — не считаем baseline
    anomaly_sigma:       float = 2.0  # порог в σ


config = Config()
