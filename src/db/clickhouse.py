"""
Два клиента ClickHouse:
  source_client — читаем bank_raw (processor)
  mart_client   — пишем bank_marts (mart)

Оба с retry при потере соединения.
"""
import logging
import time
from typing import Any

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from config import config

log = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 3


def _make_client(host, port, user, password, db) -> Client:
    return Client(
        host=host, port=port,
        user=user, password=password,
        database=db,
        connect_timeout=10,
        send_receive_timeout=60,
    )


class ClickHouseGateway:
    """
    Обёртка над двумя клиентами с retry-логикой.
    Используется всеми job'ами и детекторами.
    """

    def __init__(self) -> None:
        self._source: Client | None = None
        self._mart: Client | None = None

    def _get_source(self) -> Client:
        if self._source is None:
            self._source = _make_client(
                config.source_host, config.source_port,
                config.source_user, config.source_password,
                config.source_db,
            )
        return self._source

    def _get_mart(self) -> Client:
        if self._mart is None:
            self._mart = _make_client(
                config.mart_host, config.mart_port,
                config.mart_user, config.mart_password,
                config.mart_db,
            )
        return self._mart

    def query(self, sql: str, params: dict = None) -> list[dict]:
        """SELECT из bank_raw."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                client = self._get_source()
                rows = client.execute(sql, params or {}, with_column_types=True)
                data, columns = rows
                col_names = [c[0] for c in columns]
                return [dict(zip(col_names, row)) for row in data]
            except ClickHouseError as e:
                log.warning("Source query error (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
                self._source = None
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY * attempt)

    def insert(self, table: str, rows: list[dict]) -> None:
        """INSERT в bank_marts."""
        if not rows:
            return
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                client = self._get_mart()
                client.execute(f"INSERT INTO {table} VALUES", rows)
                log.debug("Inserted %d rows into %s", len(rows), table)
                return
            except ClickHouseError as e:
                log.warning("Mart insert error (attempt %d/%d) table=%s: %s",
                            attempt, MAX_RETRIES, table, e)
                self._mart = None
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY * attempt)

    def mart_query(self, sql: str, params: dict = None) -> list[dict]:
        """SELECT из bank_marts (для детекторов которые читают витрины)."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                client = self._get_mart()
                rows = client.execute(sql, params or {}, with_column_types=True)
                data, columns = rows
                col_names = [c[0] for c in columns]
                return [dict(zip(col_names, row)) for row in data]
            except ClickHouseError as e:
                log.warning("Mart query error (attempt %d/%d): %s", attempt, MAX_RETRIES, e)
                self._mart = None
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY * attempt)


# Синглтон — один на весь процесс
db = ClickHouseGateway()
