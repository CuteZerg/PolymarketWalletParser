"""
Модуль для работы с PostgreSQL (asyncpg).

Отвечает за:
- Создание схемы базы данных (таблицы + индексы)
- Чтение/обновление контрольных точек индексатора 
- Массовую вставку сырых логов из блокчейна с дедупликацией

Все функции, которые ПИШУТ данные (save_logs, update_last_indexed_block),
принимают connection, а не pool — это позволяет вызывающему коду
обернуть их в единую транзакцию:

    async with pool.acquire() as conn:
        async with conn.transaction():
            await save_logs(conn, logs)
            await update_last_indexed_block(conn, source, block)

Если скрипт упадёт между этими вызовами — транзакция откатится,
и при перезапуске индексатор повторит этот чанк заново.
"""

import asyncpg

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS indexer_state (
    source_name   TEXT PRIMARY KEY,
    last_block    BIGINT NOT NULL,
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_logs (
    tx_hash           BYTEA NOT NULL,
    log_index         INTEGER NOT NULL,
    block_number      BIGINT NOT NULL,
    block_timestamp   BIGINT,
    contract_address  BYTEA NOT NULL,
    topic0            BYTEA,
    topic1            BYTEA,
    topic2            BYTEA,
    topic3            BYTEA,
    data              BYTEA,
    PRIMARY KEY (tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_raw_logs_topic0
    ON raw_logs (topic0);
CREATE INDEX IF NOT EXISTS idx_raw_logs_contract
    ON raw_logs (contract_address);
CREATE INDEX IF NOT EXISTS idx_raw_logs_block
    ON raw_logs (block_number);
"""


async def init_db(database_url: str) -> asyncpg.Pool:
    """
    Создаёт пул соединений к PostgreSQL и инициализирует схему.
    """
    pool = await asyncpg.create_pool(database_url)

    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)

    return pool


async def get_last_indexed_block(
    pool: asyncpg.Pool,
    source_name: str,
    default_block: int,
) -> int:
    """
    Возвращает номер последнего обработанного блока для потока.
    Если записи нет (первый запуск) — возвращает default_block.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_block FROM indexer_state WHERE source_name = $1",
            source_name,
        )

    if row is None:
        return default_block

    return row["last_block"]


async def update_last_indexed_block(
    conn: asyncpg.Connection,
    source_name: str,
    block_number: int,
) -> None:
    """
    Обновляет контрольную точку для указанного потока.
    """
    await conn.execute(
        """
        INSERT INTO indexer_state (source_name, last_block, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (source_name)
        DO UPDATE SET last_block = $2, updated_at = NOW()
        """,
        source_name,
        block_number,
    )


def _parse_log(log: dict) -> tuple:
    """
    Конвертирует один лог из формата web3.py в кортеж для вставки в БД.
    """

    tx_hash = bytes(log["transactionHash"])

    log_index = log["logIndex"]
    block_number = log["blockNumber"]

    # blockTimestamp: может быть hex-строкой, int, или отсутствовать
    block_timestamp = None
    raw_ts = log.get("blockTimestamp")
    if raw_ts is not None:
        if isinstance(raw_ts, str) and raw_ts.startswith("0x"):
            block_timestamp = int(raw_ts, 16)
        elif isinstance(raw_ts, int):
            block_timestamp = raw_ts


    contract_address = bytes.fromhex(log["address"][2:])

    topics = log.get("topics", [])
    topic0 = bytes(topics[0]) if len(topics) > 0 else None
    topic1 = bytes(topics[1]) if len(topics) > 1 else None
    topic2 = bytes(topics[2]) if len(topics) > 2 else None
    topic3 = bytes(topics[3]) if len(topics) > 3 else None

    raw_data = log.get("data")
    data = bytes(raw_data) if raw_data is not None else None

    return (
        tx_hash,
        log_index,
        block_number,
        block_timestamp,
        contract_address,
        topic0,
        topic1,
        topic2,
        topic3,
        data,
    )


async def save_logs(conn: asyncpg.Connection, logs: list) -> int:
    """
    Массово вставляет сырые логи в таблицу raw_logs.

    Returns:
        Количество обработанных логов (верхняя граница;
        фактически вставленных может быть меньше из-за дедупликации).
    """
    if not logs:
        return 0

    records = [_parse_log(log) for log in logs]

    await conn.executemany(
        """
        INSERT INTO raw_logs (
            tx_hash, log_index, block_number, block_timestamp,
            contract_address, topic0, topic1, topic2, topic3, data
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (tx_hash, log_index) DO NOTHING
        """,
        records,
    )

    return len(records)