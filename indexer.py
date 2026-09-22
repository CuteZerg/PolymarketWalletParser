import asyncio

import asyncpg
from web3 import AsyncWeb3

from config import CHUNK_SIZE, PADDED_WALLET_ADDRESS, RATE_LIMIT_DELAY, START_BLOCK
from db import get_last_indexed_block, save_logs, update_last_indexed_block
from rpc import fetch_logs_chunk


async def run_indexer(w3: AsyncWeb3, pool: asyncpg.Pool, end_block: int):
    await asyncio.gather(
        index_topic_slot(w3, pool, slot=1, end_block=end_block),
        index_topic_slot(w3, pool, slot=2, end_block=end_block),
        index_topic_slot(w3, pool, slot=3, end_block=end_block),
    )


async def index_topic_slot(w3: AsyncWeb3, pool: asyncpg.Pool, slot: int, end_block: int):
    source_name = f"topic{slot}"
    current_block = await get_last_indexed_block(pool, source_name, START_BLOCK)

    topics = [None, None, None, None]
    topics[slot] = PADDED_WALLET_ADDRESS
    while topics and topics[-1] is None:
        topics.pop()
    while current_block < end_block:
        to_block = min(current_block + CHUNK_SIZE, end_block)
        logs = await   fetch_logs_chunk(w3, current_block, to_block, topics)
        async with pool.acquire() as conn, conn.transaction():      
            await save_logs(conn, logs)
            await update_last_indexed_block(conn, source_name, to_block)

        progress = (to_block - START_BLOCK) / (end_block - START_BLOCK) * 100
        print(f"[{source_name}] Блок {to_block} / {end_block} ({progress:.1f}%)")
        current_block = to_block + 1
        await asyncio.sleep(RATE_LIMIT_DELAY)