import asyncio

from config import DB_URL, RPC_URL
from db import init_db
from indexer import run_indexer
from rpc import create_async_web3


async def main():
    # 1.Инициализация пула соединений с базой данных
    pool = await init_db(DB_URL)
    # 2.Создание экземпляра AsyncWeb3
    w3 = create_async_web3(RPC_URL)
    # 3.Получение номера последнего блока в сети
    end_block = await w3.eth.get_block_number()
    # 4.Запуск индексера
    await run_indexer(w3, pool, end_block)

if __name__ == "__main__":
    asyncio.run(main())
