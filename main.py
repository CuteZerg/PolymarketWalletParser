import asyncio
import logging
import sys
 
from balance import calculate_balances, verify_balances
from config import DB_URL, RPC_URL, WALLET_ADDRESS
from db import init_db
from indexer import run_indexer
from rpc import create_async_web3
 
logger = logging.getLogger(__name__)
 
# последние несколько блоков сети могут ещё быть переписаны реорганизацией, 
# поэтому индексируем не до самого latest, а с отступом
REORG_SAFETY_MARGIN = 150
 
 
def print_report(mismatches: list[dict], end_block: int) -> None:
    if not mismatches:
        print(f"\nВсе балансы совпадают с on-chain значениями на блоке {end_block}.")
        return
 
    print(f"\nНайдено расхождений: {len(mismatches)} (блок {end_block})")
    for m in mismatches:
        token_id = m["token_id"]
        token_label = f", token_id={token_id}" if token_id is not None else ""
        if isinstance(m["on_chain"], str):
            # RPC-вызов упал с ошибкой — diff в этом случае недоступен
            print(f" {m['contract']}{token_label}: расчёт={m['calculated']}, on-chain={m['on_chain']}")
        else:
            print(
                f"{m['contract']}{token_label}: расчёт={m['calculated']}, "
                f"on-chain={m['on_chain']}, разница={m['diff']}"
            )
 
 
async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
 
 
    # 1. Инициализация пула соединений с базой данных
    pool = await init_db(DB_URL)
 
    try:
        # 2. Создание экземпляра AsyncWeb3
        w3 = create_async_web3(RPC_URL)
 
        # 3. Получение номера блока, до которого индексируем и на котором верифицируем
        latest_block = await w3.eth.get_block_number()
        end_block = latest_block - REORG_SAFETY_MARGIN
 
        logger.info("Индексация и верификация кошелька %s до блока %d", WALLET_ADDRESS, end_block)
 
        # 4. Запуск индексера
        await run_indexer(w3, pool, end_block)
 
        # 5. Подсчёт балансов из БД
        balances = await calculate_balances(pool, WALLET_ADDRESS)
        logger.info("Найдено позиций для верификации: %d", len(balances))
 
        # 6. Верификация балансов против on-chain balanceOf на том же end_block
        mismatches = await verify_balances(w3, balances, WALLET_ADDRESS, end_block)
 
        # 7. Вывод результата
        print_report(mismatches, end_block)
 
        return 1 if mismatches else 0
    finally:
        # Всегда закрываем пул, даже если индексация/верификация упала с исключением
        await pool.close()
 
 
if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)