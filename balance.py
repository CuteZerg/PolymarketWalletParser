import asyncio
import logging
from collections import defaultdict

import asyncpg
from web3 import AsyncWeb3, Web3

from config import CURSOR_PREFETCH, MAX_CONCURRENT_REQUESTS
from rpc import check_balance, check_balance_batch

logger = logging.getLogger(__name__)


TRANSFER_TOPIC0_HASH = Web3.keccak(text="Transfer(address,address,uint256)")
TRANSFER_TOPIC0_HASH_SINGLE = Web3.keccak(text="TransferSingle(address,address,address,uint256,uint256)")
TRANSFER_TOPIC0_HASH_BATCH = Web3.keccak(text="TransferBatch(address,address,address,uint256[],uint256[])")


def _pad_address(address: str) -> bytes:
    return bytes(12) + bytes.fromhex(address[2:])


def _uint256(data: bytes, offset: int = 0) -> int:
    return int.from_bytes(data[offset : offset + 32], "big")


def _decode_batch_data(data: bytes) -> list[tuple[int, int]]:
    """
    Декодирует ABI-encoded (uint256[] ids, uint256[] values) из TransferBatch.
    """
    ids_offset = _uint256(data, 0)
    vals_offset = _uint256(data, 32)

    n = _uint256(data, ids_offset)

    return [
        (_uint256(data, ids_offset + 32 + i * 32), _uint256(data, vals_offset + 32 + i * 32))
        for i in range(n)
    ]


async def calculate_balances(
    pool: asyncpg.Pool,
    wallet_address: str,
) -> dict[tuple[bytes, int | None], int]:
    """
    Считает балансы токенов по событиям в raw_logs.

    Возвращает: {(contract_address_bytes, token_id_or_None): balance}
      - token_id = None для ERC-20 / ERC-721
      - token_id = int для ERC-1155
    """
    padded_wallet = _pad_address(wallet_address)
    balances: dict[tuple[bytes, int | None], int] = defaultdict(int)
 
    async with pool.acquire() as conn:
        # ERC-20 и ERC-721 используют одинаковую Transfer сигнатуру
        # У ERC-721 третий  параметр — tokenId (лежит в topic3) а data пустой
        # У ERC-20 data содержит uint256 value
 
        # async with conn.transaction():
        #     async for row in conn.cursor(
        #         "SELECT contract_address, topic3, data FROM raw_logs WHERE topic0 = $1 AND topic2 = $2",
        #         TRANSFER_TOPIC0_HASH, padded_wallet,
        #         prefetch=CURSOR_PREFETCH,
        #     ):
        #         contract = bytes(row["contract_address"])
        #         data = bytes(row["data"]) if row["data"] else b""
        #         if row["topic3"] is not None and len(data) == 0:
        #             value = 1  # ERC-721: один NFT
        #         else:
        #             value = _uint256(data)
        #         balances[(contract, None)] += value

        async with conn.transaction():
            async for row in conn.cursor(
                "SELECT contract_address, topic3, data FROM raw_logs WHERE topic0 = $1 AND topic2 = $2",
                TRANSFER_TOPIC0_HASH, padded_wallet,
                prefetch=CURSOR_PREFETCH,
            ):
                contract = bytes(row["contract_address"])
                data = bytes(row["data"]) if row["data"] else b""
                if row["topic3"] is not None and len(data) == 0:
                    # ERC-721: tokenId лежит в topic3, считаем по (contract, tokenId)
                    token_id = _uint256(bytes(row["topic3"]))
                    balances[(contract, token_id)] += 1
                else:
                    # ERC-20: value лежит в data
                    value = _uint256(data)
                    balances[(contract, None)] += value
 
        async with conn.transaction():
            async for row in conn.cursor(
                "SELECT contract_address, topic3, data FROM raw_logs WHERE topic0 = $1 AND topic1 = $2",
                TRANSFER_TOPIC0_HASH, padded_wallet,
                prefetch=CURSOR_PREFETCH,
            ):
                contract = bytes(row["contract_address"])
                data = bytes(row["data"]) if row["data"] else b""
                if row["topic3"] is not None and len(data) == 0:
                    token_id = _uint256(bytes(row["topic3"]))
                    balances[(contract, token_id)] -= 1
                else:
                    value = _uint256(data)
                    balances[(contract, None)] -= value
 
        # ERC-1155 TransferSingle: id и value закодированы в data
 
        async with conn.transaction():
            async for row in conn.cursor(
                "SELECT contract_address, data FROM raw_logs WHERE topic0 = $1 AND topic3 = $2",
                TRANSFER_TOPIC0_HASH_SINGLE, padded_wallet,
                prefetch=CURSOR_PREFETCH,
            ):
                data = bytes(row["data"])
                balances[(bytes(row["contract_address"]), _uint256(data, 0))] += _uint256(data, 32)
 
        async with conn.transaction():
            async for row in conn.cursor(
                "SELECT contract_address, data FROM raw_logs WHERE topic0 = $1 AND topic2 = $2",
                TRANSFER_TOPIC0_HASH_SINGLE, padded_wallet,
                prefetch=CURSOR_PREFETCH,
            ):
                data = bytes(row["data"])
                balances[(bytes(row["contract_address"]), _uint256(data, 0))] -= _uint256(data, 32)
 
        # ERC-1155 TransferBatch
 
        async with conn.transaction():
            async for row in conn.cursor(
                "SELECT contract_address, data FROM raw_logs WHERE topic0 = $1 AND topic3 = $2",
                TRANSFER_TOPIC0_HASH_BATCH, padded_wallet,
                prefetch=CURSOR_PREFETCH,
            ):
                contract = bytes(row["contract_address"])
                for token_id, value in _decode_batch_data(bytes(row["data"])):
                    balances[(contract, token_id)] += value
 
        async with conn.transaction():
            async for row in conn.cursor(
                "SELECT contract_address, data FROM raw_logs WHERE topic0 = $1 AND topic2 = $2",
                TRANSFER_TOPIC0_HASH_BATCH, padded_wallet,
                prefetch=CURSOR_PREFETCH,
            ):
                contract = bytes(row["contract_address"])
                for token_id, value in _decode_batch_data(bytes(row["data"])):
                    balances[(contract, token_id)] -= value
 
    return dict(balances)


async def _verify_single(
    w3: AsyncWeb3,
    semaphore: asyncio.Semaphore,
    wallet_address: str,
    contract_bytes: bytes,
    token_id: int | None,
    expected: int,
    block: int,
) -> dict | None:
    """
    Проверяет один токен, сравнивая с чейн balanceOf.
 
    Возвращает словарь с расхождением, если баланс не совпал или сам
    RPC-вызов упал с ошибкой, иначе None.
    """
    contract_hex = "0x" + contract_bytes.hex()
 
    async with semaphore:
        try:
            on_chain = await check_balance(w3, contract_hex, wallet_address, block, token_id)
        except Exception as e:
            return {
                "contract": contract_hex,
                "token_id": token_id,
                "calculated": expected,
                "on_chain": f"ERROR: {e}",
                "diff": None,
            }
 
    if expected != on_chain:
        return {
            "contract": contract_hex,
            "token_id": token_id,
            "calculated": expected,
            "on_chain": on_chain,
            "diff": expected - on_chain,
        }
 
    return None


async def _verify_batch(
    w3: AsyncWeb3,
    semaphore: asyncio.Semaphore,
    wallet_address: str,
    contract_bytes: bytes,
    token_ids: list[int],
    expected_balances: list[int],
    block: int,
) -> list[dict]:
    contract_hex = "0x" + contract_bytes.hex()
    
    async with semaphore:
        try:
            on_chain_balances = await check_balance_batch(w3, contract_hex, wallet_address, block, token_ids)
        except Exception as e:
            return [{
                "contract": contract_hex,
                "token_id": tid,
                "calculated": exp,
                "on_chain": f"ERROR: {e}",
                "diff": None,
            } for tid, exp in zip(token_ids, expected_balances)]
            
    mismatches = []
    for tid, exp, on_chain in zip(token_ids, expected_balances, on_chain_balances):
        if exp != on_chain:
            mismatches.append({
                "contract": contract_hex,
                "token_id": tid,
                "calculated": exp,
                "on_chain": on_chain,
                "diff": exp - on_chain,
            })
            
    return mismatches


async def verify_balances(
    w3: AsyncWeb3,
    balances: dict[tuple[bytes, int | None], int],
    wallet_address: str,
    block: int,
) -> list[dict]:
    """
    Сравнивает рассчитанные балансы с чейн balanceOf на заданном блоке.
    Возвращает список расхождений.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    tasks = []
    
    # Группируем ERC-1155 по контрактам
    erc1155_by_contract = defaultdict(list)
    
    for (contract_bytes, token_id), expected in balances.items():
        if token_id is None:
            tasks.append(_verify_single(w3, semaphore, wallet_address, contract_bytes, token_id, expected, block))
        else:
            erc1155_by_contract[contract_bytes].append((token_id, expected))
            
    for contract_bytes, items in erc1155_by_contract.items():
        # Разбиваем на батчи по 200
        BATCH_SIZE = 200
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i:i + BATCH_SIZE]
            token_ids = [item[0] for item in batch]
            expected_balances = [item[1] for item in batch]
            tasks.append(_verify_batch(w3, semaphore, wallet_address, contract_bytes, token_ids, expected_balances, block))
            
    results = await asyncio.gather(*tasks)
    
    mismatches = []
    for r in results:
        if isinstance(r, list):
            mismatches.extend(r)
        elif r is not None:
            mismatches.append(r)
            
    matched = len(balances) - len(mismatches)
    logger.info("Verified %d balances: %d matched, %d mismatched", len(balances), matched, len(mismatches))
    
    return mismatches