import aiohttp
from web3 import AsyncWeb3
from web3.middleware import ExtraDataToPOAMiddleware

ERC20_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]
 
ERC1155_BALANCE_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

ERC1155_BALANCE_OF_BATCH_ABI = [
    {
        "constant": True,
        "inputs": [
            {"name": "accounts", "type": "address[]"},
            {"name": "ids", "type": "uint256[]"},
        ],
        "name": "balanceOfBatch",
        "outputs": [{"name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def create_async_web3(rpc_url: str) -> AsyncWeb3:
    #w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url))
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url, request_kwargs={"timeout": aiohttp.ClientTimeout(total=30)}))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3


async def fetch_logs_chunk(w3: AsyncWeb3, from_block: int, to_block: int, topics: list) -> list:

    filter_params = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "topics": topics,
    }

    return await w3.eth.get_logs(filter_params)


async def get_latest_block_number(w3: AsyncWeb3) -> int:
    return await w3.eth.get_block_number()


async def check_balance(
    w3: AsyncWeb3,
    contract_address: str,
    wallet: str,
    block: int,
    token_id: int | None = None,
) -> int:
    checksum_contract = w3.to_checksum_address(contract_address)
    checksum_wallet = w3.to_checksum_address(wallet)
 
    if token_id is None:
        contract = w3.eth.contract(address=checksum_contract, abi=ERC20_BALANCE_ABI)
        return await contract.functions.balanceOf(checksum_wallet).call(block_identifier=block)
    else:
        contract = w3.eth.contract(address=checksum_contract, abi=ERC1155_BALANCE_ABI)
        return await contract.functions.balanceOf(checksum_wallet, token_id).call(block_identifier=block)


async def check_balance_batch(
    w3: AsyncWeb3,
    contract_address: str,
    wallet: str,
    block: int,
    token_ids: list[int],
) -> list[int]:
    checksum_contract = w3.to_checksum_address(contract_address)
    checksum_wallet = w3.to_checksum_address(wallet)
    accounts = [checksum_wallet] * len(token_ids)
    
    contract = w3.eth.contract(address=checksum_contract, abi=ERC1155_BALANCE_OF_BATCH_ABI)
    return await contract.functions.balanceOfBatch(accounts, token_ids).call(block_identifier=block)