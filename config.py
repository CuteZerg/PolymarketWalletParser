import os

from dotenv import load_dotenv
from web3 import Web3

load_dotenv()
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS") 
RPC_URL = os.getenv("POLYGON_RPC_URL")
DB_URL = os.getenv("DATABASE_URL")

PADDED_WALLET_ADDRESS = '0x' + WALLET_ADDRESS[2:].zfill(64)

TRANSFER_TOPIC0 = '0x' + Web3.keccak(text="Transfer(address,address,uint256)").hex()                                # For ERC20 and ERC721
TRANSFER_SINGLE_TOPIC0 = '0x' + Web3.keccak(text="TransferSingle(address,address,address,uint256,uint256)").hex()   # For ERC1155 (single transfers)
TRANSFER_BATCH_TOPIC0 = '0x' + Web3.keccak(text="TransferBatch(address,address,address,uint256[],uint256[])").hex() # For ERC1155 (batch transfers)

START_BLOCK = 80813682 - 5 # Starting block for indexing, subtracting 5 to ensure we capture any missed events due to reorgs or delays.
CHUNK_SIZE = 2000
MAX_CONCURRENT_REQUESTS = 5
RATE_LIMIT_DELAY = 0.1