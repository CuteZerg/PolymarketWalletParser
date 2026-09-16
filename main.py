import os

import requests
from dotenv import load_dotenv
from web3 import Web3


def main():
    load_dotenv()

    USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174" 
    WALLET_ADDRESS = os.getenv("WALLET_ADDRESS") 

    w3 = Web3(Web3.HTTPProvider(os.getenv("POLYGON_RPC_URL")))
    wallet_address = Web3.to_checksum_address(WALLET_ADDRESS)
    usdc_address = Web3.to_checksum_address(USDC_ADDRESS)

    transfer_topic_0 = '0x' + w3.keccak(text="Transfer(address,address,uint256)").hex()
    padded_wallet = '0x' + wallet_address[2:].zfill(64)

    from_block = 80813686 # starting block number for USDC transfers on Polygon
    latest_block = w3.eth.block_number
    chunk_size = 9
    to_block = min(from_block + chunk_size, latest_block)

    try:
        logs_in = w3.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': to_block,
            'address': usdc_address,
            'topics': [transfer_topic_0, None, padded_wallet] # None = any sender
        })
        
        logs_out = w3.eth.get_logs({
            'fromBlock': from_block,
            'toBlock': to_block,
            'address': usdc_address,
            'topics': [transfer_topic_0, padded_wallet, None]
        })

        print(logs_in)
        print(logs_out)

    except requests.exceptions.HTTPError as e:
        print("Alchemy answered with an error:", e.response.text)
        raise e 
   


if __name__ == "__main__":
    main()
