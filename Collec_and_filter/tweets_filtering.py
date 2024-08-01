import json
import pandas as pd
import re
import os
import bz2
import gzip
import time
import argparse
from concurrent.futures import ProcessPoolExecutor


# root_directory_path = r'C:\Users\Manuel\Documents\TFM\twitter-stream-2023-01\2023\1\6'

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process JSON files from a directory.")
    parser.add_argument('dir_number', type=int, help="The number to append to the directory path")
    return parser.parse_args()


# Define a set of cryptocurrency-related keywords and symbols of most of the founded cryptocurrencies
crypto_keywords = {
    'crypto', 'cryptocurrency', 'cryptocurrencies', 'blockchain', 'btc', 'bitcoin', 'ethereum', 'eth',
    'altcoin', 'altcoins', 'defi', 'nft', 'nfts', 'non-fungible token', 'non-fungible tokens',
    'blockchain', 'smart contract', 'smart contracts', 'dapp', 'dapps', 'decentralized application',
    'decentralized applications', 'mining', 'crypto mining', 'staking', 'yield farming', 'stablecoin',
    'stablecoins', 'tether', 'usdt', 'usd coin', 'usdc', 'binance coin', 'bnb', 'solana', 'sol',
    'cardano', 'ada', 'ripple', 'xrp', 'litecoin', 'ltc', 'dogecoin', 'doge', 'shiba inu', 'shib',
    'polkadot', 'dot', 'chainlink', 'uniswap', 'uni', 'terra', 'luna', 'avalanche', 'avax',
    'polygon', 'matic', 'cosmos', 'atom', 'vechain', 'vet', 'crypto exchange', 'crypto wallet',
    'ledger', 'metamask', 'cold wallet', 'hot wallet', 'coinbase', 'binance', 'kraken', 'gemini',
    'robinhood', 'pancakeswap', 'sushiswap', 'aave', 'compound', 'curve finance', 'synthetix',
    'yearn finance', 'maker', 'makerdao', 'dai', 'zcash', 'zc', 'monero', 'xmr', 'privacy coin',
    'privacy coins', 'decentralized finance', 'decentralized exchanges', 'dex', 'cbdc', 'central bank digital currency',
    'initial coin offering', 'ico', 'security token offering', 'sto', 'initial exchange offering',
    'ieo', 'token', 'tokens', 'tokenization', 'proof of work', 'proof of stake', 'pow', 'pos',
    'hashrate', 'cryptographic', 'cryptography', 'cryptographic key', 'public key', 'private key',
    'digital asset', 'digital assets', 'digital currency', 'digital currencies', 'LTC', 'DASH', 'XMR',
    'ETC', 'DOGE', 'ZEC', 'BTS', 'DGB', 'XRP', 'NAV', 'XLM', 'SYS', 'VTC', 'BCN', 'SIGNA', 'MAID', 'NXS',
    'GRS', 'SC', 'VAL', 'DCR', 'REP', 'LSK', 'WAVES', 'STEEM', 'BNT', 'STRAX', 'KMD', 'NEO', 'COVAL',
    'RRT', 'FIRO', 'ARDR', 'ARK', 'GLM', 'MKR', 'PIVX', 'PLU', 'MLN', 'RLC', 'GNO', 'PZM', 'ANT', 'BAT',
    'QTUM', 'ZEN', 'MIOTA', 'SNT', 'CVC', 'DENT', 'VGX', 'XTZ', 'EOS', 'MCO', 'NMR', 'ADX', 'USDT',
    'NANO', 'SSV', 'DNT', 'KIN', 'MTL', 'PPT', 'SNC', 'OAX', 'ZRX', 'STORJ', 'OMG', 'AE', 'AGRS', 'ADS',
    'VIB', 'ATOM', 'MANA', 'BCH', 'STOX', 'BNB', 'FIL', 'VET', 'UTK', 'AMB', 'WAN', 'PST', 'ENJ', 'WTC',
    'CAPP', 'VEE', 'LINK', 'PNT', 'KNC', 'TRX', 'LRC', 'CAS', 'ADA', 'YOYOW', 'ICX', 'REQ', 'AST',
    'WAXP', 'POWR', 'MDA', 'BTG', 'RNDR', 'KCS', 'DRGN', 'OST', 'NULS', 'SRN', 'QSP', 'BCD', 'UQC',
    'AION', 'ACT', 'ADB', 'TNB', 'BNTY', 'ELF', 'DBC', 'GNX', 'REM', 'HPB', 'TEL', 'INT', 'CRPT', 'DTA',
    'OCN', 'THETA', 'MDT', 'IOST', 'TRAC', 'ZIL', 'SWFTC', 'YEE', 'AXPR', 'FAIRG', 'POLY', 'ELA', 'BLZ',
    'CHSB', 'BIX', 'ABT', 'FSN', 'HT', 'CTC', 'SHPING', 'TONE', 'REN', 'RVN', 'XYO', 'CEL', 'CRDTS',
    'ONT', 'CLO', 'SNX', 'LOOM', 'TUSD', 'MITH', 'CTXC', 'XHV', 'DOCK', 'DERO', 'KRL', 'NEXO', 'VRA',
    'TRUE', 'LBA', 'SKM', 'UBEX', 'IOTX', 'NKN', 'QKC', 'OXEN', 'CBC', 'PI', 'SEELE', 'VIDT', 'MERCU',
    'EGT', 'MET', 'CET', 'IQ', 'VITE', 'RNT', 'RPL', 'GAMB', 'NOIA', 'OMI', 'APL', 'ENQ', 'DAG', 'OKB',
    'KCASH', 'OGSP', 'EURS', 'MOF', 'COTI', 'TOPC', 'GUSD', 'HYC', 'USDC', 'ONGAS', 'USDP', 'DIVI', 'VTHO',
    'BF', 'ABBC', 'VEX', 'CIX100', 'LPT', 'BSV', 'WOM', 'NRG', 'SYLO', 'AVA', 'FTM', 'RBTC', 'AERGO',
    'RIF', 'DUSK', 'BTT', 'WBTC', 'QNT', 'ASD', 'LTO', 'PLG', 'FET', 'ANKR', 'SHA', 'CRO', 'DIO', 'VSYS',
    'ZEON', 'CELR', 'TFUEL', 'OOKI', 'ORBS', 'SOLVE', 'BOLT', 'MATIC', 'DREP', 'FX', 'IOWN', 'RSR', 'ONE',
    'CHR', 'TRIAS', 'WXT', 'KAT', 'ATP', 'TOPN', 'STPT', 'CVNT', 'IRIS', 'MXC', 'JAR', 'AMPL', 'ROOBEE',
    'GNY', 'SRK', 'ARPA', 'WIN', 'LUNC', 'FTT', 'LOCUS', 'SERO', 'EM', 'FKX', 'TSHP', 'SXP', 'GT', 'CHZ',
    'BDX', 'AKRO', 'BHP', 'PNK', 'PROM', 'ULTRA', 'NYE', 'BAND', 'HBAR', 'BUSD', 'PAXG', 'MX', 'TLOS', 'LBK',
    'STX', 'KAVA', 'CKB', 'DAI', 'NODE', 'VLX', 'RUNE', 'DILI', 'MCH', 'DAD', 'EUM', 'EOSC', 'KLAY', 'USDN', 'ROAD',
    'BRZ', 'CNTM', 'XDC', 'TRB', 'OXT', 'BOA', 'CETH', 'XTP', 'OGN', 'HEX', 'KOK', 'APIX', 'MESH', 'PCI',
    'KDA', 'BKK', 'SYM', 'WRX', 'NWC', 'XAUT', 'ERG', 'HBD', 'GOM2', 'USDJ', 'EWT', 'EURT', 'LCX', 'HIVE',
    'HUNT', 'SOL', 'HMR', 'CTSI', 'MWC', 'OBSR', 'EC', 'JST', 'TNC', 'PXP', 'BNS', 'TWT', 'LYXE', 'XPR',
    'STMX', 'AR', 'ASM', 'RSV', 'DKA', 'ALCH', '8X8', 'WGRT', 'COMP', 'UMA', 'CELO', 'BAL', 'BTSE', 'DOT',
    'ARX', 'ISP', 'AVAX', 'FIO', 'DEXT', 'ALEPH', 'MTA', 'ORN', 'DFI', 'YFI', 'XOR', 'HNT', 'RING', 'WNXM', 'CNS',
    'SUKU', 'DF', 'CREAM', 'GEEQ', 'XRT', 'SRM', 'NXM', 'ANW', 'CRV', 'SAND', 'OM', 'PRQ', 'YFII', 'PSG', 'SUSHI',
    'KLV', 'PEARL', 'EGLD', 'USTC', 'CORN', 'JFI', 'BEL', 'MATH', 'SWRV', 'AMP', 'PHA', 'FUND', 'ACH', 'GOF',
    'WING', 'PICKLE', 'GHST', 'UNI', 'ZYRO', 'DBOX', 'RARI', 'GALA', 'RIO', 'DHT', 'FLM', 'VELO', 'RFUEL', 'ONIT',
    'TITAN', 'CRU', 'POLS', 'CAKE', 'MXT', 'DMD', 'SFG', 'VALUE', 'AAVE', 'XVS', 'ALPHA', 'SCRT', 'DODO', 'CVP',
    'TRIX', 'INJ', 'PLA', 'EVER', 'UFT', 'AKT', 'FIS', 'HMT', 'FARM', 'AUDIO', 'CTK', 'KP3R', 'PERP', 'SLP', 'WOO',
    'AXS', 'GSWAP', 'UNFI', 'ALBT', 'SFI', 'HEGIC', 'UBX', 'API3', 'SKL', 'MED', 'MIR', 'COVER', 'ROOK', 'CTI',
    'BADGER', 'GRT', 'REEF', 'BOND', 'POND', 'TVK', 'RLY', 'STETH', '1INCH', 'FXS', 'LDO', 'TRU', 'BETH', 'LIT',
    'XTM', 'SFP', 'GUM', 'XNO', 'INDEX', 'ETH2', 'DAO', 'MDX', 'QUICKOLD', 'TORN', 'RAY', 'QTF', 'BONDLY', 'DG',
    'MUSE', 'MONAV', 'MASK', 'ALCX', 'STUDENTC', 'SUPER', 'BDP', 'ALICE', 'RAD', 'OXY', 'ANC', 'XYM', 'DORA',
    'CAT', 'PUNDIX', 'ILV', 'MOB', 'MCO2', 'LQTY', 'TKO', 'TLM', 'MBOX', 'FIDA', 'BOSON', 'PYR', 'SHIB', 'MINA',
    'FORTH', 'XCH', 'CTX', 'FLUX', 'SOMNIUM', 'ICP', 'GYEN', 'GDT', 'CSPR', 'CVX', 'KUB', 'GTC', 'NFT', 'FLY',
    'SPELL', 'ATA', 'CQT', 'FLOKI', 'XAVA', 'GENS', 'TRIBE', 'OOE', 'CFG', 'WCFG', 'CLV', 'C98', 'YGG', 'SAITAMA',
    'MPL', 'BIT', 'XRD', 'QI', 'MOVR', 'JASMY', 'EDEN', 'DDX', 'AGLD', 'DYDX', 'BTRST', 'ATLAS', 'KLO', 'PIT', 'ARV',
    'RARE', 'WTK', 'SBR', 'METIS', 'EVRY', 'RBN', 'ENS', 'SGB', 'SAMO', 'DAR', 'IMX', 'BEAM', 'BNX', 'NOTE', 'BOBA',
    'ANGLE', 'GMCOIN', 'BICO', 'DEXA', 'TONCOIN', 'VVS', 'VOXEL', 'UNB', 'DESO', 'SFM', 'HIGH', 'MNGO', 'SYN', 'GLMR',
    'GFI', 'AURORA', 'LOKA', 'OSMO', 'ASTR', 'GARI', 'DMTR', 'GMTT', 'CPOOL', 'ORCA', 'ATS', 'POSI', 'LNR', 'CCD',
    'DOME', 'ZENITH', 'BSW', 'APE', 'BCOIN', 'NYM', 'SD', 'GMT', 'STG', 'ALI', 'ASTO', 'GMX', 'MAGIC', 'XCN', 'PHB',
    'GAL', 'USDD', 'REI', 'FITFI', 'SIPHER', 'EVMOS', 'LUNA', 'OP', 'BREED', 'FORT', 'EKTA', 'THALES', 'EUL', 'SDL',
    'PATH', 'GBPT', 'STC', 'SQUIDGROW', 'ETHW', 'BLD', 'TAMA', 'AXL', 'TRIBL', 'MPLX', 'APT', 'WAXL', 'POLYX', 'RED',
    'HFT', 'ECOX', 'IPX', 'FBX', 'ING', 'QUICK', 'POL', 'AGI', 'MC', 'JUP', 'FEG', 'TAI', 'TBTC', 'SMT', 'KSM'}

# Compile regex patterns for each keyword
crypto_patterns = [re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE) for keyword in crypto_keywords]


def contains_crypto_keyword(tweet):
    """Check if the tweet contains any of the crypto-related keywords."""
    for pattern in crypto_patterns:
        if pattern.search(tweet):
            return pattern.pattern.strip(r'\b')
    return None


def process_file(file_path):
    """Process a single JSON file and extract relevant tweets."""
    tweets = []

    with bz2.open(file_path, 'rt', encoding='utf-8') as file:
        for line in file:
            try:
                data = json.loads(line)
                tweet_data = data.get('data', {})

                tweet_processing(tweet_data, tweets)

            except json.JSONDecodeError:
                continue

    return tweets


def process_gz_file(file_path):
    """Process a single JSON file and extract relevant tweets."""
    tweets = []

    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as file:
            for line in file:
                try:
                    tweet_data = json.loads(line)

                    tweet_processing(tweet_data, tweets)

                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e} in file {file_path}")
    except OSError as e:
        print(f"OSError: {e} in file {file_path}")

    return tweets


def tweet_processing(tweet_data, tweets):
    if 'id' in tweet_data:
        tweet_text = tweet_data.get('text', '')
        keyword_found = contains_crypto_keyword(tweet_text)

        if keyword_found:
            tweet = {
                'author_id': tweet_data.get('author_id'),
                'created_at': tweet_data.get('created_at'),
                'text': tweet_text,
                'tweet_id': tweet_data.get('id'),
                'word_found': keyword_found
            }
            tweets.append(tweet)
            print(f"Processed tweet with ID: {tweet['tweet_id']}")


def main():
    args = parse_arguments()
    dir_number = args.dir_number

    # Root directory containing the folders with JSON files.
    # The dir number is the number of the day, the script will access to every hour of that day and process the files.
    root_directory_path = fr'/Users/manuel/Downloads/{dir_number}'

    start_time = time.time()  # Record the start time

    all_tweets = []

    with ProcessPoolExecutor() as executor:
        futures = []

        for root, dirs, files in os.walk(root_directory_path):
            for filename in files:
                if filename.endswith('.json.bz2'):
                    file_path = os.path.join(root, filename)
                    futures.append(executor.submit(process_file, file_path))
                elif filename.endswith('.json.gz'):
                    file_path = os.path.join(root, filename)
                    futures.append(executor.submit(process_gz_file, file_path))

        for future in futures:
            result = future.result()
            if result:
                all_tweets.extend(result)

    all_tweets_df = pd.DataFrame(all_tweets)
    print("DataFrame shape:", all_tweets_df.shape)

    filtered_file_path = f'merged_filtered_crypto_tweets_{dir_number}.csv'
    all_tweets_df.to_csv(filtered_file_path, index=False)
    print(f"Filtered tweets saved to {filtered_file_path}")

    end_time = time.time()  # Record the end time
    duration = end_time - start_time  # Calculate the duration
    print(f"Script completed in {duration:.2f} seconds")  # Print the duration


if __name__ == '__main__':
    main()