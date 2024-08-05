import requests
import json
import time
import os
from datetime import datetime

# API key for CryptoCompare
api_key = ''

# CryptoCompare endpoints
coins_list_url = 'https://min-api.cryptocompare.com/data/all/coinlist'
historical_data_url = 'https://min-api.cryptocompare.com/data/v2/histoday'

# Headers for CryptoCompare API requests
headers = {
    'authorization': f'Apikey {api_key}'
}

# Fetch the list of coins
print('Fetching list of coins...')
response = requests.get(coins_list_url, headers=headers)
coins = response.json().get('Data', {})

# Setting the specific date range for the historical data request
#start_date = datetime(2021, 08, 01)
#end_date = datetime(2021, 10, 31)

# Convert datetime to Unix timestamp
#start_timestamp = int(time.mktime(start_date.timetuple()))
#end_timestamp = int(time.mktime(end_date.timetuple()))
start_timestamp = 1664582400
end_timestamp = 1675123200

# Ensure output directory exists
output_dir = 'historical_data'
os.makedirs(output_dir, exist_ok=True)

# Prepare the data for each coin
filtered_coin_data = []

# Fetching details for each coin
for coin_symbol, coin_info in coins.items():
    coin_name = coin_info['FullName']
    coin_id = coin_info['Id']
    
    try:
        print(f'Fetching historical data for {coin_name} ({coin_symbol})...', flush=True)

        # Fetching historical data for each coin
        params = {
            'fsym': coin_symbol,
            'tsym': 'USD',
            'limit': 200,  # Maximum number of days CryptoCompare allows per request
            'toTs': end_timestamp
        }
        history_response = requests.get(historical_data_url, headers=headers, params=params)
        
        try:
            history_data = history_response.json()
            
            # Example data extraction (you need to adjust based on actual response structure)
            try:
                if history_data and 'Data' in history_data and 'Data' in history_data['Data']:
                    daily_volumes = [day['volumeto'] for day in history_data['Data']['Data']]
                    total_volume_usd = sum(daily_volumes)

                    # Check if any day has a volume between 100,000 and 50,000,000 USD
                    if any(100000 <= volume <= 50000000 for volume in daily_volumes):
                        filtered_coin_data.append({
                            'name': coin_name,
                            'symbol': coin_symbol,
                            'total_volume_usd': total_volume_usd
                        })
                        print(f'Coin {coin_name} ({coin_symbol}) has been added to the filtered list.')

                        # Save the historical data to a file named after the coin symbol
                        with open(os.path.join(output_dir, f'{coin_symbol}_history.json'), 'w', encoding='utf-8') as f:
                            json.dump(history_data, f, indent=4)

            except KeyError as e:
                print(f"KeyError for coin {coin_name} ({coin_symbol}): {e}")
            
        except json.JSONDecodeError:
            print(f"Could not decode JSON for historical data of coin {coin_name} ({coin_symbol}). Skipping...")

        # Sleep for a short while to avoid hitting rate limits
        time.sleep(1)
    
    except UnicodeEncodeError:
        print(f"Skipping coin {coin_name} ({coin_symbol}) due to encoding issues.")

# Save the filtered coin data to a JSON file
with open('filtered_coin_data.json', 'w', encoding='utf-8') as f:
    json.dump(filtered_coin_data, f, indent=4, ensure_ascii=False)

print(f'Filtered coin data has been saved to filtered_coin_data.json with {len(filtered_coin_data)} entries')
print(f'Individual historical data files have been saved in the {output_dir} directory')
