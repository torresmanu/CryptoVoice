import json
import os

# Load the filtered coin data
with open('filtered_coin_data.json', 'r', encoding='utf-8') as f:
    filtered_coin_data = json.load(f)

# Ensure output directory exists
filtered_historical_dir = 'filtered_historical'
os.makedirs(filtered_historical_dir, exist_ok=True)

# Source directory for existing historical data
historical_data_dir = 'historical_data'

# Copy historical data of filtered coins to the new directory
for coin in filtered_coin_data:
    coin_symbol = coin['symbol']
    historical_data_file = os.path.join(historical_data_dir, f'{coin_symbol}_history.json')

    # Check if the historical data file exists
    if os.path.exists(historical_data_file):
        with open(historical_data_file, 'r', encoding='utf-8') as f:
            history_data = json.load(f)

        # Save the historical data to the filtered_historical directory
        filtered_historical_file = os.path.join(filtered_historical_dir, f'{coin_symbol}_history.json')
        with open(filtered_historical_file, 'w', encoding='utf-8') as f:
            json.dump(history_data, f, indent=4, ensure_ascii=False)
        
        print(f'Historical data for {coin["name"]} ({coin_symbol}) has been saved to {filtered_historical_file}')
    else:
        print(f'Historical data file for {coin["name"]} ({coin_symbol}) not found in {historical_data_dir}')

print('Filtered historical data files have been saved in the filtered_historical directory')
