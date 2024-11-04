import pandas as pd
import numpy as np

data_hourly = pd.read_csv('/file_hourly.csv')
data_daily = pd.read_csv('/file_daily.csv')

coins = ['BTC', 'ETH', 'LTC','BNB', 'SOL', 'LINK', 'MATIC', 'ADA']
daily_metrics = ['dormancy_flow', 'puell_multiple', 'fear&greed', 'supply_long_term_holders'] #Métricas diarias
base_tokens = ['BTC', 'ETH']


token_final_dataset = {}
token_datasets_hourly = {}
token_datasets_daily = {}

#Filtrar datasets
for coin in coins:
    token_data_hourly = data_hourly[data_hourly['Token'] == coin]     # filtrar por coin dataset por hora
    token_datasets_hourly[coin] = token_data_hourly     #guardar dataset por hora
    token_data_daily = data_daily[data_daily['Token'] == coin]     # filtrar por coin dataset por día
    token_datasets_daily[coin] = token_data_daily     #guardar dataset por día

#Frecuencia diaria a hora + interpolar
for coin in coins:
    hourly_df = token_datasets_hourly[coin]
    daily_df = token_datasets_daily[coin]

    hourly_df['Fecha'] = pd.to_datetime(hourly_df['Fecha'], format='%Y-%m-%dT%H:%M:%SZ')
    daily_df['Fecha'] = pd.to_datetime(daily_df['Fecha'], format='%Y-%m-%dT%H:%M:%SZ')

    hourly_df.set_index('Fecha', inplace=True)
    daily_df.set_index('Fecha', inplace=True)
    if 'Precio_USD' in daily_df.columns:
        daily_df['Precio_USD'] = daily_df['Precio_USD'].fillna(method='ffill')


    daily_hourly = daily_df.resample('H').asfreq()


    # Interpolar
    for metric in daily_metrics:
        if metric in daily_hourly.columns:
            daily_hourly[metric] = daily_hourly[metric].interpolate()


    combined_df = hourly_df.join(daily_hourly, how='outer', lsuffix='_hourly', rsuffix='_daily')

    combined_df = combined_df.drop(columns=[col for col in combined_df.columns if col.endswith('_daily')])
    combined_df.columns = combined_df.columns.str.replace('_hourly', '')
    combined_df.reset_index(inplace=True)

    token_final_dataset[coin] = combined_df

    token_final_dataset[coin] = token_final_dataset[coin].dropna(subset=['Precio_USD'])

    token_final_dataset[coin] = token_final_dataset[coin].reset_index(drop=True)

#Sumar precio de base token a dataset
for coin in coins:

    if coin not in base_tokens:
        # Calcular correlación con tokens base
        correlations = {
            base_token: token_final_dataset[coin]['Precio_USD'].corr(token_final_dataset[base_token]['Precio_USD'])
            for base_token in base_tokens
        }


        max_corr_token = max(correlations, key=correlations.get)  # encontrar correlación más alta

        # Ensure 'Fecha' columns are in datetime format for subsequent operations
        token_final_dataset[max_corr_token]['Fecha'] = pd.to_datetime(token_final_dataset[max_corr_token]['Fecha'])
        token_final_dataset[coin]['Fecha'] = pd.to_datetime(token_final_dataset[coin]['Fecha'])

        # lookup
        base_token_lookup = token_final_dataset[max_corr_token].set_index('Fecha')['Precio_USD'].to_dict()

        # sumar precio de token base a dataset
        token_final_dataset[coin][f'Precio_USD_{max_corr_token}'] = token_final_dataset[coin]['Fecha'].map(base_token_lookup)
        token_final_dataset[coin] = token_final_dataset[coin].dropna(axis=1, how='all')
        token_final_dataset[coin] = token_final_dataset[coin].dropna()

