import requests
import datetime
import os
import pytz
import pandas as pd
from io import BytesIO 


headers = {
        "Cookie": "Cookie"
    }

def download_glassnode_data(coin, start_date_str, end_date_str):
    start_date = int(datetime.datetime.strptime(start_date_str, '%d-%m-%Y').timestamp())
    end_date = int(datetime.datetime.strptime(end_date_str, '%d-%m-%Y').timestamp())

   
    dataframes = {}  # Diccionario para almacenar los DataFrames por token

    # --- Precio ---
    base_url_price = "https://api.glassnode.com/v1/metrics/market/price_usd_close"
    url_price = f"{base_url_price}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_price = requests.get(url_price, headers=headers)

    if response_price.status_code == 200:
        df_price = pd.read_csv(BytesIO(response_price.content))
        df_price.columns = ['Fecha', 'Precio_USD']
        df_price['Token'] = coin
        dataframes['price'] = df_price  # Almacenar en el diccionario

    # --- Direcciones Activas ---
    base_url_active = "https://api.glassnode.com/v1/metrics/addresses/active_count"
    url_active = f"{base_url_active}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_active = requests.get(url_active, headers=headers)

    if response_active.status_code == 200:
        df_active = pd.read_csv(BytesIO(response_active.content))
        df_active.columns = ['Fecha', 'Direcciones_Activas']
        df_active['Token']= coin
        dataframes['active'] = df_active  # Almacenar en el diccionario


    # --- Total Transfer Volume from Exchanges ---
    base_url_transfers = "https://api.glassnode.com/v1/metrics/transactions/transfers_volume_from_exchanges_sum"
    url_transfers = f"{base_url_transfers}?a={coin}&c=usd&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_transfers = requests.get(url_transfers, headers=headers)


    if response_transfers.status_code == 200:
        df_transferencias = pd.read_csv(BytesIO(response_transfers.content))
        df_transferencias.columns = ['Fecha', 'Total Transfer Volume from Exchanges']  # Renombrar la columna
        df_transferencias['Token'] = coin  # Agregar columna 'Token'
        dataframes['Total Transfer Volume from Exchanges'] = df_transferencias
    
    # --- otal Transfer Volume to Exchanges --- 

    base_url_transfers_to_exchange = "https://api.glassnode.com/v1/metrics/transactions/transfers_volume_to_exchanges_sum"
    url_transfers_to_exchange = f"{ base_url_transfers_to_exchange}?a={coin}&c=usd&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_transfers_to_exchange = requests.get(url_transfers_to_exchange, headers=headers)


    if response_transfers_to_exchange.status_code == 200:
        df_transfers_to_exchange = pd.read_csv(BytesIO(response_transfers_to_exchange.content))
        df_transfers_to_exchange.columns = ['Fecha', 'Total Transfer Volume to Exchanges']  # Renombrar la columna
        df_transfers_to_exchange['Token'] = coin  # Agregar columna 'Token'
        dataframes['Total Transfer Volume to Exchanges'] = df_transfers_to_exchange
    

    # --- Nuevas direcciones --- 
    base_url_new_adresses = "https://api.glassnode.com/v1/metrics/addresses/new_non_zero_count"
    url_new_adresses = f"{base_url_new_adresses}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_new_adresses = requests.get(url_new_adresses, headers=headers)

    if response_new_adresses.status_code == 200:
        df_new_adressess = pd.read_csv(BytesIO(response_new_adresses.content))
        df_new_adressess.columns = ['Fecha', 'new_adressess']  # Renombrar la columna
        df_new_adressess['Token'] = coin  # Agregar columna 'Token'
        dataframes['transferencias'] = df_new_adressess

    # --- Balance on exchange--- 
    #'https://api.glassnode.com/v1/metrics/distribution/balance_exchanges?a=BTC&c=native&e=aggregated&f=CSV&i=24h&timestamp_format=humanized'

    base_url_balance_exchange = "https://api.glassnode.com/v1/metrics/distribution/balance_exchanges"
    url_balance_exchange = f"{base_url_balance_exchange}?a={coin}&c=usd&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_balance_exchange = requests.get(url_balance_exchange, headers=headers)

    if response_balance_exchange.status_code == 200:
        df_balance_exchange = pd.read_csv(BytesIO(response_balance_exchange.content))
        df_balance_exchange.columns = ['Fecha', 'balance_exchange']  # Renombrar la columna
        df_balance_exchange['Token'] = coin  # Agregar columna 'Token'
        dataframes['balance_exchange'] = df_balance_exchange
    
    # --- Total transfer volume on chain---
    #'https://api.glassnode.com/v1/metrics/transactions/transfers_volume_sum?a=BTC&c=native&f=CSV&i=24h&timestamp_format=humanized' 

    base_url_on_chain_total_transfer_volume = "https://api.glassnode.com/v1/metrics/transactions/transfers_volume_sum"
    url_on_chain_total_transfer_volume = f"{base_url_on_chain_total_transfer_volume}?a={coin}&c=usd&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_on_chain_total_transfer_volume = requests.get(url_on_chain_total_transfer_volume, headers=headers)

    if response_on_chain_total_transfer_volume.status_code == 200:
        df_bon_chain_total_transfer_volume = pd.read_csv(BytesIO(response_on_chain_total_transfer_volume.content))
        df_bon_chain_total_transfer_volume.columns = ['Fecha', 'on_chain_total_transfer_volume']  # Renombrar la columna
        df_bon_chain_total_transfer_volume['Token'] = coin  # Agregar columna 'Token'
        dataframes['on_chain_total_transfer_volume'] = df_bon_chain_total_transfer_volume

    # --- Price down from ATH --- 
    #"https://api.glassnode.com/v1/metrics/market/price_drawdown_relative?a=BTC&f=CSV&i=24h&timestamp_format=humanized"

    base_url_down_from_ATH = "https://api.glassnode.com/v1/metrics/market/price_drawdown_relative"
    url_down_from_ATH = f"{base_url_down_from_ATH}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_down_from_ATH = requests.get(url_down_from_ATH, headers=headers)

    if response_down_from_ATH.status_code == 200:
        df_down_from_ATH = pd.read_csv(BytesIO(response_down_from_ATH.content))
        df_down_from_ATH.columns = ['Fecha', 'down_from_ATH(%)']  # Renombrar la columna
        df_down_from_ATH['Token'] = coin  # Agregar columna 'Token'
        dataframes['down_from_ATH(%)'] = df_down_from_ATH
    
    # --- Reserve risk ---
    #'https://api.glassnode.com/v1/metrics/indicators/reserve_risk?a=BTC&f=CSV&i=1h&timestamp_format=humanized'
    
    base_url_reserve_risk= "https://api.glassnode.com/v1/metrics/indicators/reserve_risk"
    url_reserve_risk = f"{base_url_reserve_risk}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_reserve_risk = requests.get(url_reserve_risk, headers=headers)

    if response_reserve_risk.status_code == 200:
        df_reserve_risk= pd.read_csv(BytesIO(response_reserve_risk.content))
        df_reserve_risk.columns = ['Fecha', 'Reserve risk']  # Renombrar la columna
        df_reserve_risk['Token'] = coin  # Agregar columna 'Token'
        dataframes['Reserve risk'] = df_reserve_risk

    # ---  Futures Long Liquidations (Total) [USD] --- 
    #'https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_long_sum?a=BTC&c=usd&e=aggregated&f=CSV&i=24h&timestamp_format=humanized'

    base_url_futures_l_liq= "https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_long_sum"
    url_futures_l_liq = f"{base_url_futures_l_liq}?a={coin}&c=usd&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_reserve_futures_l_liq = requests.get(url_futures_l_liq, headers=headers)

    if response_reserve_futures_l_liq.status_code == 200:
        df_reserve_futures_l_liq= pd.read_csv(BytesIO(response_reserve_futures_l_liq.content))
        df_reserve_futures_l_liq.columns = ['Fecha', 'futures_l_liq']  # Renombrar la columna
        df_reserve_futures_l_liq['Token'] = coin  # Agregar columna 'Token'
        dataframes['futures_l_liq'] = df_reserve_futures_l_liq

    # --- BTCBTC: Futures Short Liquidations (Total) [USD] - All Exchanges --- 
    #'https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_short_sum?a=BTC&c=usd&e=aggregated&f=CSV&i=1h&timestamp_format=humanized'

    base_url_futures_s_liq= "https://api.glassnode.com/v1/metrics/derivatives/futures_liquidated_volume_short_sum"
    url_futures_s_liq = f"{base_url_futures_s_liq}?a={coin}&c=usd&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_reserve_futures_s_liq = requests.get(url_futures_s_liq, headers=headers)

    if response_reserve_futures_s_liq.status_code == 200:
        df_reserve_futures_s_liq= pd.read_csv(BytesIO(response_reserve_futures_s_liq.content))
        df_reserve_futures_s_liq.columns = ['Fecha', 'futures_s_liq']  # Renombrar la columna
        df_reserve_futures_s_liq['Token'] = coin  # Agregar columna 'Token'
        dataframes['futures_s_liq'] = df_reserve_futures_s_liq

    #--- Realized Price [USD] --- 
    #'https://api.glassnode.com/v1/metrics/market/price_realized_usd?a=BTC&f=CSV&i=24h&timestamp_format=humanized'

    base_url_realized_price= "https://api.glassnode.com/v1/metrics/market/price_realized_usd"
    url_realized_price = f"{base_url_realized_price}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_realized_price = requests.get(url_realized_price, headers=headers)

    if response_realized_price.status_code == 200:
        df_realized_price= pd.read_csv(BytesIO(response_realized_price.content))
        df_realized_price.columns = ['Fecha', 'Realized_price']  # Renombrar la columna
        df_realized_price['Token'] = coin  # Agregar columna 'Token'
        dataframes['Realized_price'] = df_realized_price

    #--- MVRV --- 

    #'https://api.glassnode.com/v1/metrics/market/mvrv?a=BTC&f=CSV&i=1h&timestamp_format=humanized'

    base_url_MVRV= "https://api.glassnode.com/v1/metrics/market/mvrv"
    url_MVRV = f"{base_url_MVRV}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_MVRV = requests.get(url_MVRV, headers=headers)

    if response_MVRV.status_code == 200:
        df_MVRV= pd.read_csv(BytesIO(response_MVRV.content))
        df_MVRV.columns = ['Fecha', 'MVRV']  # Renombrar la columna
        df_MVRV['Token'] = coin  # Agregar columna 'Token'
        dataframes['MVRV'] = df_MVRV

    #--- NVT Ratio --- 
    #'https://api.glassnode.com/v1/metrics/indicators/nvt?a=BTC&f=CSV&i=1h&timestamp_format=humanized'

    base_url_NTV= "https://api.glassnode.com/v1/metrics/indicators/nvt"
    url_NVT = f"{base_url_NTV}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_NVT = requests.get(url_NVT, headers=headers)

    if response_NVT.status_code == 200:
        df_NVT= pd.read_csv(BytesIO(response_NVT.content))
        df_NVT.columns = ['Fecha', 'NVT']  # Renombrar la columna
        df_NVT['Token'] = coin  # Agregar columna 'Token'
        dataframes['NVT'] = df_NVT


    #--- Adjusted SOPR---
    #'https://api.glassnode.com/v1/metrics/indicators/sopr_adjusted?a=BTC&f=CSV&i=1h&timestamp_format=humanized'
    base_url_SOPR= "https://api.glassnode.com/v1/metrics/indicators/sopr_adjusted"
    url_SOPR = f"{base_url_SOPR}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_SOPR = requests.get(url_SOPR, headers=headers)

    if response_SOPR.status_code == 200:
        df_SOPR= pd.read_csv(BytesIO(response_SOPR.content))
        df_SOPR.columns = ['Fecha', 'SOPR']  # Renombrar la columna
        df_SOPR['Token'] = coin  # Agregar columna 'Token'
        dataframes['SOPR'] = df_SOPR

    
    # --- BTC: Stock-to-Flow Deflection --- 
    #'https://api.glassnode.com/v1/metrics/indicators/stock_to_flow_deflection?a=BTC&f=CSV&i=1h&timestamp_format=humanized'
    
    base_url_stock_to_flow_deflection= "https://api.glassnode.com/v1/metrics/indicators/stock_to_flow_deflection"
    url_stock_to_flow_deflection = f"{base_url_stock_to_flow_deflection}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_stock_to_flow_deflection = requests.get(url_stock_to_flow_deflection, headers=headers)

    if response_stock_to_flow_deflection.status_code == 200:
        df_stock_to_flow_deflectio= pd.read_csv(BytesIO(response_stock_to_flow_deflection.content))
        df_stock_to_flow_deflectio.columns = ['Fecha', 'stock to flow']  # Renombrar la columna
        df_stock_to_flow_deflectio['Token'] = coin  # Agregar columna 'Token'
        dataframes['stock to flow'] = df_stock_to_flow_deflectio
    
    base_url_percent_supply_in_profit= "https://api.glassnode.com/v1/metrics/supply/profit_relative"
    url_supply_percent_supply_in_profit = f"{base_url_percent_supply_in_profit}?a={coin}&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_percent_supply_in_profit = requests.get(url_supply_percent_supply_in_profit, headers=headers)

    if response_percent_supply_in_profit.status_code == 200:
        df_percent_supply_in_profit= pd.read_csv(BytesIO(response_percent_supply_in_profit.content))
        df_percent_supply_in_profit.columns = ['Fecha', 'percent_supply_in_profit']  # Renombrar la columna
        df_percent_supply_in_profit['Token'] = coin
        dataframes['percent_supply_in_profit'] = df_percent_supply_in_profit
    



    # --- Combinar DataFrames ---
    combined_df = None
    for metric, df in dataframes.items():
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.merge(df, on=['Fecha', 'Token'], how='outer')  # Combinación externa


    return combined_df  

coins = ['BTC', 'ETH', 'BNB', 'SOL', 'LTC','LINK','MATIC','ADA'] #Lista de criptomonedas objetivo

# Rango de fechas
start_date_str = '01-01-2020'  # Ejemplo
end_date_str = '31-07-2024' 

# Descargar y combinar datos
all_data = []
for coin in coins:
    df = download_glassnode_data(coin, start_date_str, end_date_str)
    if df is not None:
        all_data.append(df)

combined_df = pd.concat(all_data)

# Guardar en CSV
ruta_guardado = "/Users/marcoaedo/Desktop/TFM/2da iteración/Nuevos modelos/"
now = datetime.datetime.now(pytz.timezone('UTC'))
nombre_archivo = f" BTC_Data_glassnode24h_{start_date_str}_to_{end_date_str}.csv"
ruta_completa = os.path.join(ruta_guardado, nombre_archivo)

columnas_deseadas = ['Fecha', 'Token', 'Precio_USD','Direcciones_Activas','Total Transfer Volume from Exchanges','Total Transfer Volume to Exchanges','new_adressess','balance_exchange','on_chain_total_transfer_volume',
             'down_from_ATH(%)','Reserve risk','Realized_price','MVRV','SOPR','stock to flow','percent_supply_in_profit'] #Manejar variables según disponibilidad
columnas_existentes = [col for col in columnas_deseadas if col in combined_df.columns]

# Guardar el DataFrame resultante en un CSV
combined_df[columnas_existentes].to_csv(ruta_completa, index=False)

print(f"Datos combinados guardados en: {ruta_completa}")
