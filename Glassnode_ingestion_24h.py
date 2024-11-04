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


    dataframes = {}     # Diccionario para almacenar los DataFrames por token

    # --- Precio ---
    base_url_price = "https://api.glassnode.com/v1/metrics/market/price_usd_close"
    url_price = f"{base_url_price}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_price = requests.get(url_price, headers=headers)

    if response_price.status_code == 200:
        df_price = pd.read_csv(BytesIO(response_price.content))
        df_price.columns = ['Fecha', 'Precio_USD']
        df_price['Token'] = coin
        dataframes['price'] = df_price  # Almacenar en el diccionario

    
    #---Dormancy flow ---

    base_url_dormancy_flow= "https://api.glassnode.com/v1/metrics/indicators/dormancy_flow"
    url_reserve_dormancy_flow = f"{base_url_dormancy_flow}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_dormancy_flow = requests.get(url_reserve_dormancy_flow, headers=headers)

    if response_dormancy_flow.status_code == 200:
        df_dormancy_flow= pd.read_csv(BytesIO(response_dormancy_flow.content))
        df_dormancy_flow.columns = ['Fecha', 'dormancy_flow']  # Renombrar la columna
        df_dormancy_flow['Token'] = coin
        dataframes['dormancy_flow'] = df_dormancy_flow  # Almacenar en el diccionario

    

    # --- puell multiple ---
    #'https://api.glassnode.com/v1/metrics/indicators/puell_multiple?a=BTC&f=CSV&i=24h&timestamp_format=humanized'

    base_url_puell_multiple= "https://api.glassnode.com/v1/metrics/indicators/puell_multiple"
    url_stock_to_puell_multiple = f"{base_url_puell_multiple}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_puell_multiple = requests.get(url_stock_to_puell_multiple, headers=headers)

    if response_puell_multiple.status_code == 200:
        df_puell_multiple= pd.read_csv(BytesIO(response_puell_multiple.content))
        df_puell_multiple.columns = ['Fecha', 'puell_multiple']  # Renombrar la columna
        df_puell_multiple['Token'] = coin  # Agregar columna 'Token'
        dataframes['puell_multiple'] = df_puell_multiple


    # --- fear & greed index --- 
    #'https://api.glassnode.com/v1/metrics/indicators/fear_greed?a=BTC&f=CSV&i=24h&timestamp_format=humanized'

    base_url_fear_greed_index= "https://api.glassnode.com/v1/metrics/indicators/fear_greed"
    url_reserve_fear_greed_index = f"{base_url_fear_greed_index}?a={coin}&e=aggregated&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_reserve_fear_greed_index = requests.get(url_reserve_fear_greed_index, headers=headers)

    if response_reserve_fear_greed_index.status_code == 200:
        df_reserve_fear_greed_index= pd.read_csv(BytesIO(response_reserve_fear_greed_index.content))
        df_reserve_fear_greed_index.columns = ['Fecha', 'fear&greed']  # Renombrar la columna
        df_reserve_fear_greed_index['Token'] = coin
        dataframes['fear&greed'] = df_reserve_fear_greed_index

    # --- BTC: Total Supply Held by Long-Term Holders [BTC] --- 
    #'https://api.glassnode.com/v1/metrics/supply/lth_sum?a=BTC&c=native&f=CSV&i=24h&timestamp_format=humanized'


    base_url_supply_long_term_holders= "https://api.glassnode.com/v1/metrics/supply/lth_sum"
    url_supply_long_term_holders = f"{base_url_supply_long_term_holders}?a={coin}&c=usd&f=CSV&i=24h&s={start_date}&timestamp_format=humanized&u={end_date}"
    response_supply_long_term_holders = requests.get(url_supply_long_term_holders, headers=headers)

    if response_supply_long_term_holders.status_code == 200:
        df_supply_long_term_holders= pd.read_csv(BytesIO(response_supply_long_term_holders.content))
        df_supply_long_term_holders.columns = ['Fecha', 'supply_long_term_holders']  # Renombrar la columna
        df_supply_long_term_holders['Token'] = coin
        dataframes['supply_long_term_holders'] = df_supply_long_term_holders
    

    # --- Combinar DataFrames ---
    combined_df = None
    for metric, df in dataframes.items():
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.merge(df, on=['Fecha', 'Token'], how='outer')  # Combinación externa

    return combined_df  


coins = ['BTC', 'ETH', 'BNB', 'SOL', 'LTC','LINK','MATIC','ADA'] # Lista de criptomonedas objetivo

# Rango fechas 
start_date_str = '01-01-2020'  
end_date_str = '31-07-2024' 

all_data = []
for coin in coins:
    df = download_glassnode_data(coin, start_date_str, end_date_str)
    if df is not None:
        all_data.append(df)

combined_df = pd.concat(all_data)

# Guardar datos
ruta_guardado = "/route/"
now = datetime.datetime.now(pytz.timezone('UTC'))
nombre_archivo = f"Data_glassnode24h_{start_date_str}_to_{end_date_str}.csv"
ruta_completa = os.path.join(ruta_guardado, nombre_archivo)

columnas_deseadas = ['Fecha', 'Token', 'Precio_USD','fear&greed','dormancy_flow','puell_multiple','supply_long_term_holders']

columnas_existentes = [col for col in columnas_deseadas if col in combined_df.columns]

# Guardar el DataFrame resultante en un CSV
combined_df[columnas_existentes].to_csv(ruta_completa, index=False)

#combined_df[['Fecha', 'Token', 'Precio_USD','fear&greed','dormancy_flow','puell_multiple','supply_long_term_holders']].to_csv(ruta_completa, index=False)  # Guardar solo las columnas necesarias

print(f"Datos combinados guardados en: {ruta_completa}")
