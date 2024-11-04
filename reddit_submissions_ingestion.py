import praw
import csv
import datetime
import os
import time
import prawcore

#Credenciales
reddit = praw.Reddit(
    client_id='client_id',
    client_secret='client_secret',   
    user_agent='user_agent')

start_datetime_str = '2020-01-01 00:00:00'  
end_datetime_str = '2023-01-31 23:59:59'  

# Convertir las fechas y horas a datetime
start_datetime = datetime.datetime.strptime(start_datetime_str, '%Y-%m-%d %H:%M:%S')
end_datetime = datetime.datetime.strptime(end_datetime_str, '%Y-%m-%d %H:%M:%S')


csv_file_path = '/route/file.csv'
crypto_keywords = {
    'BTC': ['Bitcoin', 'BTC', 'Satoshi', '₿', 'bitcoin', 'btc', 'satoshi'],  
    'ETH': ['Ethereum', 'ETH', 'Ether', 'Ξ', 'ethereum', 'ether'],  
    'BNB': ['Binance Coin', 'BNB', 'binance coin', 'bnb'],
    'SOL': ['Solana', 'SOL', 'solana', 'sol'],
    'LTC': ['Litecoin', 'LTC', 'litecoin', 'ltc'],
    'LINK': ['Chainlink', 'LINK', 'chainlink', 'link'],
    'MATIC': ['Polygon', 'MATIC', 'polygon', 'matic'],
    'ADA': ['Cardano', 'cardano', 'ADA', 'ada'],
    'DOT': ['Polkadot', 'Dot', 'dot', 'polkadot'],
}


# Palabras clave generales relacionadas con criptomonedas y sentimientos
general_keywords = [
    'Crypto','crypto', 'cryptocurrency', 'blockchain', 'DeFi', 'NFT', 'altcoin', 'stablecoin',
    'HODL', 'FOMO', 'FUD', 'bullish', 'bearish', 'pump', 'dump', 'moon',
]

# Combinar las listas en una sola
all_keywords = general_keywords + [keyword for sublist in crypto_keywords.values() for keyword in sublist]

crypto_subreddits =['ethtrader','defi','CryptoMarkets','CardanoTrading','CardanoStakePools','CardanoDevelopers','Chainlink','CryptoCurrency','Bitcoin','ethereum','altcoin','CryptoTechnology','CryptoMoonShots','BitcoinBeginners','CryptoCurrencies','ICO','BitcoinMarkets','Ethfinance','solana', 'litecoin','CryptoNews','CryptoAnalysis','Crypto_General','CryptoCurrencyTrading','binance','maticnetwork']

headers = ['submission_id', "subreddit",'submission_title', 'submission_text', 'author', 'created_datetime', 'subreddit', 'score', 'total_awards_received']

#Guardar_comentarios
def save_submission(submission, csv_file_path):
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)

    with open(csv_file_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Escribir encabezados solo la primera vez
        if f.tell() == 0:
            writer.writerow(headers)

        created_datetime = datetime.datetime.utcfromtimestamp(submission.created_utc).strftime('%d/%m/%Y %H:%M:%S')
        author_name = submission.author.name if submission.author else "[deleted]"

        # Escribir datos de la publicación en columnas separadas
        writer.writerow([
            submission.id,
            submission.subreddit.display_name,
            submission.title,
            submission.selftext,
            author_name,
            created_datetime,
            submission.subreddit,
            submission.score,
            submission.total_awards_received
        ])

# Contador de publicaciones guardadas
submission_count = 0

request_count = 0
start_time = time.time()


start_timestamp = start_datetime.timestamp()
end_timestamp = end_datetime.timestamp()

# Obtener publicaciones de los subreddits
for subreddit_name in crypto_subreddits:
    last_submission = None
    while True:
        search_query = f"({' OR '.join(all_keywords)})"
        if last_submission:
            search_query += f" after:{last_submission.created_utc}"

        try:
            submissions = list(reddit.subreddit(subreddit_name).search(search_query, limit=None,sort='asc'))
        
        except prawcore.exceptions.NotFound:  # Capturar el error 404
            print(f"Error 404: Subreddit {subreddit_name} no encontrado o no accesible.")
            break  # Salir del bucle si el subreddit no existe

        if not submissions:
            break  # No hay más resultados

        for submission in submissions:
            submission_created_utc = submission.created_utc
            # Verificar si la publicación está dentro del rango de fechas
            if start_timestamp <= submission_created_utc: #<= end_timestamp:
                save_submission(submission, csv_file_path)
                print(f"Publicación guardada: {submission.id} (subreddit: {submission.subreddit}) - {datetime.datetime.utcfromtimestamp(submission.created_utc).strftime('%d/%m/%Y %H:%M:%S')}")
                submission_count += 1

            request_count += 1  

            # Verificar si se ha alcanzado el límite de solicitudes por minuto
            if request_count >= 100:
                elapsed_time = time.time() - start_time
                if elapsed_time < 60:
                    time.sleep(60 - elapsed_time)  # Esperar hasta completar el minuto
                request_count = 0  # Reiniciar el contador de solicitudes
                start_time = time.time() 
        last_submission = submissions[-1]