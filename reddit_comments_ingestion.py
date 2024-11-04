import praw
import csv
import datetime
import os
import time
import prawcore

#  Reddit credentials
reddit = praw.Reddit(
    client_id='client_id',
    client_secret='client_secret',   
    user_agent='user-agent'
    )


start_datetime_str = '2020-01-01 00:00:00'  
end_datetime_str = '2024-07-31 23:59:59'    

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

# Combinar listas
all_keywords = [keyword for sublist in crypto_keywords.values() for keyword in sublist] + general_keywords  


# Subreddits
crypto_subreddits =['ethtrader','defi','CryptoMarkets','CardanoTrading','CardanoStakePools','CardanoDevelopers','Chainlink','CryptoCurrency','Bitcoin','ethereum','altcoin','CryptoTechnology','CryptoMoonShots','BitcoinBeginners','CryptoCurrencies','ICO','BitcoinMarkets','Ethfinance','solana', 'litecoin','CryptoNews','CryptoAnalysis','Crypto_General','CryptoCurrencyTrading','binance','maticnetwork']

#Guardar_comentarios
def save_comments(submission, csv_file_path):

    try:
        with open(csv_file_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)

            if csv_file.tell() == 0:
                writer.writerow([
                    "submission_id","subreddit",
                    "comment_id", "comment_body", "author", "created_datetime",
                    "Score", "distinguished", "total_awards"
                ])

            submission.comments.replace_more(limit=None) 

            for comment in submission.comments.list():
                created_datetime = datetime.datetime.utcfromtimestamp(comment.created_utc).strftime('%d/%m/%Y %H:%M:%S')
                author_name = comment.author.name if comment.author else "[deleted]"

                # Write each field to its own cell
                writer.writerow([
                    submission.id,
                    submission.subreddit.display_name,
                    comment.id,
                    comment.body,
                    author_name,
                    created_datetime,
                    comment.score,
                    comment.total_awards_received
                ])
    
    except Exception as e:
        print(f"Error saving comments from submission {submission.id}: {e}")

##Manejo limitaciones API
# Contador de publicaciones guardadas y tiempos de espera
submission_count = 0
request_count = 0
start_time = time.time()
wait_time = 60 
retry_attempts = 0
max_retries = 2

# Convertir datetime a timestamps
start_timestamp = start_datetime.timestamp()
end_timestamp = end_datetime.timestamp()

# Obtener publicaciones de los subreddits
for subreddit_name in crypto_subreddits:
    last_submission = None
    while True:
        search_query = (f"({' OR '.join(all_keywords)})")
        if last_submission:
            search_query += f" after:{last_submission.created_utc}"

        try:
            submissions = list(reddit.subreddit(subreddit_name).search(search_query, limit=None,sort='asc'))
        
        except prawcore.exceptions.NotFound:  # Capturar el error 404
            print(f"Error 404: Subreddit {subreddit_name} no encontrado o no accesible.")
            break  # Salir del bucle si el subreddit no existe
        
        except prawcore.exceptions.TooManyRequests as e:
                print("Rate limit exceeded. Sleeping for 1 minute...")
                time.sleep(60)  #Esperar 60 segundos antes de reintentar
                save_comments(submission, csv_file_path) 

        except prawcore.exceptions.ServerError as e:
            if e.response.status_code == 429:  # manejar específicamente error 429
                    print(f"Max retries reached for subreddit {subreddit_name}. Skipping...")
                    time.sleep(60)  
                    continue  # volver al inicio
                    break 
        if not submissions:
            break  # No hay más resultados

        for submission in submissions:
            submission_created_utc = submission.created_utc
            # Verificar si la publicación está dentro del rango de fechas
            if start_timestamp <= submission_created_utc: # <= end_timestamp:
                save_comments(submission, csv_file_path) # Llama a la funcion para guardar comments
                print(f"Comments saved from submission: {submission.id} (subreddit: {submission.subreddit}) - {datetime.datetime.utcfromtimestamp(submission.created_utc).strftime('%d/%m/%Y %H:%M:%S')}")
                submission_count += 1
                request_count += 1   # Seguir contando las requests, incluso si la publicación no se guarda
               
                 
        last_submission = submissions[-1]
