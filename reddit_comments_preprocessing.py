import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import re
import nltk
from nltk.corpus import stopwords
import numpy as np
import time

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://user_tfm:<password>@cluster-tfm.oobnjla.mongodb.net/?retryWrites=true&w=majority&appName=cluster-tfm"
client = MongoClient(uri, server_api=ServerApi('1')) #client para conectar 
# confirmar conexión
try:
    client.admin.command('ping')
    print("conexión exitosa")
except Exception as e:
    print(e)

db = client["TFM"]
collection = db["reddit_comments"]

start_time = time.time()

nltk.download('stopwords') #stopwords en inglés

# Cargar el modelo CryptoBERT y el tokenizador correspondiente
tokenizer = AutoTokenizer.from_pretrained("kk08/CryptoBERT",model_max_length=512)
model = AutoModelForSequenceClassification.from_pretrained("kk08/CryptoBERT")

classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer) # Crear el pipeline de análisis de sentimiento

df_comments = pd.read_csv ("/route/file.csv") # Leer comentarios

# cripto_keywords
crypto_keywords = {
    'BTC': ['Bitcoin', 'BTC', 'Satoshi', '₿', 'bitcoin', 'btc', 'satoshi'],
    'ETH': ['Ethereum', 'ETH', 'Ether', 'Ξ', 'ethereum', 'ether','eth'],
    'BNB': ['Binance Coin', 'BNB', 'binance coin', 'bnb'],
    'SOL': ['Solana', 'SOL', 'solana', 'sol'],
    'LTC': ['Litecoin', 'LTC', 'litecoin', 'ltc'],
    'LINK': ['Chainlink', 'LINK', 'chainlink', 'link'],
    'MATIC': ['Polygon', 'MATIC', 'polygon', 'matic'],
    'ADA': ['Cardano', 'cardano', 'ADA', 'ada'],
}

# Palabras clave generales relacionadas con criptomonedas y sentimientos
general_keywords = [
    'crypto', 'cryptocurrency', 'blockchain', 'DeFi', 'NFT', 'altcoin', 'stablecoin',
    'hodl', 'fomo', 'fud', 'bullish', 'bearish', 'pump', 'dump', 'moon', 'rekt'
]

# Función para limpiar el texto 
def clean_text(text):
    text = re.sub(r'http\S+', '', text) # Eliminar URLs
    text = re.sub(r'[^\w\s]', '', text)  # Eliminar íconos y caracteres especiales 
    words = text.lower().split()  # Tokenizar el texto
    words = [word for word in words if word not in stopwords.words('english')]
    return " ".join(words)

# Función para extraer tokens de criptomonedas
def extract_crypto_tokens(text):
    tokens = []
    for word in text.split():
        for symbol, keywords in crypto_keywords.items():
            if word.lower() in keywords:
                tokens.append(symbol)  # Usar el símbolo en lugar del nombre completo
        if word.lower() in general_keywords:
            tokens.append(word.lower())  # Agregar palabras clave generales
    return ", ".join(set(tokens))  # Eliminar duplicados y unir


### -------------------- Comments ----------------------- ### 
df_comments = df_comments[df_comments['distinguished'] != 'moderator'] #eliminar si es un comentario del moderador del subreddit
df_comments = df_comments[df_comments['comment_body'] != 'deleted'] #eliminar si es un comentario fue borrado
df_comments = df_comments[df_comments['comment_body'] != 'removed'] #eliminar si es un comentario fue limitado

df_comments["comment_body"] = df_comments["comment_body"].astype(str).apply(clean_text)  # Aplicar la función de limpieza a la columna del texto  desde el texto del comentario
df_comments['sentiment_analisis'] = pd.DataFrame(df_comments["comment_body"].apply(lambda x: classifier(x, truncation=True)).tolist())  #Aplicar la función de análisis de sentimiento usando CryptoBERT
df_comments["crypto_tokens"] = df_comments["comment_body"].apply(extract_crypto_tokens) # Aplicar la función para extraer tokens de criptomonedas
df_comments['sentiment_analisis'] = df_comments['sentiment_analisis'].astype(str)
df_comments['comment_label'] = df_comments['sentiment_analisis'].str.extract(r"'label':\s'(\w+)'")     # Extract label del comentario
df_comments['comment_score'] = df_comments['sentiment_analisis'].str.extract(r"'score':\s(\d+\.\d+)").astype(float) # Extract score del comentario
df_comments["crypto_tokens"] = df_comments["comment_body"].apply(extract_crypto_tokens) # Aplicar la función para extraer tokens de criptomonedas
df_comments['comment_label'] = df_comments['comment_label'].replace({'LABEL_0': -1, 'LABEL_1': 1})

df_comments['created_datetime'] = pd.to_datetime(df_comments['created_datetime'], format='%d/%m/%Y %H:%M:%S')
df_comments['created_datetime'] = df_comments['created_datetime'].dt.floor('H').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
df_comments['created_datetime'] = pd.to_datetime(df_comments['created_datetime'], format='%Y-%m-%dT%H:%M:%SZ')

df_comments['crypto_tokens'] = df_comments['crypto_tokens'].str.split(', ')

columns_to_keep_comments=['submission_id','comment_id','comment_body','subreddit','created_datetime','crypto_tokens','comment_label','comment_score']
df_comments = df_comments[columns_to_keep_comments]

new_column_names_comments = {
    'submission_id': 'submission_id',
    'comment_id':'comment_id',
    'comment_body': 'cleant_text',
    'subreddit':'subreddit',
    'created_datetime': 'created_datetime',
    'crypto_tokens': 'tokens',
    'comment_label': 'label',
    'comment_score': 'score'
}

df_comments = df_comments.rename(columns=new_column_names_comments)

#  Insertar los documentos en la colección de mongo
records = df_comments.to_dict(orient='records')
result = collection.insert_many(records)
print(f"Se insertaron {len(result.inserted_ids)} documentos.")



