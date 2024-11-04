
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
collection = db["reddit_submissions"]

start_time = time.time()
nltk.download('stopwords') #stopwords en inglés

# Cargar el modelo CryptoBERT y el tokenizador correspondiente
tokenizer = AutoTokenizer.from_pretrained("kk08/CryptoBERT",model_max_length=512)
model = AutoModelForSequenceClassification.from_pretrained("kk08/CryptoBERT")

classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)  # Crear el pipeline de análisis de sentimiento

df_submissions = pd.read_csv("/route/file.csv") # Leer publicaciones

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
    words = text.lower().split() # Tokenizar el texto
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


df_submissions["submission_text"] = df_submissions["submission_text"].astype(str).apply(clean_text) # Aplicar la función de limpieza a la columna del texto  desde el texto de la publicación
df_submissions["submission_text_crypto_tokens"] = df_submissions["submission_text"].apply(extract_crypto_tokens) # Aplicar la función para extraer tokens de criptomonedas desde el texto de la publicación
df_submissions['submission_text_sentiment_analisis'] = pd.DataFrame(df_submissions["submission_text"].apply(lambda x: classifier(x, truncation=True)).tolist()) #Análisis de sentimiento
df_submissions['submission_text_sentiment_analisis'] = df_submissions['submission_text_sentiment_analisis'].astype(str)
df_submissions['submission_text_label'] = df_submissions['submission_text_sentiment_analisis'].str.extract(r"'label':\s'(\w+)'")    # Extract label texto de la publicación
df_submissions['submission_text_score'] = df_submissions['submission_text_sentiment_analisis'].str.extract(r"'score':\s(\d+\.\d+)").astype(float) # Extraer score texto de la publicación

df_submissions["submission_title"] = df_submissions["submission_title"].astype(str).apply(clean_text) # Aplicar la función de limpieza a la columna del texto  desde el título de la publicación
df_submissions["submission_title_crypto_tokens"] = df_submissions["submission_title"].apply(extract_crypto_tokens) # Aplicar la función para extraer tokens de criptomonedas del título de la publicación
df_submissions['submission_title_sentiment_analisis'] = pd.DataFrame(df_submissions["submission_title"].apply(lambda x: classifier(x, truncation=True)).tolist())    #Aplicar la función de análisis de sentimiento usando CryptoBERT
df_submissions['submission_title_sentiment_analisis'] = df_submissions['submission_title_sentiment_analisis'].astype(str)
df_submissions['submission_title_label'] = df_submissions['submission_title_sentiment_analisis'].str.extract(r"'label':\s'(\w+)'")       # Extract label desde el título de la publicación
df_submissions['submission_title_score'] = df_submissions['submission_title_sentiment_analisis'].str.extract(r"'score':\s(\d+\.\d+)").astype(float) # Extract score esde el título de la publicación

#Manejar nulls
df_submissions['submission_text_crypto_tokens'] = df_submissions['submission_text_crypto_tokens'].fillna('[]')
df_submissions['submission_title_crypto_tokens'] = df_submissions['submission_title_crypto_tokens'].fillna('[]')

# Convertir strings a listas
df_submissions['submission_text_crypto_tokens'] = df_submissions['submission_text_crypto_tokens'].astype(str).str.strip('[]').str.split(', ')
df_submissions['submission_title_crypto_tokens'] = df_submissions['submission_title_crypto_tokens'].astype(str).str.strip('[]').str.split(', ')

# Combinar columnas
df_submissions['all_crypto_tokens'] = df_submissions['submission_text_crypto_tokens'] + df_submissions['submission_title_crypto_tokens']
df_submissions['all_crypto_tokens'] = df_submissions['all_crypto_tokens'].apply(lambda x: list(set(filter(None, x))))


#Condiciones para elegir score y label entre texto y título de la publicación
conditions = [
    df_submissions['submission_text'].isna(),
    df_submissions['submission_text_score'] >= df_submissions['submission_title_score'],
    df_submissions['submission_text_score'] < df_submissions['submission_title_score']
]

choices_score = [
    df_submissions['submission_title_score'],
    df_submissions['submission_text_score'],
    df_submissions['submission_title_score']
]

choices_label = [
    df_submissions['submission_title_label'],
    df_submissions['submission_text_label'],
    df_submissions['submission_title_label']
]

df_submissions['final_score'] = np.select(conditions, choices_score, default=np.nan)
df_submissions['final_label'] = np.select(conditions, choices_label, default=np.nan)


# Columnas finales
columns_to_keep_submissions = ['submission_id', 'created_datetime','submission_title', 'submission_text', 'all_crypto_tokens', 'final_score', 'final_label']

# Dataframe final
df_submissions = df_submissions[columns_to_keep_submissions]

new_column_names_submissions = {
    'submission_id': 'submission_id',
    'created_datetime': 'created_datetime',
    'submission_title': 'title',
    'submission_text': 'text',
    'all_crypto_tokens': 'tokens',
    'final_score': 'score',
    'final_label':'label'
}

df_submissions = df_submissions.rename(columns=new_column_names_submissions)

df_submissions['created_datetime'] = pd.to_datetime(df_submissions['created_datetime'], format='%d/%m/%Y %H:%M:%S')
df_submissions['created_datetime'] = df_submissions['created_datetime'].dt.floor('H').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
df_submissions['created_datetime'] = pd.to_datetime(df_submissions['created_datetime'], format='%Y-%m-%dT%H:%M:%SZ')


#Llevar submissiones preprocesadas a mongo
records = df_submissions.to_dict(orient='records')
result = collection.insert_many(records)
print(f"Se insertaron {len(result.inserted_ids)} documentos.")



