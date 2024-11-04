import pymongo
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://user_tfm:<password>@cluster-tfm.oobnjla.mongodb.net/?retryWrites=true&w=majority&appName=cluster-tfm"
client = MongoClient(uri, server_api=ServerApi('1'))  #client para conectar
db = client["TFM"]

# Colecciones
submissions_collection = db["reddit_submissions"]
comments_collection = db["reddit_comments"]

# confirmar conexión
try:
    client.admin.command('ping')
    print("conexión exitosa")
except Exception as e:
    print(e)

# Queries y proyecciones para cada colección
submissions_query = {}
submissions_projection = {"submission_id": 1, "tokens": 1, "_id": 0}

comments_query = {}
comments_projection = {
    "submission_id": 1,
    "comment_id": 1,
    "tokens": 1,
    "created_datetime": 1,
    "label": 1,
    "_id": 0,
}

# Ejecutar las consultas y crear DataFrames
submissions_results = list(submissions_collection.find(submissions_query, submissions_projection))
comments_results = list(comments_collection.find(comments_query, comments_projection))

df_submissions = pd.DataFrame(submissions_results)
df_comments = pd.DataFrame(comments_results)

merged_df = pd.merge(df_comments, df_submissions, on='submission_id', how='left')

# Combinar los arrays de tokens en una nueva columna
merged_df['tokens_x'] = merged_df['tokens_x'].apply(lambda x: [x] if isinstance(x, float) else x)
merged_df['tokens_y'] = merged_df['tokens_y'].apply(lambda x: [x] if isinstance(x, float) else x)
merged_df['combined_tokens'] = (merged_df['tokens_x'] + merged_df['tokens_y']).apply(list)

# Asegurar que los tokens en la lista combinada sean únicos
merged_df['combined_tokens'] = merged_df['combined_tokens'].apply(lambda x: list(set(x)))

# Eliminar las columnas de tokens originale
merged_df = merged_df.drop(['tokens_x', 'tokens_y'], axis=1)
merged_df['combined_tokens'] = merged_df['combined_tokens'].apply(lambda x: [token for token in x if token])

### ---- General ---- "" 
general_keywords = [
    'Crypto','crypto', 'cryptocurrency', 'blockchain', 'DeFi', 'NFT', 'altcoin', 'stablecoin',
    'HODL', 'FOMO', 'FUD', 'bullish', 'bearish', 'pump', 'dump', 'moon',
]

general_keywords_lower = [keyword.lower() for keyword in general_keywords]

# Función para procesar cada lista de tokens
def replace_keywords(token_list):
    return ['general' if token in general_keywords_lower else token for token in token_list]

# Función a la columna 'combined_tokens' 
merged_df['combined_tokens'] = merged_df['combined_tokens'].apply(replace_keywords)
merged_df['combined_tokens'] = merged_df['combined_tokens'].apply(lambda x: list(set(x)))
merged_df['combined_tokens'] = merged_df['combined_tokens'].apply(lambda x: [token for token in x if token])


# Generar N Registros por token 
exploded_df = merged_df.explode('combined_tokens')

exploded_df = exploded_df.rename(columns={'combined_tokens': 'token'})

#Agrupar por created_datetime y token, contar comentarios positivos y negativos y generar un total
grouped_df = exploded_df.groupby(['created_datetime', 'token'])['label'].value_counts().unstack(fill_value=0)

# Reset index
grouped_df.reset_index(inplace=True)

# Rename columnas
grouped_df = grouped_df.rename(columns={-1.0: 'count_negative', 1.0: 'count_positive'})
grouped_df["count_total"] = grouped_df["count_negative"] + grouped_df["count_positive"]

grouped_df['created_datetime'] = pd.to_datetime(grouped_df['created_datetime'], format='%d/%m/%Y %H:%M:%S')
grouped_df['created_datetime'] = grouped_df['created_datetime'].dt.floor('H').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
grouped_df['created_datetime'] = pd.to_datetime(grouped_df['created_datetime'], format='%Y-%m-%dT%H:%M:%SZ')

collection_grouped = db["reddit_grouped"]

#  Insertar los documentos en la colección de mongo
records = grouped_df.to_dict(orient='records')
result = collection_grouped.insert_many(records)
print(f"Se insertaron {len(result.inserted_ids)} documentos.")