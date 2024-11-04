import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Attention, GlobalAveragePooling1D
from tensorflow.keras.layers import LSTM
from keras.layers import Bidirectional, Attention
import time
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import math
import matplotlib.pyplot as plt
import requests,datetime,os,pytz
from io import BytesIO


coins = ['BTC','ETH','BNB','SOL','LINK','LTC', 'MATIC', 'ADA']
token_datasets = {}
reddit_datasets = {}
news_datasets = {}
token_final_dataset = {}
token_final_dataset_plot = {}
scaled_dataset = {}
predict_scaled_dataset = {}
sc_predict_scaler = {}
seq_length = {
    "BTC": 720,
    "ETH": 540,
    "SOL": 540,
    "ADA": 240,
    "LTC": 540,
    "MATIC": 720,
    "BNB": 240,
    "LINK": 540
}
X_train_list ={}
y_train_list = {}
X_test_list = {}
y_test_list = {}
models = {}
training_time = {}
last_seq_length_data = {}
train_predict = {}
test_predict = {}
res_train_predict = {}
res_y_train_list = {}
res_test_predict  = {}
res_y_test_list = {}
plot_data = {}
future_predictions = {}
rescaled_future_predictions = {}


uri = "mongodb+srv://user_tfm:tfm.123.@cluster-tfm.oobnjla.mongodb.net/?retryWrites=true&w=majority&appName=cluster-tfm"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client["TFM"]
try:
    client.admin.command('ping')
    print("Conexion exitosa")
except Exception as e:
    print(e)

glassnode_collection = db["Glassnode"] #Collection glassnode
reddit_collection = db["reddit_grouped"] #Collection reddit
news_collection = db["news_grouped"] #Collection news

projection = {"_id": 0}
glassnode = list(glassnode_collection.find({}, projection))
reddit = list(reddit_collection.find({}, projection))
news = list(news_collection.find({}, projection))

reddit_tokens_df = pd.DataFrame(reddit)   #convertir collections en df
glassnode_df = pd.DataFrame(glassnode)  #convertir collections en df
news_df = pd.DataFrame(news)        #convertir collections en df

#renombrar columnas
reddit_tokens_df = reddit_tokens_df.rename(columns={
      'created_datetime': 'created_datetime',
      'count_posititve': 'token_reddit_pos_count',
      'count_negative': 'token_reddit_neg_count',
      'count_total': 'token_reddit_total'
  })
reddit_general_df = reddit_tokens_df[reddit_tokens_df['token'] == 'general'].reset_index(drop=True)

reddit_general_df = reddit_general_df.rename(columns={
      'created_datetime': 'created_datetime',
      'token': 'token',
      'token_reddit_pos_count': 'general_reddit_pos_count',
      'token_reddit_neg_count': 'general_reddit_neg_count',
      'token_reddit_total': 'general_reddit_total'
  })
reddit_general_df = reddit_general_df.drop(['token'], axis=1)

news_df = news_df.rename(columns={
      'DATE': 'created_datetime',
      'TOKEN' : 'token',
      'count_positive': 'news_pos_count',
      'count_negative': 'news_neg_count',
      'total_count': 'news_total'
  })
news_general_df = news_df[news_df['token'] == 'GENERAL'].reset_index(drop=True)

news_general_df = news_general_df.rename(columns={
      'created_datetime': 'created_datetime',
      'token' : 'token',
      'news_pos_count': 'news_general_pos_count',
      'news_neg_count': 'news_general_neg_count',
      'news_total': 'news_general_total'
  })

#Unir dataframes por token y calcular médias móviles
for coin in coins:
    token_dataset = glassnode_df[glassnode_df['Token'] == coin]  # Filtrar por token
    token_datasets[coin] = token_dataset  
    reddit_filtered = reddit_tokens_df[reddit_tokens_df['token'] == coin] # Filtrar por token
    reddit_datasets[coin] = reddit_filtered 
    news_dataset = news_df[news_df['token'] == coin] # Filtrar por token
    news_datasets[coin] = news_dataset 

    hourly_df = token_datasets[coin]
    reddit_df = reddit_datasets[coin]
    news_df = news_datasets[coin]

    hourly_df['Fecha'] = pd.to_datetime(hourly_df['Fecha'])
    hourly_df.set_index('Fecha', inplace=True)     # Set 'Fecha' as the index (for resampling)

    # Combine the dataframes using an outer join to keep all data
    combined_df = hourly_df.join(reddit_df.set_index('created_datetime'), how='left', on='Fecha')

    specific_columns = ['general_reddit_pos_count', 'general_reddit_neg_count','general_reddit_total']
    combined_df = combined_df.join(reddit_general_df.set_index('created_datetime')[specific_columns], how='left', on='Fecha', lsuffix='_combined')#, rsuffix='_reddit')


    combined_df['token_reddit_pos_count'] = combined_df['token_reddit_pos_count'].fillna(method='ffill')
    combined_df['token_reddit_neg_count'] = combined_df['token_reddit_neg_count'].fillna(method='ffill')
    combined_df['token_reddit_total'] = combined_df['token_reddit_total'].fillna(method='ffill')

    combined_df['general_reddit_pos_count'] = combined_df['general_reddit_pos_count'].fillna(method='ffill')
    combined_df['general_reddit_neg_count'] = combined_df['general_reddit_neg_count'].fillna(method='ffill')
    combined_df['general_reddit_total'] = combined_df['general_reddit_total'].fillna(method='ffill')

    combined_df.reset_index(inplace=True)


    news_df['created_datetime'] = pd.to_datetime(news_df['created_datetime'])
    news_general_df['created_datetime'] = pd.to_datetime(news_general_df['created_datetime'])

    news_specific_columns = ['news_pos_count', 'news_neg_count', 'news_total']
    news_general_specific_columns = ['news_general_pos_count', 'news_general_neg_count', 'news_general_total']

    combined_df = combined_df.join(news_df.set_index('created_datetime')[news_specific_columns], how='left', on='Fecha')

    combined_df['news_pos_count'] = combined_df['news_pos_count'].fillna(method='ffill')
    combined_df['news_neg_count'] = combined_df['news_neg_count'].fillna(method='ffill')
    combined_df['news_total'] = combined_df['news_total'].fillna(method='ffill')

    combined_df = combined_df.join(news_general_df.set_index('created_datetime')[news_general_specific_columns], how='left', on='Fecha')

    combined_df['news_general_pos_count'] = combined_df['news_general_pos_count'].fillna(method='ffill')
    combined_df['news_general_neg_count'] = combined_df['news_general_neg_count'].fillna(method='ffill')
    combined_df['news_general_total'] = combined_df['news_general_total'].fillna(method='ffill')


    token_final_dataset[coin] = combined_df
    token_final_dataset_plot[coin] = combined_df.copy()
    token_final_dataset[coin] = token_final_dataset[coin].dropna(subset=['Precio_USD'])

    windows = [336]  # Days for moving averages

    for window in windows:
      for col in ['token_reddit_pos_count', 'token_reddit_neg_count', 'token_reddit_total','general_reddit_pos_count','general_reddit_neg_count','general_reddit_total']:#,'news_pos_count','news_neg_count','news_total','news_general_pos_count','news_general_neg_count','news_general_total']:#,'general_reddit_pos_count','general_reddit_neg_count','general_reddit_total']:
          token_final_dataset[coin][f'{col}_ma_{window}'] = token_final_dataset[coin][col].rolling(window=window).mean()


    token_final_dataset[coin] = token_final_dataset[coin].dropna(axis=1, how='all')
    token_final_dataset[coin] = token_final_dataset[coin].drop(['token'], axis=1)


    token_final_dataset[coin] = token_final_dataset[coin].reset_index(drop=True)
    token_final_dataset[coin] = token_final_dataset[coin].dropna()
    token_final_dataset[coin] = token_final_dataset[coin].drop(['Token'], axis=1)
    token_final_dataset[coin] = token_final_dataset[coin].drop(['Fecha'], axis=1)

#Mantener variables con correlación lineal vs precio > 0.4 (valor absoluto)
for coin in coins:
    df = token_final_dataset[coin]
    numeric_columns = df.select_dtypes(include='float64').columns.drop('Precio_USD')    
    correlation_matrix = df[['Precio_USD'] + list(numeric_columns)].dropna().corr() 
    correlation_with_precio = correlation_matrix['Precio_USD'] 
    variables_a_mantener = correlation_with_precio[abs(correlation_with_precio) >= 0.4].index.tolist() 
    df_filtrado = df[variables_a_mantener]  # Crear un nuevo dataframe con solo las variables seleccionadas
    token_final_dataset[coin] = df_filtrado

#crear secuencias
def create_sequences(data, seq_length):
    xs, ys = [], []
    for i in range(len(data) - seq_length):
        x = data[i:(i + seq_length)]  # Seleccionar todas las columnas
        y = data[i + seq_length, 0]   # Precio_USD en el siguiente paso
        xs.append(x)
        ys.append(y)
    return np.array(xs), np.array(ys)

# Normalizar datasets
for coin in coins:
    df = token_final_dataset[coin]
    sc_training = MinMaxScaler()
    scaled_data = sc_training.fit_transform(df)
    scaled_dataset[coin] = scaled_data

    sc_predict = MinMaxScaler()
    predict_scaled_data = sc_predict.fit_transform(df.iloc[:, 0:1])
    predict_scaled_dataset[coin] = predict_scaled_data
    sc_predict_scaler[coin] = sc_predict

# Crear secuencias y dividir para cada dataset normalizado
for coin in coins:
    X, y = create_sequences(scaled_dataset[coin], seq_length[coin])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    X_train_list[coin] = X_train
    y_train_list[coin] = y_train
    X_test_list[coin] = X_test
    y_test_list[coin] = y_test

#Modelo 
for coin in coins:
    model = Sequential()  # Creación del modelo
    model.add(Bidirectional(LSTM(64, return_sequences=True), input_shape=(X_train_list[coin].shape[1], X_train_list[coin].shape[2])))
    model.add(Bidirectional(LSTM(32, return_sequences=True)))
    model.add(Bidirectional(LSTM(32, return_sequences=True)))
    model.add(Bidirectional(LSTM(32)))
    model.add(Dropout(0.2))
    model.add(Dense(32, activation='relu'))  
    model.add(Dense(16, activation='relu')) 
    model.add(Dense(1))
    model.compile(optimizer='adam', loss='mse')

    history = model.fit(X_train_list[coin], y_train_list[coin], epochs=50, batch_size=32, validation_split=0.1) # Entrenamiento del modelo

    start_time = time.time()
    end_time = time.time()
    training_time[coin] = end_time - start_time
    models[coin] = model     # Agregar el modelo entrenado a la lista

#Desescalar resultados
for coin in coins:
  train_predict[coin] = models[coin].predict(X_train_list[coin])
  test_predict[coin] = models[coin].predict(X_test_list[coin])
  res_train_predict[coin] = sc_predict_scaler[coin].inverse_transform(train_predict[coin])
  res_y_train_list[coin] = sc_predict_scaler[coin].inverse_transform(y_train_list[coin].reshape(-1, 1))
  res_test_predict[coin] = sc_predict_scaler[coin].inverse_transform(test_predict[coin])
  res_y_test_list[coin] = sc_predict_scaler[coin].inverse_transform(y_test_list[coin].reshape(-1, 1))

#Predecir 720 horas (30 días)
for coin in coins:
    last_seq_length_data[coin] = scaled_dataset[coin][-seq_length[coin]:]  
    current_seq = last_seq_length_data[coin]
    predictions = []
    for _ in range(720):  
        predicted_value = models[coin].predict(current_seq.reshape(1, seq_length[coin], current_seq.shape[1])) #
        predictions.append(predicted_value)
        current_seq = np.roll(current_seq, -1, axis=0)
        current_seq[-1] = predicted_value
        future_predictions[coin] = np.array(predictions)

    rescaled_future_predictions[coin] = sc_predict_scaler[coin].inverse_transform(future_predictions[coin].reshape(-1, 1))

