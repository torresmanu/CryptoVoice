import pandas as pd
import numpy as np
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import re
import nltk
from nltk.corpus import stopwords
import swifter  # Library to speed up apply operations in pandas
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Function to clean the text
def clean_text(text):
    # Remove URLs
    text = url_pattern.sub('', text)
    # Remove icons and special characters
    text = non_word_pattern.sub('', text)
    # Tokenize the text and remove English stopwords
    words = text.lower().split()
    words = [word for word in words if word not in stop_words]
    return " ".join(words)

# Function to extract cryptocurrency tokens
def extract_crypto_tokens(text):
    tokens = set()  # Use a set to avoid duplicates
    for word in text.split():
        for symbol, keywords in crypto_keywords.items():
            if word.lower() in keywords:
                tokens.add(symbol)
        if word.lower() in general_keywords:
            tokens.add(word.lower())
    return ", ".join(tokens)

# Function to perform sentiment analysis in batches
def sentiment_analysis_batch(texts, classifier):
    return [classifier(text, truncation=True) for text in texts]

if __name__ == '__main__':
    # Download English stopwords if you don't have them yet
    print("Downloading NLTK stopwords...")
    nltk.download('stopwords', quiet=True)
    stop_words = set(stopwords.words('english'))
    print("Stopwords downloaded and loaded.")

    # Compiled regex patterns
    url_pattern = re.compile(r'http\S+')
    non_word_pattern = re.compile(r'[^\w\s]')

    # Load the tokenizer and model
    print("Loading tokenizer and model...")
    tokenizer = AutoTokenizer.from_pretrained("kk08/CryptoBERT", model_max_length=512)
    model = AutoModelForSequenceClassification.from_pretrained("kk08/CryptoBERT")
    classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    print("Tokenizer and model loaded.")

    # Read the CSV file
    print("Reading CSV file...")
    df = pd.read_csv("/Users/Manuel/Documents/TFM/merged_filtered_crypto_tweets_20220603.csv")
    print("CSV file read. Number of records:", len(df))

    # Dictionary of tokens or keywords related to cryptocurrencies
    crypto_keywords = {
        'BTC': ['Bitcoin', 'BTC', 'Satoshi', '₿', 'bitcoin', 'btc', 'satoshi'],
        'ETH': ['Ethereum', 'ETH', 'Ether', 'Ξ', 'ethereum', 'ether'],
        'BNB': ['Binance Coin', 'BNB', 'binance coin', 'bnb'],
        'SOL': ['Solana', 'SOL', 'solana', 'sol'],
        'LTC': ['Litecoin', 'LTC', 'litecoin', 'ltc'],
        'LINK': ['Chainlink', 'LINK', 'chainlink', 'link'],
        'MATIC': ['Polygon', 'MATIC', 'polygon', 'matic']
    }

    # General keywords related to cryptocurrency and sentiments
    general_keywords = [
        'crypto', 'cryptocurrency', 'blockchain', 'DeFi', 'NFT', 'altcoin', 'stablecoin',
        'HODL', 'FOMO', 'FUD', 'bullish', 'bearish', 'pump', 'dump', 'moon', 'rekt'
    ]

    print("Cleaning text...")
    df["cleaned_text"] = df["text"].swifter.apply(clean_text)
    print("Text cleaned.")

    # Number of processes to use
    num_processes = multiprocessing.cpu_count()
    print(f"Using {num_processes} processes based on CPU cores.")

    # Start sentiment analysis
    print("Starting sentiment analysis...")
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        chunks = np.array_split(df["cleaned_text"], num_processes)
        tasks = [executor.submit(sentiment_analysis_batch, chunk, classifier) for chunk in chunks]
        sentiment_results = [task.result() for task in tasks]
    print("Sentiment analysis completed.")

    # Flatten the results and create DataFrame columns
    flat_results = [result for sublist in sentiment_results for result in sublist]
    results_df = pd.DataFrame(flat_results)

    # Concatenate the DataFrames
    df = pd.concat([df, results_df], axis=1)

    print("Extracting crypto tokens...")
    df["crypto_tokens"] = df["cleaned_text"].swifter.apply(extract_crypto_tokens)
    print("Crypto tokens extracted.")

    # Save the resulting DataFrame to a new CSV file
    output_path = "/Users/Manuel/Documents/TFM/resultados_sentimiento.csv"
    print(f"Saving results to {output_path}...")
    df.to_csv(output_path, index=False)
    print("Results saved successfully.")
