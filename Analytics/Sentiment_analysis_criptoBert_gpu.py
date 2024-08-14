# sentiment_analysis.py

import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline
import re
import nltk
from nltk.corpus import stopwords
from datasets import Dataset  # Import the datasets library
import swifter
import torch
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="Perform sentiment analysis on cryptocurrency tweets.")
    parser.add_argument('date', type=str, help="The date of the CSV file to process (format: YYYYMMDD)")
    return parser.parse_args()

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

if __name__ == '__main__':
    # Parse command-line arguments
    args = parse_arguments()
    date = args.date

    # Download English stopwords if you don't have them yet
    print("Downloading NLTK stopwords...")
    nltk.download('stopwords', quiet=True)
    stop_words = set(stopwords.words('english'))
    print("Stopwords downloaded and loaded.")

    # Compiled regex patterns
    url_pattern = re.compile(r'http\S+')
    non_word_pattern = re.compile(r'[^\w\s]')

    # Determine the device (GPU or CPU)
    device = 0 if torch.cuda.is_available() else -1
    if device == 0:
        print(f"CUDA is available. Using device: {torch.cuda.get_device_name(0)}")
    else:
        print("CUDA is not available. Using CPU.")

    # Load the tokenizer and model
    print("Loading tokenizer and model...")
    tokenizer = AutoTokenizer.from_pretrained("kk08/CryptoBERT", model_max_length=512)
    model = AutoModelForSequenceClassification.from_pretrained("kk08/CryptoBERT").to(device)
    classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer, device=device)
    print("Tokenizer and model loaded.")

    # Read the CSV file
    print("Reading CSV file...")
    df = pd.read_csv(fr'/Users/Manuel/Documents/TFM/merged_filtered_crypto_tweets_{date}.csv')
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
    # Use Swifter to speed up the apply operation directly on the Series
    df["cleaned_text"] = df["text"].swifter.apply(clean_text)
    print("Text cleaned.")

    # Convert the DataFrame to a Dataset
    dataset = Dataset.from_pandas(df)

    # Define a function to apply the classifier to each batch
    def batch_predict(batch):
        # Perform sentiment analysis on the batch
        predictions = classifier(batch['cleaned_text'], truncation=True, batch_size=16)
        # Extract labels and scores
        labels = [pred['label'] for pred in predictions]
        scores = [pred['score'] for pred in predictions]
        # Return as a dictionary to update the dataset
        return {'label': labels, 'score': scores}

    # Apply the classifier in batches using the map function
    print("Starting sentiment analysis...")
    results = dataset.map(batch_predict, batched=True, batch_size=16)
    print("Sentiment analysis completed.")

    # Convert results back to a DataFrame
    results_df = results.to_pandas()

    # Concatenate the DataFrames
    df = pd.concat([df, results_df[['label', 'score']]], axis=1)

    print("Extracting crypto tokens...")
    df["crypto_tokens"] = df["cleaned_text"].apply(extract_crypto_tokens)
    print("Crypto tokens extracted.")

    # Save the resulting DataFrame to a new CSV file
    output_path = fr'/Users/Manuel/Documents/TFM/resultados_sentimiento_{date}.csv'
    print(f"Saving results to {output_path}...")
    df.to_csv(output_path, index=False)
    print("Results saved successfully.")
