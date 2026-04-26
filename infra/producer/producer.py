import time
import requests
import json
from kafka import KafkaProducer

API_Key = 'd6biu71r01qp4lhvkfk0d6biu71r01qp4lhvkfkg'
BASE_URL = 'https://finnhub.io/api/v1/quote'
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 8, 0)
)

def fetch_quote(symbol):
    url = f'{BASE_URL}?symbol={symbol}&token={API_Key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data['symbol'] = symbol
        data['fetched_at'] = time.time()
        return data
    except Exception as e:
        print(f"Error fetching quote for {symbol}: {e}")
        return None

while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f'producsing {quote}')
            producer.send('stock-quotes', value=quote)
    time.sleep(6)
