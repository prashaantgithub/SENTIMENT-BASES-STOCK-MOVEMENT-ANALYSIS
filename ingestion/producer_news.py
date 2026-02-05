import sys
import os

# --- FIX: Add project root to path so 'ingestion.config' can be imported ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import time
import requests
import uuid
from ingestion import config

def write_to_staging(data):
    """Writes a single news record as a JSON file in the staging folder."""
    try:
        filename = f"news_{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
        filepath = os.path.join(config.STAGING_NEWS, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f)
            
    except Exception as e:
        print(f"❌ Error writing file: {e}")

def fetch_and_produce_news():
    if not config.NEWS_API_KEY:
        print("❌ ERROR: NEWS_API_KEY is missing in .env file.")
        return

    base_url = "https://newsapi.org/v2/everything"
    
    print(f"🚀 Starting Real News Producer for stocks: {config.STOCKS_LIST}")
    print(f"📂 Writing to: {config.STAGING_NEWS}")

    while True:
        for stock in config.STOCKS_LIST:
            params = {
                "q": stock,
                "apiKey": config.NEWS_API_KEY,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 5
            }

            try:
                response = requests.get(base_url, params=params)
                data = response.json()

                if data.get("status") == "ok":
                    articles = data.get("articles", [])
                    for article in articles:
                        news_message = {
                            "stock": stock,
                            "title": article.get("title"),
                            "description": article.get("description"),
                            "source": article.get("source", {}).get("name"),
                            "published_at": article.get("publishedAt"),
                            "url": article.get("url")
                        }
                        
                        write_to_staging(news_message)
                        print(f"[{stock}] Saved news: {article.get('title')[:50]}...")
                else:
                    print(f"⚠️ API Error for {stock}: {data.get('message')}")

            except Exception as e:
                print(f"❌ Error fetching news for {stock}: {e}")

        print("⏳ Waiting 60 seconds before next fetch cycle...")
        time.sleep(60)

if __name__ == "__main__":
    fetch_and_produce_news()