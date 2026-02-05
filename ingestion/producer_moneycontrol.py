import sys
import os
import json
import time
import requests
import uuid
import datetime
from bs4 import BeautifulSoup

# --- FIX: Add project root to path so 'ingestion.config' can be imported ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ingestion import config

# Mapping Stock Codes to MoneyControl URL Slugs
MC_SLUGS = {
    "RELIANCE": "reliance-industries",
    "TCS": "tcs",
    "INFY": "infosys",
    "HDFCBANK": "hdfc-bank",
    "ICICIBANK": "icici-bank",
    "TATAMOTORS": "tata-motors",
    "AXISBANK": "axis-bank",
    "HCLTECH": "hcl-technologies",
    "BHARTIARTL": "bharti-airtel",
    "WIPRO": "wipro"
}

# User-Agent is crucial for scraping to avoid 403 Forbidden errors
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def write_to_staging(data):
    """Writes a scraped headline as a JSON file in the staging folder."""
    try:
        # Create unique filename: mc_TIMESTAMP_UUID.json
        filename = f"mc_{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
        filepath = os.path.join(config.STAGING_MONEYCONTROL, filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f)
            
    except Exception as e:
        print(f"❌ Error writing file: {e}")

def scrape_moneycontrol():
    print(f"🚀 Starting MoneyControl Scraper...")
    print(f"📂 Writing to: {config.STAGING_MONEYCONTROL}")

    while True:
        for stock_code in config.STOCKS_LIST:
            slug = MC_SLUGS.get(stock_code)
            if not slug:
                continue

            url = f"https://www.moneycontrol.com/news/tags/{slug}.html"
            
            try:
                response = requests.get(url, headers=HEADERS)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # MoneyControl News Structure: <li> with class 'clearfix' inside a specific ID/div
                    news_items = soup.find_all('li', class_='clearfix')
                    
                    count = 0
                    for item in news_items:
                        if count >= 2: break # Grab top 2 latest headlines per stock per cycle
                        
                        # Extract Headline
                        h2 = item.find('h2')
                        if not h2: continue
                        
                        link = h2.find('a')
                        if not link: continue
                        
                        headline = link.get_text().strip()
                        
                        # Extract Time (if available)
                        time_span = item.find('span')
                        date_str = time_span.get_text() if time_span else datetime.datetime.now().strftime("%B %d, %Y %I:%M %p IST")
                        
                        # Create Message
                        # We use 'text' field to match the schema Spark expects
                        msg = {
                            "id": str(uuid.uuid4()),
                            "text": headline,
                            "created_at": datetime.datetime.utcnow().isoformat(),
                            "stock_tag": stock_code,
                            "source": "MoneyControl",
                            "display_date": date_str
                        }
                        
                        write_to_staging(msg)
                        print(f"[{stock_code}] Scraped: {headline[:50]}...")
                        count += 1
                        
                else:
                    print(f"⚠️ Failed to fetch {url}: Status {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Scrape Error for {stock_code}: {e}")
                
            # Short sleep between stocks to be polite
            time.sleep(1) 

        print("⏳ Waiting 3 minutes before next scrape cycle...")
        time.sleep(180)

if __name__ == "__main__":
    scrape_moneycontrol()