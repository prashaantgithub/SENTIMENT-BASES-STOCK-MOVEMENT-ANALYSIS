import os
from dotenv import load_dotenv

# Load .env
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

# --- DIRECTORY SETUP (File-Based Streaming) ---
BASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

# We now use 'moneycontrol' instead of 'tweets'
STAGING_MONEYCONTROL = os.path.join(BASE_DIR, 'staging', 'moneycontrol')
STAGING_NEWS = os.path.join(BASE_DIR, 'staging', 'news')

# Create directories if they don't exist
os.makedirs(STAGING_MONEYCONTROL, exist_ok=True)
os.makedirs(STAGING_NEWS, exist_ok=True)

# API Keys
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

# Stock List
STOCKS_STR = os.getenv("STOCKS", "RELIANCE,TCS,INFY,HDFCBANK,ICICIBANK,TATAMOTORS,AXISBANK,HCLTECH,BHARTIARTL,WIPRO")
STOCKS_LIST = [s.strip() for s in STOCKS_STR.split(",")]