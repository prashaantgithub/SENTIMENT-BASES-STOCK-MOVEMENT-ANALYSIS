import sys
import os

# --- FIX: Add project root to path ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import glob
import pandas as pd
import numpy as np
import xgboost as xgb
import yfinance as yf
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

# --- CONFIGURATION ---
BASE_PATH = os.path.join(os.getcwd(), "data")
MC_PATH = os.path.join(BASE_PATH, "processed_moneycontrol")
NEWS_PATH = os.path.join(BASE_PATH, "processed_news")
MODELS_DIR = os.path.join(os.getcwd(), "models")

# UPDATED STOCK LIST (Replaced TATAMOTORS.NS with SBIN.NS)
STOCKS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS", 
          "SBIN.NS", "AXISBANK.NS", "HCLTECH.NS", "BHARTIARTL.NS", "WIPRO.NS"]

def load_parquet_data(path):
    """Loads parquet data from the directory if it exists."""
    try:
        files = glob.glob(os.path.join(path, "**/*.parquet"), recursive=True)
        if not files:
            return pd.DataFrame()
        
        df_list = [pd.read_parquet(f) for f in files]
        full_df = pd.concat(df_list, ignore_index=True)
        return full_df
    except Exception as e:
        print(f"⚠️ Could not load data from {path}: {e}")
        return pd.DataFrame()

def download_stock_prices():
    """Downloads last 2 years of stock prices."""
    print("📉 Downloading historical stock prices...")
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    all_data = []
    for ticker in STOCKS:
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
            
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

                df = df.reset_index()
                df.columns = [str(c).lower() for c in df.columns]
                
                if 'date' not in df.columns and 'index' in df.columns:
                    df.rename(columns={'index': 'date'}, inplace=True)
                
                if 'close' in df.columns:
                    df['stock'] = ticker.replace(".NS", "")
                    all_data.append(df[['date', 'close', 'stock']])
        except Exception as e:
            print(f"❌ Error fetching {ticker}: {e}")
            
    if all_data:
        return pd.concat(all_data)
    return pd.DataFrame()

def generate_synthetic_data():
    """Generates dummy data if real streaming data is insufficient for training."""
    print("⚠️ Generating SYNTHETIC TRAINING DATA (for pipeline verification)...")
    dates = pd.date_range(end=datetime.now(), periods=100).tolist()
    data = []
    
    for stock in [s.replace(".NS","") for s in STOCKS]:
        for d in dates:
            row = {
                "date": d,
                "stock": stock,
                "mc_sentiment": np.random.uniform(-0.5, 0.8),
                "close": np.random.uniform(500, 3000),
                "ma_5": np.random.uniform(500, 3000),
                "ma_10": np.random.uniform(500, 3000),
                "volatility": np.random.uniform(0.01, 0.05),
                "target": np.random.randint(0, 2)
            }
            data.append(row)
    
    return pd.DataFrame(data)

def train_pipeline():
    print("🚀 Starting ML Training Pipeline...")
    
    mc_df = load_parquet_data(MC_PATH)
    sentiment_df = pd.DataFrame()
    
    if not mc_df.empty and 'date' in mc_df.columns:
         mc_agg = mc_df.groupby(['date', 'stock_tag'])['sentiment_score'].mean().reset_index()
         mc_agg.rename(columns={'stock_tag': 'stock', 'sentiment_score': 'mc_sentiment'}, inplace=True)
         sentiment_df = mc_agg

    prices_df = download_stock_prices()
    
    if prices_df.empty or (sentiment_df.empty and len(prices_df) < 50):
        final_df = generate_synthetic_data()
    else:
        prices_df['date'] = pd.to_datetime(prices_df['date'])
        
        if not sentiment_df.empty:
            sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])
            final_df = pd.merge(prices_df, sentiment_df, on=['date', 'stock'], how='left')
            final_df['mc_sentiment'] = final_df['mc_sentiment'].fillna(0)
        else:
            final_df = prices_df
            final_df['mc_sentiment'] = 0

        final_df['ma_5'] = final_df.groupby('stock')['close'].transform(lambda x: x.rolling(window=5).mean())
        final_df['ma_10'] = final_df.groupby('stock')['close'].transform(lambda x: x.rolling(window=10).mean())
        final_df['volatility'] = final_df.groupby('stock')['close'].transform(lambda x: x.pct_change().rolling(window=5).std())
        
        final_df['next_close'] = final_df.groupby('stock')['close'].shift(-1)
        final_df['target'] = (final_df['next_close'] > final_df['close']).astype(int)
        
        final_df.dropna(inplace=True)

    if final_df.empty:
        print("❌ Not enough data to train. Exiting.")
        return

    print(f"📊 Training Data Shape: {final_df.shape}")

    features = ['mc_sentiment', 'close', 'ma_5', 'ma_10', 'volatility']
    
    for f in features:
        if f not in final_df.columns:
            final_df[f] = 0
            
    X = final_df[features]
    y = final_df['target']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = xgb.XGBClassifier(use_label_encoder=False, eval_metric='logloss')
    model.fit(X_train, y_train)
    
    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    print(f"✅ Model Trained. Accuracy: {acc:.4f}")
    
    if not os.path.exists(MODELS_DIR):
        os.makedirs(MODELS_DIR)
        
    model_path = os.path.join(MODELS_DIR, "xgboost_stock_model.json")
    model.save_model(model_path)
    print(f"💾 Model saved to: {model_path}")

if __name__ == "__main__":
    train_pipeline()