import sys
import os

# --- FIX: Add project root to path ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import glob
import pandas as pd
import numpy as np
import xgboost as xgb
import yfinance as yf
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_PATH = os.path.join(os.getcwd(), "data")
MC_PATH = os.path.join(BASE_PATH, "processed_moneycontrol")
MODELS_DIR = os.path.join(os.getcwd(), "models")
PREDICTIONS_FILE = os.path.join(BASE_PATH, "latest_predictions.json")

# UPDATED STOCK LIST
STOCKS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS", 
          "SBIN.NS", "AXISBANK.NS", "HCLTECH.NS", "BHARTIARTL.NS", "WIPRO.NS"]

def get_latest_sentiment(stock_symbol):
    try:
        files = glob.glob(os.path.join(MC_PATH, "**/*.parquet"), recursive=True)
        if not files:
            return 0.0
            
        files = sorted(files, key=os.path.getmtime, reverse=True)[:5]
        
        df_list = []
        for f in files:
            try:
                df = pd.read_parquet(f)
                df_list.append(df)
            except:
                continue
                
        if not df_list:
            return 0.0
            
        full_df = pd.concat(df_list)
        
        stock_clean = stock_symbol.replace(".NS", "")
        if 'stock_tag' not in full_df.columns:
            return 0.0

        stock_df = full_df[full_df['stock_tag'] == stock_clean]
        
        if stock_df.empty:
            return 0.0
            
        avg_score = stock_df['sentiment_score'].mean()
        return float(avg_score) if not pd.isna(avg_score) else 0.0
        
    except Exception as e:
        print(f"⚠️ Error reading sentiment for {stock_symbol}: {e}")
        return 0.0

def generate_predictions():
    print("🔮 Starting Daily Prediction Pipeline...")
    
    model_path = os.path.join(MODELS_DIR, "xgboost_stock_model.json")
    if not os.path.exists(model_path):
        print("❌ Model not found! Please run train_model.py first.")
        return
    
    model = xgb.XGBClassifier()
    model.load_model(model_path)
    
    predictions = []
    
    for ticker in STOCKS:
        try:
            df = yf.download(ticker, period="3mo", progress=False, auto_adjust=True)
            
            if df.empty:
                print(f"⚠️ No price data for {ticker}")
                continue
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.columns = [str(c).lower() for c in df.columns]

            if 'close' not in df.columns:
                print(f"⚠️ 'close' column missing for {ticker}")
                continue
            
            df['ma_5'] = df['close'].rolling(window=5).mean()
            df['ma_10'] = df['close'].rolling(window=10).mean()
            df['volatility'] = df['close'].pct_change().rolling(window=5).std()
            
            df = df.dropna()

            if df.empty:
                print(f"⚠️ Not enough data to calculate indicators for {ticker}")
                continue

            latest = df.iloc[-1].copy()
            sentiment = get_latest_sentiment(ticker)
            
            features = pd.DataFrame([{
                'mc_sentiment': sentiment,
                'close': float(latest['close']),
                'ma_5': float(latest['ma_5']),
                'ma_10': float(latest['ma_10']),
                'volatility': float(latest['volatility'])
            }])
            
            features.fillna(0, inplace=True)
            
            prob = model.predict_proba(features)[0][1]
            prediction = "UP" if prob > 0.5 else "DOWN"
            
            result = {
                "stock": ticker.replace(".NS", ""),
                "current_price": round(float(latest['close']), 2),
                "prediction": prediction,
                "confidence": round(float(prob) * 100, 2),
                "sentiment_score": round(sentiment, 4),
                "timestamp": datetime.now().isoformat()
            }
            predictions.append(result)
            print(f"✅ {ticker}: {prediction} ({prob:.2f})")
            
        except Exception as e:
            print(f"❌ Failed to predict for {ticker}: {e}")

    with open(PREDICTIONS_FILE, "w") as f:
        json.dump(predictions, f, indent=4)
        
    print(f"💾 Predictions saved to {PREDICTIONS_FILE}")

if __name__ == "__main__":
    generate_predictions()