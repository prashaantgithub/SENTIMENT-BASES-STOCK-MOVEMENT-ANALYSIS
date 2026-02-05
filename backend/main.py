import os
import json
import glob
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from typing import List, Optional
import yfinance as yf

app = FastAPI(title="Stock Big Data API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_PATH = os.path.join(ROOT_DIR, "data")
PREDICTIONS_FILE = os.path.join(BASE_PATH, "latest_predictions.json")
PROCESSED_DATA_FILE = os.path.join(BASE_PATH, "processed_data", "processed_stocks.csv")
PLOT_DIR = os.path.join(ROOT_DIR, "eda", "plots")

# Mount EDA plots folder so Frontend can access images
if os.path.exists(PLOT_DIR):
    app.mount("/plots", StaticFiles(directory=PLOT_DIR), name="plots")

@app.get("/")
def health_check():
    return {"status": "ok", "message": "Big Data API is running"}

@app.get("/predictions")
def get_all_predictions():
    if not os.path.exists(PREDICTIONS_FILE):
        return []
    with open(PREDICTIONS_FILE, "r") as f:
        data = json.load(f)
    return data

@app.get("/news/{stock}")
def get_stock_news(stock: str):
    """Returns the latest records for this stock from the processed big data file."""
    try:
        if not os.path.exists(PROCESSED_DATA_FILE):
            return []
        df = pd.read_csv(PROCESSED_DATA_FILE)
        stock_data = df[df['Stock_Symbol'] == stock].sort_values(by='Date', ascending=False).head(10)
        return stock_data.to_dict(orient="records")
    except:
        return []

@app.get("/history/{stock}")
def get_stock_history(stock: str):
    try:
        ticker = f"{stock}.NS"
        df = yf.download(ticker, period="3mo", interval="1d", progress=False, auto_adjust=True)
        if df.empty: return []
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [str(c).lower() for c in df.columns]
        df = df.reset_index()
        date_col = next((c for c in df.columns if 'date' in c.lower() or 'index' in c.lower()), None)
        if not date_col: return []
        history = []
        for _, row in df.iterrows():
            history.append({
                "date": row[date_col].strftime('%Y-%m-%d'),
                "close": round(float(row['close']), 2),
                "volume": int(row['volume']) if 'volume' in row else 0
            })
        return history
    except Exception as e:
        print(f"Error fetching history: {e}")
        return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)