import os
import pandas as pd
import xgboost as xgb
import json
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, "data", "processed_data", "processed_stocks.csv")
MODELS_DIR = os.path.join(BASE_DIR, "models")
PREDICTIONS_FILE = os.path.join(BASE_DIR, "data", "latest_predictions.json")

def train_and_predict():
    print("🚀 Step 7: Training Classification Model (XGBoost)...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: Processed data not found at {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    
    features = ['MA_10', 'Sentiment_Score', 'Volatility', 'Close', 'Daily_Return']
    X = df[features]
    y = df['Target']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = xgb.XGBClassifier(eval_metric='logloss')
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    acc = accuracy_score(y_test, preds)
    print(f"✅ Model Trained. Accuracy: {acc:.4f}")

    os.makedirs(MODELS_DIR, exist_ok=True)
    model.save_model(os.path.join(MODELS_DIR, "stock_model.json"))

    print("🔮 Generating latest predictions for dashboard...")
    latest_preds = []
    for stock in df['Stock_Symbol'].unique():
        stock_data = df[df['Stock_Symbol'] == stock].iloc[-1:]
        
        prob = model.predict_proba(stock_data[features])[0][1]
        pred_label = "UP" if prob > 0.5 else "DOWN"
        
        latest_preds.append({
            "stock": stock,
            "current_price": round(float(stock_data['Close'].iloc[0]), 2),
            "prediction": pred_label,
            "confidence": round(float(max(prob, 1-prob)) * 100, 2),
            "sentiment_score": round(float(stock_data['Sentiment_Score'].iloc[0]), 4),
            "timestamp": datetime.now().isoformat()
        })

    with open(PREDICTIONS_FILE, "w") as f:
        json.dump(latest_preds, f, indent=4)
    
    print(f"💾 Dashboard predictions saved to {PREDICTIONS_FILE}")

if __name__ == "__main__":
    train_and_predict()