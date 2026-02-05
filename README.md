Sentiment-Based Stock Movement Analysis
Project Overview
This Big Data Analytics (BDA) project provides an end-to-end pipeline for predicting price movements of 10 high-volume Indian stocks across Sensex and Nifty 50 indices. The system integrates historical market data with simulated news sentiment analysis to categorize potential market directions as UP or DOWN.
Problem Statement
Predicting stock market trends is challenging due to the high volatility and noise in financial data. This project solves the problem by processing over 12,000 historical records and combining technical indicators with news-based sentiment to assist in data-driven investment decision-making.
Key Features
Automated Data Ingestion: Fetches 5 years of real-world OHLCV data and news headlines using the Yahoo Finance API.
Big Data Processing: Utilizes high-performance processing engines (PySpark/Polars) to handle 10,000+ records and perform complex feature engineering.
Sentiment Integration: Incorporates VADER sentiment analysis to quantify the impact of news on stock price volatility.
Machine Learning: Implements an XGBoost classification model to predict next-day price direction with confidence scores.
Interactive Dashboard: A full-stack React and FastAPI application providing real-time technical charts and Exploratory Data Analysis (EDA) insights.
Tech Stack
Frontend: React.js, Material UI, Recharts
Backend: FastAPI, Uvicorn
Big Data: PySpark, Polars
Machine Learning: XGBoost, Scikit-learn
Database: SQLite3, Parquet
Data Visualization: Matplotlib, Seaborn
Project Workflow
Step 1: Problem Definition and project scoping.
Step 2: Data Understanding through the analysis of 10 specific Indian stock symbols.
Step 3: Data Preprocessing including duplicate removal, missing value handling, and type conversion.
Step 4: Big Data Processing involving filtering, aggregations, and window-based feature transformations.
Step 5: Exploratory Data Analysis (EDA) generating correlation heatmaps and distribution plots.
Step 6: Insight Generation based on statistical patterns and correlation between sentiment and returns.
Step 7: Model Building using gradient boosting for direction classification.
Folder Hierarchy
code
Text
CIA 3/
├── backend/
│   ├── main.py               (FastAPI Endpoints)
│   └── api.py                (Internal API logic)
├── data/
│   ├── processed_data/       (PySpark/Polars output)
│   ├── latest_predictions.json
│   ├── raw_stocks_10000.csv
│   └── stocks_data.db        (SQLite Database)
├── eda/
│   ├── plots/                (Generated EDA Images)
│   └── data_analysis.py      (Visualization Script)
├── frontend/
│   ├── public/
│   ├── src/
│   │   ├── components/       (UI Components)
│   │   ├── Dashboard.js      (Main Dashboard Logic)
│   │   └── api.js            (Frontend API bridge)
│   └── package.json
├── ingestion/
│   └── load_data.py          (Data Fetching & SQLite Load)
├── ml_pipeline/
│   └── model_training.py     (XGBoost Training Script)
├── models/
│   └── stock_model.json      (Trained ML Model)
├── processing/
│   └── pyspark_processor.py  (Big Data Engine)
├── start_app.py              (Orchestrator Script)
└── requirements.txt
Installation and Setup
Install Python dependencies:
pip install -r requirements.txt
Run the full-stack orchestrator (Executes Step 1 to 7 and starts Backend):
python start_app.py
Start the React Frontend:
cd frontend
npm install
npm start
View the dashboard at http://localhost:3000
