import os
import sys
import time
import json
import glob
import pandas as pd
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- CONFIGURATION ---
BASE_PATH = os.path.join(os.getcwd(), "data")
STAGING_MC = os.path.join(BASE_PATH, "staging", "moneycontrol")
STAGING_NEWS = os.path.join(BASE_PATH, "staging", "news")

# Output Paths
MC_OUTPUT_PATH = os.path.join(BASE_PATH, "processed_moneycontrol")
NEWS_OUTPUT_PATH = os.path.join(BASE_PATH, "processed_news")
ARCHIVE_PATH = os.path.join(BASE_PATH, "archive")

# Ensure directories exist
for path in [STAGING_MC, STAGING_NEWS, MC_OUTPUT_PATH, NEWS_OUTPUT_PATH, ARCHIVE_PATH]:
    os.makedirs(path, exist_ok=True)

analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    if not text:
        return 0.0
    return float(analyzer.polarity_scores(text)['compound'])

def process_files(source_dir, output_dir, file_type):
    """
    Reads JSON files from source_dir, applies sentiment, saves to output_dir (Parquet),
    and moves raw files to archive.
    """
    # Find all JSON files
    files = glob.glob(os.path.join(source_dir, "*.json"))
    
    if not files:
        return
    
    data_buffer = []
    processed_files = []

    print(f"🔄 Processing {len(files)} new files from {os.path.basename(source_dir)}...")

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                record = json.load(f)
            
            # Extract text for sentiment
            if file_type == "moneycontrol":
                text = record.get("text", "") or record.get("title", "")
                date_str = record.get("created_at", datetime.now().isoformat())
            else: # news
                text = record.get("title", "")
                date_str = record.get("published_at", datetime.now().isoformat())

            # Apply Sentiment
            record['sentiment_score'] = get_sentiment(text)
            
            # Normalize Date for Partitioning
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                record['date'] = dt.strftime('%Y-%m-%d')
            except:
                record['date'] = datetime.now().strftime('%Y-%m-%d')
            
            data_buffer.append(record)
            processed_files.append(file_path)
            
        except Exception as e:
            print(f"⚠️ Error reading {file_path}: {e}")

    # Save to Parquet
    if data_buffer:
        df = pd.DataFrame(data_buffer)
        
        # Partition by Date
        for date_key, group in df.groupby('date'):
            partition_dir = os.path.join(output_dir, f"date={date_key}")
            os.makedirs(partition_dir, exist_ok=True)
            
            # Save file
            filename = f"part-{int(time.time())}.parquet"
            save_path = os.path.join(partition_dir, filename)
            group.to_parquet(save_path, index=False)
            print(f"✅ Saved batch to {save_path}")

    # Move processed files to archive (prevent re-reading)
    for file_path in processed_files:
        try:
            filename = os.path.basename(file_path)
            archive_subdir = os.path.join(ARCHIVE_PATH, os.path.basename(source_dir))
            os.makedirs(archive_subdir, exist_ok=True)
            os.replace(file_path, os.path.join(archive_subdir, filename))
        except:
            pass

def run_streaming():
    print("=================================================")
    print("   NATIVE PYTHON SENTIMENT STREAMING ACTIVE      ")
    print("=================================================")
    print("🚀 Watching 'data/staging' for new news...")
    
    while True:
        try:
            process_files(STAGING_MC, MC_OUTPUT_PATH, "moneycontrol")
            process_files(STAGING_NEWS, NEWS_OUTPUT_PATH, "news")
            time.sleep(5) # Poll every 5 seconds
        except Exception as e:
            print(f"❌ Processing Loop Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_streaming()