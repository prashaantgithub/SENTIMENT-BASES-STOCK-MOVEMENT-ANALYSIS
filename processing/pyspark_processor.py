import os
import polars as pl
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_INPUT = os.path.join(BASE_DIR, "data", "raw_stocks_10000.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed_data")

def run_big_data_processing():
    print("🚀 Step 4: Big Data Processing...")
    q = pl.scan_csv(CSV_INPUT)
    q = q.filter(pl.col("Volume") > 50000)

    q_transformed = q.with_columns([
        pl.col("Close").rolling_mean(window_size=10).over("Stock_Symbol").alias("MA_10"),
        pl.col("Sentiment_Score").shift(1).over("Stock_Symbol").alias("Prev_Day_Sentiment"),
        ((pl.col("High") - pl.col("Low")) / pl.col("Close")).alias("Volatility"),
        (((pl.col("Close") - pl.col("Close").shift(1).over("Stock_Symbol")) / pl.col("Close").shift(1).over("Stock_Symbol")) * 100).alias("Daily_Return")
    ])

    df_final = q_transformed.sort(["Stock_Symbol", "Date"]).drop_nulls().collect()

    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    
    output_file = os.path.join(OUTPUT_DIR, "processed_stocks.csv")
    df_final.write_csv(output_file)
    print(f"✨ Step 4 Complete. Processed data saved.")

if __name__ == "__main__":
    run_big_data_processing()