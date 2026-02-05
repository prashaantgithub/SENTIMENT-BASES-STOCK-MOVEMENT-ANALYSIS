import subprocess
import sys
import os
import time

PYTHON_EXEC = sys.executable
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

def run_step(script_path, name):
    print(f"\n--- [STEP: {name}] ---")
    script_full_path = os.path.join(ROOT_DIR, script_path)
    result = subprocess.run([PYTHON_EXEC, script_full_path], capture_output=False)
    if result.returncode != 0:
        print(f"❌ {name} failed. Exiting.")
        sys.exit(1)
    print(f"✅ {name} completed.")

def main():
    print("=================================================")
    print("   STOCK SENTIMENT BIG DATA FULL-STACK APP       ")
    print("=================================================")

    # 1. Ingestion (Step 1-3)
    run_step("ingestion/load_data.py", "Data Ingestion & Preprocessing")

    # 2. Big Data Processing (Step 4)
    run_step("processing/pyspark_processor.py", "Big Data Feature Engineering")

    # 3. EDA & Insights (Step 5-6)
    run_step("eda/data_analysis.py", "Exploratory Data Analysis")

    # 4. Model Training (Step 7)
    run_step("ml_pipeline/model_training.py", "ML Model Training & Prediction")

    # 5. Start Backend API
    print("\n🚀 All Big Data steps complete. Launching Backend API...")
    backend_dir = os.path.join(ROOT_DIR, "backend")
    
    # We use subprocess.Popen for the backend to keep it running
    try:
        subprocess.run([PYTHON_EXEC, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"], cwd=backend_dir)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")

if __name__ == "__main__":
    main()