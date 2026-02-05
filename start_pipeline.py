import subprocess
import sys
import time
import os
import threading
import signal

# --- CONFIGURATION ---
PYTHON_EXEC = sys.executable  # Uses the current python environment
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Define paths to scripts
SCRIPT_MC = os.path.join(ROOT_DIR, "ingestion", "producer_moneycontrol.py")
SCRIPT_NEWS = os.path.join(ROOT_DIR, "ingestion", "producer_news.py")
SCRIPT_SPARK = os.path.join(ROOT_DIR, "processing", "spark_streaming.py")
SCRIPT_TRAIN = os.path.join(ROOT_DIR, "ml_pipeline", "train_model.py")
SCRIPT_PREDICT = os.path.join(ROOT_DIR, "ml_pipeline", "daily_prediction.py")
BACKEND_DIR = os.path.join(ROOT_DIR, "backend")

processes = []

def setup_environment():
    """Sets up global environment variables for PySpark and Java compatibility."""
    env = os.environ.copy()
    
    # 1. Point PySpark to the correct Python
    env['PYSPARK_PYTHON'] = sys.executable
    env['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # 2. Java 17+ Compatibility Flags
    # These flags allow Spark to access internal Java memory APIs blocked in newer JDKs.
    java_opens = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
    )
    # JAVA_TOOL_OPTIONS is automatically picked up by any JVM started
    env['JAVA_TOOL_OPTIONS'] = java_opens
    
    return env

def run_process(cmd_list, name, cwd=ROOT_DIR):
    """Helper to run a subprocess"""
    print(f"🚀 Starting {name}...")
    try:
        # Pass the modified environment to the subprocess
        p = subprocess.Popen(cmd_list, cwd=cwd, shell=False, env=setup_environment())
        processes.append(p)
        return p
    except Exception as e:
        print(f"❌ Failed to start {name}: {e}")

def ml_scheduler():
    """Runs ML Training and Prediction periodically"""
    print("⏳ ML Scheduler initializing...")
    
    # Visual countdown for the first run
    for i in range(60, 0, -10):
        print(f"⏳ ML Pipeline starting in {i} seconds (waiting for data)...")
        time.sleep(10)
    
    env = setup_environment()
    
    while True:
        print("\n🧠 --- RUNNING ML PIPELINE ---")
        try:
            # Run Training
            print("   Running Model Training...")
            subprocess.run([PYTHON_EXEC, SCRIPT_TRAIN], check=False, env=env)
            
            # Run Prediction
            print("   Running Daily Prediction...")
            subprocess.run([PYTHON_EXEC, SCRIPT_PREDICT], check=False, env=env)
            
            print("✅ ML Pipeline Finished. Next run in 10 minutes.\n")
        except Exception as e:
            print(f"❌ ML Pipeline Error: {e}")
            
        time.sleep(600)  # Run every 10 minutes

def cleanup(signum, frame):
    """Kills all processes on Ctrl+C"""
    print("\n🛑 Shutting down all services...")
    for p in processes:
        try:
            p.terminate()
            p.wait(timeout=2)
        except:
            p.kill()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    print("=================================================")
    print("   BIG DATA STOCK SENTIMENT SYSTEM - STARTUP     ")
    print("=================================================")

    # 1. Start Ingestion
    run_process([PYTHON_EXEC, SCRIPT_MC], "MoneyControl Scraper")
    run_process([PYTHON_EXEC, SCRIPT_NEWS], "News Producer")

    # 2. Start Spark Streaming
    run_process([PYTHON_EXEC, SCRIPT_SPARK], "Spark Streaming")

    # 3. Start API
    run_process([PYTHON_EXEC, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"], "FastAPI Backend", cwd=BACKEND_DIR)

    # 4. Start ML Scheduler
    ml_thread = threading.Thread(target=ml_scheduler, daemon=True)
    ml_thread.start()

    print("\n✅ SYSTEM IS LIVE!")
    print("-------------------------------------------------")
    print("Backend API: http://localhost:8000")
    print("Logs from all services will appear below.")
    print("Press Ctrl+C to stop everything.")
    print("-------------------------------------------------\n")

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()