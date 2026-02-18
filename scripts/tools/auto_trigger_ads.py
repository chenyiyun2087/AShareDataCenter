import os
import time
import subprocess
import datetime

def run_ads_stage(start_date, end_date):
    print(f"Starting ADS Stage 5 from {start_date} to {end_date}...")
    cmd = [
        "python", "scripts/sync/run_ads.py",
        "--mode", "incremental",
        "--start-date", str(start_date),
        "--end-date", str(end_date),
        "--chunk-by", "month",
        "--workers", "4",
        "--config", "config/etl.ini"
    ]
    
    with open("/tmp/ads_sync.log", "w") as log_file:
        process = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONPATH": f"{os.environ.get('PYTHONPATH', '')}:{os.getcwd()}/scripts"}
        )
        print(f"ADS sync started with PID {process.pid}. Logs at /tmp/ads_sync.log")
        return process

def monitor_dws_and_trigger_ads():
    dws_log = "/tmp/dws_sync_sequential.log"
    print(f"Monitoring DWS log: {dws_log}")
    
    # Wait for DWS to finish
    while True:
        try:
            # Check if run_dws.py is still running
            result = subprocess.run(["pgrep", "-f", "run_dws.py"], capture_output=True)
            if result.returncode != 0:
                print("DWS process not found. It may have finished.")
                break
            
            # optional: tail log to see progress
            # subprocess.run(["tail", "-n", "1", dws_log])
            
            time.sleep(60)
        except KeyboardInterrupt:
            print("Monitoring stopped.")
            return

    print("DWS Stage 4 completed. Triggering ADS Stage 5...")
    run_ads_stage(20200101, 20260217)

if __name__ == "__main__":
    monitor_dws_and_trigger_ads()
