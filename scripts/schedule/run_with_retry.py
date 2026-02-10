#!/usr/bin/env python3
import argparse
import subprocess
import time
import sys
import os
from datetime import datetime

def send_notification(title, message, is_error=False):
    """Send desktop notification on macOS using osascript."""
    sound = "Basso" if is_error else "Glass"
    script = f'display notification "{message}" with title "{title}" sound name "{sound}"'
    try:
        subprocess.run(["osascript", "-e", script], check=True)
    except Exception as e:
        print(f"Failed to send notification: {e}")

def run_command(cmd, max_retries=3, delay=60):
    """Run command with retries."""
    attempt = 0
    while attempt <= max_retries:
        print(f"\n[Attempt {attempt+1}/{max_retries+1}] Running: {' '.join(cmd)}")
        start_time = datetime.now()
        
        try:
            subprocess.check_call(cmd)
            duration = datetime.now() - start_time
            print(f"SUCCESS: Completed in {duration}")
            send_notification("AShare Pipeline Success", f"Task completed successfully in {duration}")
            return 0
        except subprocess.CalledProcessError as e:
            print(f"FAILED: Exit code {e.returncode}")
            attempt += 1
            if attempt <= max_retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Task failed.")
                send_notification("AShare Pipeline FAILED", f"Task failed after {max_retries+1} attempts.", is_error=True)
                return e.returncode

def main():
    parser = argparse.ArgumentParser(description="Run command with retry and notification.")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command to run")
    parser.add_argument("--retries", type=int, default=3, help="Max retry attempts")
    parser.add_argument("--delay", type=int, default=300, help="Delay between retries in seconds")
    
    args = parser.parse_args()
    
    if not args.command:
        print("Error: No command provided.")
        sys.exit(1)
        
    # Remove leading '--' if present (argparse.REMAINDER includes it)
    if args.command and args.command[0] == "--":
        args.command.pop(0)

    if not args.command:
        print("Error: No command provided after '--'.")
        sys.exit(1)
        
    cmd = args.command
    
    sys.exit(run_command(cmd, args.retries, args.delay))

if __name__ == "__main__":
    main()
