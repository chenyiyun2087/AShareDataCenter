import sys
import os
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.utils.notify import send_imessage

RECIPIENT = "+8618501372043"

print(f"Testing iMessage to {RECIPIENT}...")
try:
    send_imessage("Test message from AShareDataCenter debugging.", RECIPIENT)
    print("Function executed successfully.")
except Exception as e:
    print(f"Function failed with error: {e}")
