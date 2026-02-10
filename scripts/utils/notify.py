import subprocess
import logging

def send_imessage(message: str, recipient: str) -> None:
    """
    Sends an iMessage to the specified recipient (phone number or email).
    
    Args:
        message (str): The body of the message.
        recipient (str): The phone number or email address of the recipient.
    """
    if not recipient:
        logging.warning("No recipient provided for iMessage notification.")
        return

    # Escape double quotes in the message to avoid breaking the AppleScript
    safe_message = message.replace('"', '\\"')
    safe_recipient = recipient.replace('"', '\\"')

    # AppleScript command
    # Explicitly use the iMessage service to ensure we target the phone number correctly
    scpt = f'''
    tell application "Messages"
        set targetService to 1st service whose service type = iMessage
        set targetBuddy to buddy "{safe_recipient}" of targetService
        send "{safe_message}" to targetBuddy
    end tell
    '''
    
    try:
        # Run osascript
        result = subprocess.run(["osascript", "-e", scpt], check=True, capture_output=True)
        if result.stderr:
            logging.warning(f"iMessage stderr: {result.stderr.decode('utf-8')}")
        logging.info(f"iMessage sent to {recipient}")
    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.decode('utf-8') if e.stderr else str(e)
        logging.error(f"Failed to send iMessage to {recipient}: {err_msg}")
        print(f"DEBUG: osascript error: {err_msg}") # Add direct print for debugging
    except Exception as e:
        logging.error(f"Unexpected error sending iMessage: {str(e)}")
