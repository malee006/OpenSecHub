import requests
import time
import datetime
import os
import signal
import sys

# --- Configuration ---
FUNCTION_URL = os.getenv('FUNCTION_URL')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY')
SLEEP_INTERVAL_SECONDS = 180  # 1 hour
RUN_DURATION_HOURS = 8  # 8 hours

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print(f"\n[{datetime.datetime.now()}] Shutdown signal received. Finishing current operation...")
    shutdown_requested = True

def validate_config():
    """Validate that all required configuration is present"""
    if not FUNCTION_URL:
        print("ERROR: FUNCTION_URL not configured")
        return False
    
    if not SUPABASE_ANON_KEY:
        print("ERROR: SUPABASE_ANON_KEY not configured")
        return False
    
    return True

def run_github_sync_function():
    """
    Makes an HTTP POST request to the Supabase Edge Function.
    """
    print(f"[{datetime.datetime.now()}] Attempting to call Edge Function: {FUNCTION_URL}")
    
    headers = {
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Content-Type': 'application/json',
        'apikey': SUPABASE_ANON_KEY
    }
    
    try:
        response = requests.post(FUNCTION_URL, json={}, headers=headers, timeout=30)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        print(f"[{datetime.datetime.now()}] Function call successful! Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.datetime.now()}] Error calling function: {e}")
        return False
    except Exception as e:
        print(f"[{datetime.datetime.now()}] An unexpected error occurred: {e}")
        return False

def main():
    """
    Main loop to run the function repeatedly.
    """
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Validate configuration
    if not validate_config():
        sys.exit(1)
    
    start_time = time.time()
    end_time = start_time + (RUN_DURATION_HOURS * 3600) if RUN_DURATION_HOURS else float('inf')

    print(f"Starting GitHub Sync Scheduler...")
    print(f"Function URL: {FUNCTION_URL}")
    if RUN_DURATION_HOURS:
        print(f"Script will run for approximately {RUN_DURATION_HOURS} hours.")
    else:
        print("Script will run indefinitely (until manually stopped).")
    print(f"Calling function every {SLEEP_INTERVAL_SECONDS} seconds.")
    print("Press Ctrl+C to stop gracefully.\n")

    while time.time() < end_time and not shutdown_requested:
        success = run_github_sync_function()
        
        if shutdown_requested:
            break
            
        if time.time() < end_time and not shutdown_requested:
            print(f"[{datetime.datetime.now()}] Sleeping for {SLEEP_INTERVAL_SECONDS} seconds...")
            
            # Sleep in smaller chunks to allow for more responsive shutdown
            sleep_remaining = SLEEP_INTERVAL_SECONDS
            while sleep_remaining > 0 and not shutdown_requested:
                chunk_sleep = min(sleep_remaining, 10)  # Sleep in 10-second chunks
                time.sleep(chunk_sleep)
                sleep_remaining -= chunk_sleep
        else:
            if not shutdown_requested:
                print(f"[{datetime.datetime.now()}] Run duration completed. Exiting.")

    print(f"[{datetime.datetime.now()}] Script finished.")

if __name__ == "__main__":
    main()
