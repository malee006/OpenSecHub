import requests
import time
import datetime
import os
import signal
import sys
import json # Added for response parsing

# --- Configuration ---
# Read from environment variables, with defaults
ENRICH_AI_FUNCTION_URL = os.getenv('ENRICH_AI_FUNCTION_URL', 'https://oztlbsrmkzesflszmsem.supabase.co/functions/v1/enrich-ai')
SUPABASE_ANON_KEY = os.getenv('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im96dGxic3Jta3plc2Zsc3ptc2VtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDg2NTg2MTcsImV4cCI6MjA2NDIzNDYxN30.lja2lGa9t6SNkhdLOidPLK3geSX12WUGkCkN_E9Fj00')

# How often to call the enrich-ai function (in seconds)
CALL_INTERVAL_SECONDS = int(os.getenv('CALL_INTERVAL_SECONDS', 300))  # Default to 5 minutes
# Total duration the script should run (in hours)
TOTAL_RUN_DURATION_HOURS = int(os.getenv('TOTAL_RUN_DURATION_HOURS', 1)) # Default to 1 hour

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print(f"\n[{datetime.datetime.now()}] Shutdown signal received. Finishing current operation and exiting...")
    shutdown_requested = True

def validate_config():
    """Validate that all required configuration is present"""
    if not ENRICH_AI_FUNCTION_URL or 'oztlbsrmkzesflszmsem' not in ENRICH_AI_FUNCTION_URL: # Basic check for default
        print(f"ERROR: ENRICH_AI_FUNCTION_URL is not configured correctly or is still the default. Current value: {ENRICH_AI_FUNCTION_URL}")
        return False
    if not SUPABASE_ANON_KEY or 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9' not in SUPABASE_ANON_KEY: # Basic check for default
        print(f"ERROR: SUPABASE_ANON_KEY is not configured correctly or is still the default. Current value: {SUPABASE_ANON_KEY[:20]}...")
        return False
    if CALL_INTERVAL_SECONDS <= 0:
        print(f"ERROR: CALL_INTERVAL_SECONDS must be a positive integer. Current value: {CALL_INTERVAL_SECONDS}")
        return False
    if TOTAL_RUN_DURATION_HOURS < 0: # 0 means run indefinitely if logic supports it, but negative is invalid
        print(f"ERROR: TOTAL_RUN_DURATION_HOURS must be a non-negative integer. Current value: {TOTAL_RUN_DURATION_HOURS}")
        return False
    return True

def invoke_enrich_ai_function():
    """
    Makes an HTTP POST request to the Supabase Edge Function 'enrich-ai'.
    """
    print(f"[{datetime.datetime.now()}] Attempting to call AI Enrichment Edge Function: {ENRICH_AI_FUNCTION_URL}")
    
    headers = {
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Content-Type': 'application/json', 
        'apikey': SUPABASE_ANON_KEY 
    }
    
    try:
        response = requests.post(ENRICH_AI_FUNCTION_URL, json={}, headers=headers, timeout=45) 
        response.raise_for_status()  
        
        print(f"[{datetime.datetime.now()}] AI Enrichment Function call successful! Status Code: {response.status_code}")
        try:
            response_json = response.json()
            print(f"Response JSON: {json.dumps(response_json, indent=2)}")
            if response_json.get("message") == "No pending tools found to process.":
                 print(f"[{datetime.datetime.now()}] Edge function reported no pending tools.")
        except json.JSONDecodeError:
            print(f"Response Text (not JSON): {response.text}")
        return True
    except requests.exceptions.Timeout:
        print(f"[{datetime.datetime.now()}] Error calling function: Request timed out after 45 seconds.")
        return False
    except requests.exceptions.HTTPError as http_err:
        print(f"[{datetime.datetime.now()}] HTTP error calling function: {http_err}")
        try:
            print(f"Error Response: {http_err.response.text}")
        except Exception:
            pass 
        return False
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.datetime.now()}] General error calling function: {e}")
        return False
    except Exception as e:
        print(f"[{datetime.datetime.now()}] An unexpected error occurred during function invocation: {e}")
        return False

def main():
    """
    Main loop to run the AI Enrichment function periodically.
    """
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if not validate_config():
        print(f"[{datetime.datetime.now()}] Configuration validation failed. Exiting.")
        sys.exit(1)
    
    start_time = time.time()
    # If TOTAL_RUN_DURATION_HOURS is 0, run indefinitely
    end_time = start_time + (TOTAL_RUN_DURATION_HOURS * 3600) if TOTAL_RUN_DURATION_HOURS > 0 else float('inf')

    print(f"[{datetime.datetime.now()}] Starting AI Enrichment Scheduler...")
    print(f"Edge Function URL: {ENRICH_AI_FUNCTION_URL}")
    if TOTAL_RUN_DURATION_HOURS > 0:
        print(f"Script will run for approximately {TOTAL_RUN_DURATION_HOURS} hour(s).")
    else:
        print("Script will run indefinitely (TOTAL_RUN_DURATION_HOURS is 0 or less).")
    print(f"Calling AI Enrichment function every {CALL_INTERVAL_SECONDS} seconds ({(CALL_INTERVAL_SECONDS/60.0):.1f} minutes).")
    print("Press Ctrl+C to stop gracefully.\n")

    try:
        while time.time() < end_time and not shutdown_requested:
            invoke_enrich_ai_function()
            
            if shutdown_requested:
                print(f"[{datetime.datetime.now()}] Shutdown initiated, breaking loop.")
                break
            
            current_time_for_check = time.time()
            if current_time_for_check >= end_time and TOTAL_RUN_DURATION_HOURS > 0:
                print(f"[{datetime.datetime.now()}] Run duration completed before sleep interval.")
                break

            print(f"[{datetime.datetime.now()}] Sleeping for {CALL_INTERVAL_SECONDS} seconds...")
            
            sleep_remaining = CALL_INTERVAL_SECONDS
            while sleep_remaining > 0 and not shutdown_requested:
                current_time_for_sleep_check = time.time()
                if current_time_for_sleep_check >= end_time and TOTAL_RUN_DURATION_HOURS > 0:
                    break 
                
                time_until_end_for_sleep = (end_time - current_time_for_sleep_check) if TOTAL_RUN_DURATION_HOURS > 0 else float('inf')
                chunk_sleep = min(sleep_remaining, 10, time_until_end_for_sleep) 
                
                if chunk_sleep <= 0: 
                    break

                time.sleep(chunk_sleep)
                sleep_remaining -= chunk_sleep
        
        if not shutdown_requested and TOTAL_RUN_DURATION_HOURS > 0 and time.time() >= end_time:
            print(f"[{datetime.datetime.now()}] Total run duration of {TOTAL_RUN_DURATION_HOURS} hour(s) completed.")

    except Exception as e:
        print(f"[{datetime.datetime.now()}] A critical error occurred in the main loop: {e}")
    finally:
        print(f"[{datetime.datetime.now()}] AI Enrichment Scheduler finished.")

if __name__ == "__main__":
    main()
