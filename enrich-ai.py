import requests
import time
import datetime
import os
import signal
import sys

# --- Configuration ---
# IMPORTANT: Replace with your actual Supabase Edge Function URL for 'enrich-ai'
ENRICH_AI_FUNCTION_URL = 'https://oztlbsrmkzesflszmsem.supabase.co/functions/v1/enrich-ai'
# IMPORTANT: Replace with your actual Supabase Anon Key.
# Best practice is to use environment variables for secrets.
SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im96dGxic3Jta3plc2Zsc3ptc2VtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDg2NTg2MTcsImV4cCI6MjA2NDIzNDYxN30.lja2lGa9t6SNkhdLOidPLK3geSX12WUGkCkN_E9Fj00'

# How often to call the enrich-ai function (e.g., every 5 minutes)
CALL_INTERVAL_SECONDS = 300  # 5 minutes * 60 seconds
# Total duration the script should run (e.g., 1 hour)
TOTAL_RUN_DURATION_HOURS = 1

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_requested
    print(f"\n[{datetime.datetime.now()}] Shutdown signal received. Finishing current operation and exiting...")
    shutdown_requested = True

def validate_config():
    """Validate that all required configuration is present"""
    if not ENRICH_AI_FUNCTION_URL or 'your_supabase_project_id' in ENRICH_AI_FUNCTION_URL: # Basic check
        print("ERROR: ENRICH_AI_FUNCTION_URL not configured correctly. Please update it.")
        return False
    if not SUPABASE_ANON_KEY or 'YOUR_SUPABASE_ANON_KEY' in SUPABASE_ANON_KEY: # Basic check
        print("ERROR: SUPABASE_ANON_KEY not configured correctly. Please update it.")
        return False
    return True

def invoke_enrich_ai_function():
    """
    Makes an HTTP POST request to the Supabase Edge Function 'enrich-ai'.
    """
    print(f"[{datetime.datetime.now()}] Attempting to call AI Enrichment Edge Function: {ENRICH_AI_FUNCTION_URL}")
    
    headers = {
        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
        'Content-Type': 'application/json', # Edge functions usually expect a body, even if empty
        'apikey': SUPABASE_ANON_KEY # Supabase often requires apikey in header too
    }
    
    try:
        # Edge functions are typically invoked with POST, even if no significant body is sent
        response = requests.post(ENRICH_AI_FUNCTION_URL, json={}, headers=headers, timeout=45) # Increased timeout slightly
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        
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
            pass # Ignore if can't get error response text
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
        sys.exit(1)
    
    start_time = time.time()
    # If TOTAL_RUN_DURATION_HOURS is 0 or None, run indefinitely
    end_time = start_time + (TOTAL_RUN_DURATION_HOURS * 3600) if TOTAL_RUN_DURATION_HOURS else float('inf')

    print(f"Starting AI Enrichment Scheduler...")
    print(f"Edge Function URL: {ENRICH_AI_FUNCTION_URL}")
    if TOTAL_RUN_DURATION_HOURS:
        print(f"Script will run for approximately {TOTAL_RUN_DURATION_HOURS} hour(s).")
    else:
        print("Script will run indefinitely (until manually stopped).")
    print(f"Calling AI Enrichment function every {CALL_INTERVAL_SECONDS} seconds ({(CALL_INTERVAL_SECONDS/60):.1f} minutes).")
    print("Press Ctrl+C to stop gracefully.\n")

    try:
        while time.time() < end_time and not shutdown_requested:
            invoke_enrich_ai_function()
            
            if shutdown_requested:
                print(f"[{datetime.datetime.now()}] Shutdown initiated, breaking loop.")
                break
            
            # Check if it's time to stop before sleeping
            if time.time() >= end_time and TOTAL_RUN_DURATION_HOURS:
                print(f"[{datetime.datetime.now()}] Run duration completed before sleep interval.")
                break

            print(f"[{datetime.datetime.now()}] Sleeping for {CALL_INTERVAL_SECONDS} seconds...")
            
            # Sleep in smaller chunks to allow for more responsive shutdown
            sleep_remaining = CALL_INTERVAL_SECONDS
            while sleep_remaining > 0 and not shutdown_requested:
                # Ensure we don't sleep past the total end time
                current_time = time.time()
                if current_time >= end_time and TOTAL_RUN_DURATION_HOURS:
                    break 
                
                # Determine how long to sleep in this chunk
                time_until_end = (end_time - current_time) if TOTAL_RUN_DURATION_HOURS else float('inf')
                chunk_sleep = min(sleep_remaining, 10, time_until_end) # Sleep in 10-second chunks or less
                
                if chunk_sleep <= 0: # No more time left to sleep within total duration
                    break

                time.sleep(chunk_sleep)
                sleep_remaining -= chunk_sleep
        
        if not shutdown_requested and TOTAL_RUN_DURATION_HOURS and time.time() >= end_time:
            print(f"[{datetime.datetime.now()}] Total run duration of {TOTAL_RUN_DURATION_HOURS} hour(s) completed.")

    except Exception as e:
        print(f"[{datetime.datetime.now()}] A critical error occurred in the main loop: {e}")
    finally:
        print(f"[{datetime.datetime.now()}] AI Enrichment Scheduler finished.")

if __name__ == "__main__":
    main()
