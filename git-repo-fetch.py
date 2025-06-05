# scripts/run_fetch.py
import os
import httpx # Import httpx to catch its specific exceptions
from supabase import create_client, Client
# from supabase.lib.client_options import ClientOptions # If needing specific options
# from postgrest import APIError # Import if checking instance type of error
from dotenv import load_dotenv
import logging
import json

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)

def main():
    load_dotenv()

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    supabase_function_url = os.getenv("SUPABASE_FUNCTION_URL")

    try:
        batch_size = int(os.getenv("PROCESSING_BATCH_SIZE", "10"))
        if batch_size <= 0:
            logging.warning(f"PROCESSING_BATCH_SIZE ('{os.getenv('PROCESSING_BATCH_SIZE')}') was zero or negative, defaulting to 10.")
            batch_size = 10
    except ValueError:
        logging.warning(f"PROCESSING_BATCH_SIZE ('{os.getenv('PROCESSING_BATCH_SIZE')}') was not a valid integer, defaulting to 10.")
        batch_size = 10

    if not all([supabase_url, supabase_service_key, supabase_function_url]):
        logging.error(
            "Missing critical environment variables. Ensure SUPABASE_URL, "
            "SUPABASE_SERVICE_ROLE_KEY, and SUPABASE_FUNCTION_URL are set."
        )
        return

    try:
        supabase: Client = create_client(supabase_url, supabase_service_key)
    except Exception as e:
        logging.error(f"Failed to initialize Supabase client: {e}")
        return

    logging.info(f"Querying for up to {batch_size} tool(s) that need fetching or updating...")
    
    response = None # Initialize response to None
    try:
        response = supabase.rpc(
            'get_existing_tools_to_update_batched',
            {'p_limit': batch_size}
        ).execute()

        # --- Enhanced Debugging and Error Handling ---
        logging.info(f"RPC raw response type: {type(response)}")
        logging.info(f"RPC raw response attributes: {dir(response)}")
        # It's good practice to log the response status if available, though execute() might hide it
        # logging.info(f"RPC response status if available: {getattr(response, 'status_code', 'N/A')}")


        # Check for errors more safely
        rpc_error_obj = getattr(response, 'error', None) # Get 'error' attribute, default to None if not found

        if rpc_error_obj:
            # If rpc_error_obj is an object with attributes like message, code, details:
            error_message = getattr(rpc_error_obj, 'message', str(rpc_error_obj))
            error_code = getattr(rpc_error_obj, 'code', 'UnknownErrorCode')
            error_details = getattr(rpc_error_obj, 'details', 'NoDetails')
            logging.error(f"Error querying tools via RPC (from response.error): {error_message} (Code: {error_code}, Details: {error_details})")
            return # Stop further processing if an error is explicitly found in response.error

        # If response.error attribute doesn't exist or is None,
        # check if data itself looks like an error (sometimes PostgREST might return errors in data with HTTP 200)
        # This is less common if .error is properly populated by supabase-py, but as a fallback.
        # A successful data response from this RPC should be a list.
        if hasattr(response, 'data'):
            if isinstance(response.data, dict) and response.data.get('message'):
                # This might indicate an error structure within the data payload
                logging.error(f"Potential error in RPC response data: {response.data.get('message')} (Code: {response.data.get('code', 'N/A')})")
                # Decide if this should be a hard return or if data could still be processed
                # For now, let's assume if .error wasn't set, and data is not a list, it might be an issue
                if not isinstance(response.data, list):
                     logging.warning("RPC response data is not a list as expected. Further processing might fail.")
                     # Depending on strictness, you might want to 'return' here.
            
            tools_to_process = response.data if isinstance(response.data, list) else []
            if not isinstance(response.data, list):
                 logging.info(f"RPC response.data was not a list, received type: {type(response.data)}. tools_to_process initialized as empty list.")


        else: # response object does not even have a 'data' attribute
            logging.error(f"RPC response object does not have a 'data' attribute. This is unexpected. Full response: {str(response)[:500]}") # Log first 500 chars
            return


    # Catching specific HTTP errors that might be raised by .execute()
    except httpx.HTTPStatusError as e:
        # This will catch errors like 404 Not Found, 500 Internal Server Error, etc.
        # if supabase-py (or underlying httpx) raises them.
        error_body = "No response body"
        try:
            if e.response and hasattr(e.response, 'text'):
                error_body = e.response.text
            elif e.response and hasattr(e.response, 'content'):
                error_body = e.response.content.decode(errors='ignore') # Try to decode if binary
        except Exception:
            pass # Ignore errors during error body extraction for logging
        logging.error(f"HTTP error during Supabase RPC query: Status {e.response.status_code if e.response else 'N/A'} - Body: {error_body}", exc_info=False)
        return
    except Exception as e: # Catch any other exceptions during the RPC call or initial response handling
        logging.error(f"An unhandled exception occurred during Supabase RPC query or initial response processing: {e}", exc_info=True) # exc_info=True logs stack trace
        return
    # --- End of Enhanced Debugging and Error Handling ---


    if not tools_to_process: # tools_to_process would be an empty list if errors occurred above and led to it
        logging.info("No tools found requiring an update in this batch (or an error occurred before processing).")
        return

    logging.info(f"Found {len(tools_to_process)} tool(s) to process in this batch (max was {batch_size}).")

    # ... (rest of your script to iterate through tools_to_process and call Edge Function)
    # This part remains the same as your previous version
    with httpx.Client() as client:
        for tool_data in tools_to_process:
            raw_tool_id = tool_data.get('raw_tool_id')
            html_url = tool_data.get('html_url')

            if not raw_tool_id or not html_url:
                logging.warning(f"Skipping tool due to missing 'raw_tool_id' or 'html_url': {tool_data}")
                continue

            logging.info(f"Triggering Edge Function for raw_tool_id: {raw_tool_id}, URL: {html_url}")

            payload = {
                "raw_tool_id": str(raw_tool_id),
                "html_url": html_url
            }

            headers = {
                "Authorization": f"Bearer {supabase_service_key}",
                "Content-Type": "application/json"
            }

            try:
                function_response = client.post(supabase_function_url, json=payload, headers=headers, timeout=600.0)
                response_json = {}
                try:
                    response_json = function_response.json()
                except json.JSONDecodeError:
                    logging.warning(f"Non-JSON response received for {raw_tool_id}. Status: {function_response.status_code}, Body: {function_response.text[:200]}")

                if function_response.is_success:
                    if response_json.get("skipped"):
                        logging.info(f"Skipped {raw_tool_id} (unchanged by function): {response_json.get('reason', 'No reason provided')}")
                    else:
                        logging.info(f"Successfully processed {raw_tool_id}. Function response: {response_json}")
                else:
                    logging.error(
                        f"Error processing {raw_tool_id}. Status: {function_response.status_code}, "
                        f"Response: {response_json or function_response.text[:500]}"
                    )

            except httpx.TimeoutException:
                logging.error(f"Request to Edge Function timed out for {raw_tool_id} ({html_url}).")
            except httpx.RequestError as e:
                logging.error(f"HTTP request to Edge Function failed for {raw_tool_id} ({html_url}): {e}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while calling Edge Function for {raw_tool_id}: {e}", exc_info=True)

    logging.info("Python scheduler script finished processing batch.")

if __name__ == "__main__":
    main()
