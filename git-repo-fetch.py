# scripts/run_fetch.py
import os
import httpx # Using httpx for HTTP requests
from supabase import create_client, Client
from dotenv import load_dotenv # For loading .env file for local development
import logging
import json # For parsing JSON responses

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)

def main():
    """
    Main function to query for tools needing updates and trigger the Supabase Edge Function.
    """
    # 1. Load environment variables from .env file (optional, good for local dev)
    # In production, these should be set directly in the environment.
    load_dotenv()

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") # Service role key
    # Full URL to your deployed Edge Function
    supabase_function_url = os.getenv("SUPABASE_FUNCTION_URL")
    # e.g., "https://<your-project-ref>.supabase.co/functions/v1/fetch_and_store_repo"

    # Validate that all necessary environment variables are set
    if not all([supabase_url, supabase_service_key, supabase_function_url]):
        logging.error(
            "Missing critical environment variables. Ensure SUPABASE_URL, "
            "SUPABASE_SERVICE_ROLE_KEY, and SUPABASE_FUNCTION_URL are set."
        )
        return

    try:
        # Initialize Supabase client using the service role key for admin-level access
        supabase: Client = create_client(supabase_url, supabase_service_key)
    except Exception as e:
        logging.error(f"Failed to initialize Supabase client: {e}")
        return

    # 2. Query for tools to process using the pre-defined SQL function via RPC
    logging.info("Querying for tools that need fetching or updating...")
    try:
        # Call the SQL function 'get_tools_needing_update' created in your Supabase project
        response = supabase.rpc('get_tools_needing_update', {}).execute()

        if response.error:
            logging.error(f"Error querying tools via RPC: {response.error.message} (Code: {response.error.code}, Details: {response.error.details})")
            return
        
        tools_to_process = response.data if response.data else []

    except Exception as e:
        logging.error(f"An exception occurred during Supabase RPC query: {e}")
        return

    if not tools_to_process:
        logging.info("No tools found requiring an update at this time.")
        return

    logging.info(f"Found {len(tools_to_process)} tool(s) to process.")

    # 3. Invoke the Edge Function for each tool
    # Using a synchronous HTTP client here. For a large number of tools,
    # consider an asynchronous client (e.g., httpx.AsyncClient) or a job queue.
    with httpx.Client() as client:
        for tool_data in tools_to_process:
            raw_tool_id = tool_data.get('raw_tool_id')
            html_url = tool_data.get('html_url')

            if not raw_tool_id or not html_url:
                logging.warning(f"Skipping tool due to missing 'raw_tool_id' or 'html_url': {tool_data}")
                continue

            logging.info(f"Triggering Edge Function for raw_tool_id: {raw_tool_id}, URL: {html_url}")

            # Prepare payload for the Edge Function
            payload = {
                "raw_tool_id": str(raw_tool_id),  # Ensure UUID is passed as a string
                "html_url": html_url
            }
            
            # Headers for the Edge Function call, including authorization using the service key
            headers = {
                "Authorization": f"Bearer {supabase_service_key}",
                "Content-Type": "application/json"
                # Supabase Edge Functions might also require an 'apikey' if not using service_role for invocation,
                # but service_role key in Authorization Bearer token is standard for server-to-server.
            }

            try:
                # Make POST request to the Supabase Edge Function
                # Set a reasonable timeout (e.g., 5-10 minutes, as repo download can take time)
                function_response = client.post(supabase_function_url, json=payload, headers=headers, timeout=600.0)

                # Log the outcome based on the function's response
                response_json = {}
                try:
                    response_json = function_response.json()
                except json.JSONDecodeError:
                    logging.warning(f"Non-JSON response received for {raw_tool_id}. Status: {function_response.status_code}, Body: {function_response.text[:200]}")

                if function_response.is_success: # Covers 2xx status codes
                    if response_json.get("skipped"):
                        logging.info(f"Skipped {raw_tool_id} (unchanged by function): {response_json.get('reason', 'No reason provided')}")
                    else:
                        logging.info(f"Successfully processed {raw_tool_id}. Function response: {response_json}")
                else:
                    logging.error(
                        f"Error processing {raw_tool_id}. Status: {function_response.status_code}, "
                        f"Response: {response_json or function_response.text[:500]}" # Log JSON or truncated text
                    )

            except httpx.TimeoutException:
                logging.error(f"Request to Edge Function timed out for {raw_tool_id} ({html_url}).")
            except httpx.RequestError as e:
                logging.error(f"HTTP request to Edge Function failed for {raw_tool_id} ({html_url}): {e}")
            except Exception as e:
                # Catch any other unexpected errors during the processing of a single tool
                logging.error(f"An unexpected error occurred while calling Edge Function for {raw_tool_id}: {e}")
    
    logging.info("Python scheduler script finished.")

if __name__ == "__main__":
    main()
