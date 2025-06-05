# scripts/run_fetch.py
import os
import httpx
from supabase import create_client, Client
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
    try:
        response = supabase.rpc(
            'get_existing_tools_to_update_batched', # This must match the SQL function name
            {'p_limit': batch_size}
        ).execute()

        if response.error:
            logging.error(f"Error querying tools via RPC: {response.error.message} (Code: {response.error.code}, Details: {response.error.details})")
            return

        tools_to_process = response.data if response.data else []

    except Exception as e:
        logging.error(f"An exception occurred during Supabase RPC query: {e}")
        return

    if not tools_to_process:
        logging.info("No tools found requiring an update in this batch.")
        return

    logging.info(f"Found {len(tools_to_process)} tool(s) to process in this batch (max was {batch_size}).")

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
                logging.error(f"An unexpected error occurred while calling Edge Function for {raw_tool_id}: {e}")

    logging.info("Python scheduler script finished processing batch.")

if __name__ == "__main__":
    main()
