from datetime import datetime
from google_sheets_functions import append_data_to_google_sheet
from honeybadger import honeybadger
import traceback

def log_api_call(environment, endpoint, supabase_client, logger):

    try:

        response = supabase_client.table("api_calls").insert({"environment": environment, "endpoint": endpoint}).execute()
        logger.info(f"API call logged: {response.data}")
        
    # If there is an error, log it
    # Note that we are not raising an error because we don't want to interrupt the API call
    except Exception as e:

        error_class = f"API | log_api_call()"
        error_message = f"Error with log_api_call(); Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        
