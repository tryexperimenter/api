from datetime import datetime
from google_sheets_functions import append_data_to_google_sheet
from honeybadger import honeybadger
import traceback

def log_api_call(environment, endpoint, db_conn, logger):

    try:

        with db_conn.cursor() as cursor:
            cursor.execute('INSERT INTO api_calls (environment, endpoint) VALUES (%s, %s)', (environment, endpoint))

        logger.info(f"API call logged: environment: {environment}, endpoint: {endpoint}")
        
    # If there is an error, log it
    # Note that we are not raising an error because we don't want to interrupt the API call
    except Exception as e:

        error_class = f"API | log_api_call()"
        error_message = f"Error with log_api_call(); Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        
