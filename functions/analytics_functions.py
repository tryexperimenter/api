from datetime import datetime
from google_sheets_functions import append_data_to_google_sheet
from honeybadger import honeybadger
import traceback

def log_api_call(endpoint, google_sheets_service, logger):

    try:

        # Set sheet / range to log to
        sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
        sheet_range = "api_calls!A1:Z5000"

        # Get date and time
        date = datetime.now().strftime("%Y-%m-%d")
        time = datetime.now().strftime("%H:%M:%S")

        # Set data to append
        data = [
            [date, time, endpoint]
        ]

        # Read data from Google Sheets
        response = append_data_to_google_sheet(
            google_sheets_service=google_sheets_service, 
            sheet_id = sheet_id, 
            sheet_range = sheet_range,
            data = data, 
            logger = logger)
        
    # If there is an error, log it
    # Note that we are not raising an error because we don't want to interrupt the API call
    except Exception as e:

        error_class = f"API | log_api_call()"
        error_message = f"Error with log_api_call(); Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        
