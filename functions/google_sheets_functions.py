from apiclient import discovery # pip install google-api-python-client
from google.oauth2 import service_account # pip install google-auth-httplib2
import pandas as pd
from honeybadger import honeybadger
import traceback


## Additional Methods for Google Sheets: 
# Writing data: 
# https://denisluiz.medium.com/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e
# https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values

# Create connection to Google Sheets API
def create_google_sheets_service(service_account_info, logger):

    try: 
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)
        google_sheets_service = discovery.build('sheets', 'v4', credentials=credentials)

        return google_sheets_service

    except Exception as e:

        error_class = f"API | create_google_sheets_service()"
        error_message = f"create_google_sheets_service() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)


# Read values from spreadsheet
def read_data_from_google_sheet(google_sheets_service, sheet_id, sheet_range, logger):

    try: 
        request = google_sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range)
        response = request.execute()
        
        return response.get("values")
    
    except Exception as e:

        error_class = f"API | read_data_from_google_sheet()"
        error_message = f"read_data_from_google_sheet() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)
        raise Exception(error_message)

# Read values from spreadsheet
def get_df_from_google_sheet(google_sheets_service, sheet_id, sheet_range, logger):

    try:
        request = google_sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range)
        response = request.execute()

        data = response.get("values")

        return pd.DataFrame(data=data[1:], columns=data[0])
    
    except Exception as e:

        error_class = f"API | get_df_from_google_sheet()"
        error_message = f"get_df_from_google_sheet() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)
        raise Exception(error_message)

# Add row(s) of data to spreadsheet
# https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append#InsertDataOption
def append_data_to_google_sheet(google_sheets_service, sheet_id, sheet_range, data, logger):

    try: 
        request = google_sheets_service.spreadsheets().values().append(
            spreadsheetId=sheet_id, 
            range=sheet_range,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": data})
        response = request.execute()
        
        return response.get("updates")
    
    except Exception as e:

        error_class = f"API | append_data_to_google_sheet()"
        error_message = f"append_data_to_google_sheet() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)
        raise Exception(error_message)
