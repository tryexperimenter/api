from apiclient import discovery # pip install google-api-python-client
from google.oauth2 import service_account # pip install google-auth-httplib2
import pandas as pd

## Additional Methods for Google Sheets: 
# Example of writing data: https://denisluiz.medium.com/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e
# https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values

# Create connection to Google Sheets API
def create_google_sheets_service(service_account_info):

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)
    google_sheets_service = discovery.build('sheets', 'v4', credentials=credentials)

    return google_sheets_service


# Read values from spreadsheet
def read_data_from_google_sheet(google_sheets_service, sheet_id, sheet_range):

    request = google_sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range)
    print("Calling Google Sheets API")
    response = request.execute()
    print("Google Sheets API Returned")
    
    return response.get("values")

# Read values from spreadsheet
def get_df_from_google_sheet(google_sheets_service, sheet_id, sheet_range):

    request = google_sheets_service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range)
    print("Calling Google Sheets API")
    response = request.execute()
    print("Google Sheets API Returned")

    data = response.get("values")

    return pd.DataFrame(data=data[1:], columns=data[0])