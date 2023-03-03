from apiclient import discovery # pip install google-api-python-client
from google.oauth2 import service_account # pip install google-auth-httplib2

## Additional Methods for Google Sheets: 
# Example of writing data: https://denisluiz.medium.com/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e
# https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values

def read_data_from_google_sheet(service_account_info, sheet_id, sheet_range):

    # Create connection to Google Sheets API

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)
    service = discovery.build('sheets', 'v4', credentials=credentials)

    # Read values from spreadsheet

    request = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=sheet_range)
    response = request.execute()

    return response.get("values")
