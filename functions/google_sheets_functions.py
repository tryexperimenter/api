from apiclient import discovery # pip install google-api-python-client
from google.oauth2 import service_account # pip install google-auth-httplib2
import pandas as pd
from honeybadger import honeybadger
import traceback

# Set up info
# Instructions: https://denisluiz.medium.com/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e
# Sample service_account_info: {"type": "service_account","project_id": "experimenter-3744420","private_key_id": "d9737f31fcfbaf864facboops33997393d552507e","private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEIJJKSC593ASCBKgwggSkAgEAAoIBAQDOEcBjZFnZURRa\nUaXzixOsnd3efcu/Vn97OGQNOlbYBePvGZ84lV4tOPjHSo9DJaSUGMjao1nEvXfA\nN9Fbd1cRTSD2MsVBzp/tD/EqBnFGhCF08XznhFBm+qO5UwGQ1/AD0G3yt3aytCNY\nN/xmSI8WUVfB9MbQ8H4FPm+KofW3jug0a2MdQy9aKPW1KydSOvVhaVcKvRXgXhTg\noUyXf8f9IN/x8zmTStaMc3aY80LENXpIECIOR/RoCHzrZWQzoRPL/Shy\n9mqzimZ61cYHW66NStaehIMk0ZMXF1TpwDoA\nElFAWNaLAgMBAAECggEAAdYWqMwUIOOgEs33dJwgqMEfYQQt9J6ItEhkW9nRPEuR\nJGhOCusJ081WKjB35llHhipSX+BkxGQL1SKqMu51UConyQnayCNXOX2S4JT+HWrA\nAZLFUirTrJc/Lj+XG/tQoDgTQr5vyFfimL6m+SJcZA6Fgaz0uJfsB4/u3oVi8yfG\nwgRoKw3Hb1RIZS+zfH+17bzmSMkHmyA5/VWaZmOqjARApN7D8VB2A9C1tJJLLlRN\nGqVzvSqbsJWq4lvfCZSIsAuT2xreZetzmAo+9ayKV88POHlBj2KiUVgHgGlulm1D\nTGkpIliKks8G/vya1lsExuNyJQvQiRONvND009QHgQKBgQDlxC5eqsPL/uRSnBtX\n0Ud2juHge5uf+/+SQJQWbsETPscGcTUo54VKp7HDAqi9kf5OrLmx40QGA5wqaARH\n/o6t9S4F5EpDV5h5qDBytt+S/FAHRFG6p+F3Hga3xAaGtXA1NZJ8YqwrBv0/UY78\nTjMyYQpaJ8FznVPxGHTN3iqY4wKBgQDlmO7ooPyLMxAnttPmNefpJXQqbFzny+UI\n9GT/zmohmE1JOt1FPSEEwj9IEQf/qazl7mGqovkJDCgr7SFf9yP0W3Hyi4O+kyTl\nXwIGDKrRlnSi6We/AyJ9Ud+djUl2KcuSA4F3AzXS4E94ACyFoh9HZP7hDEjprNKA\n2ikRESXEOQKBgQC09QhMGJOyMyJhiX7jb/ingCqXYOKVYqPK7L9012+Kl7Op+DkU\n6RqKTH5tBsgc3UF7dv+dAU+OqQMyRs+wX+TBTssbasuuM+vrTLIzdqGoorzorD7u\nEdA5v1UtH96/81/XGEUxX4kXLh7/4l0JixE5SUIc9Rif1LXKuSctCB9mXwKBgQDC\n7pdYluYT4STEMyuxdu8ROaVpJ1uxyaEJe0YNEdl18HMdy4Z19LKF8c38h8k8vXh4\nN25gi8HYdqPct5Xwfknee41BGkaelRtsSr/TFwoorA8XCgf1Wtn7gHnUsFJAqreV\nnrharTUTdzLBdZRXWRApc4wa0m1NSFfo4lCflulzsQKBgFbnRgeYq8LOkjhDRGzq\nhJaT8Bqf3133Bd/MOMMlvMtKYIyJEsMEBCXFOkHmdNndIxkYipr6bTegLqyJpxz3\nOgnTcvVgja8LkSYR6fo9Hi7hdoaQwYoV7t3yEapbz+8YuGY36XQJDc/AOnbdAZzY\neucross94RilwuSAaPPB3h37\n-----END PRIVATE KEY-----\n","client_email": "google-sheets-api@experimenter-374430.iam.gserviceaccount.com","client_id": "110586022093557292137","auth_uri": "https://accounts.google.com/o/oauth2/auth","token_uri": "https://oauth2.googleapis.com/token","auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/google-sheets-api%40experimenter-374320.iam.gserviceaccount.com"}
# Methods: https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values

# Sample Call
# @app.get("/google-sheets/")
# async def get_google_sheets_data(row: int) -> dict:

#     logger.info(f"Endpoint called: /googlesheets/?row={row}")

#     # Set data source
#     sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
#     sheet_range = "Sheet1!A1:B234"

#     # Read data from Google Sheets
#     df = get_df_from_google_sheet(
#         google_sheets_service=google_sheets_service, 
#         sheet_id = sheet_id, 
#         sheet_range = sheet_range,
#         logger = logger)

#     return {"data": df.iloc[row].to_dict()}


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
