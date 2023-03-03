# %% Set Up

# %%%Import standard modules
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# %%% Import custom modules
# When running in Terminal
# try:
from .config import settings # Environment variables (https://itsjoshcampos.codes/fast-api-environment-variables)
from .google_sheets_functions import read_data_from_google_sheet

# When running in Visual Studio Code
# except:
#     from config import settings
#     from google_sheets_functions import read_data_from_google_sheet

# %%% Load environment variables
service_account_info = settings.GOOGLE_SHEETS_API_SERVICE_ACCOUNT

# %% Set up FastAPI

app = FastAPI()

origins = [
    "http://localhost:3000",
    "localhost:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to your FastAPI starter."}

@app.get("/user/")
async def get_log(id: int) -> dict:

    return { "message": "The user id is: " + str(id)}

@app.get("/google_sheets/")
async def get_log(row: int) -> dict:

    # Set data source
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
    sheet_range = "Sheet1!A1:B234"

    # Read data from Google Sheets
    data = read_data_from_google_sheet(
        service_account_info=service_account_info, 
        sheet_id = sheet_id, 
        sheet_range = sheet_range)

    # Convert to dataframe
    df = pd.DataFrame(
        data=data[1:], 
        columns=data[0])

    return { "data": df.iloc[row].to_dict()}