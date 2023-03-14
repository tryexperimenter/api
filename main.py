# %% Set Up

# %%%Import standard modules
import os
import uvicorn
import json
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import dotenv_values # pip install python-dotenv

# %%% Import custom modules
from google_sheets_functions import create_google_sheets_service, read_data_from_google_sheet

# %%% Load environment variables

## Load variables from .env file or OS environment variables
env_vars = {
    **dotenv_values(".env"),
    **os.environ,  # override loaded values with environment variables
}

## Assign environment variables to local variables
# Convert string environment variables to JSON
google_sheets_service_account_info = json.loads(env_vars.get('GOOGLE_SHEETS_API_SERVICE_ACCOUNT'))

## Create service accounts
google_sheets_service = create_google_sheets_service(service_account_info = google_sheets_service_account_info)

# %% Set up FastAPI

app = FastAPI()

origins = [
    "http://localhost:3000",
    "localhost:3000",
    "*" # allow all origins (not particularly safe)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# %% Define routes

@app.get("/")
def home():
    return {"message": "Welcome to your FastAPI starter."}

@app.get("/user/")
async def get_log(id: int) -> dict:

    return { "message": "The user id is: " + str(id)}

@app.get("/google-sheets/")
async def get_log(row: int) -> dict:

    # Set data source
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
    sheet_range = "Sheet1!A1:B234"

    # Read data from Google Sheets
    data = read_data_from_google_sheet(
        google_sheets_service=google_sheets_service, 
        sheet_id = sheet_id, 
        sheet_range = sheet_range)

    # Convert to dataframe
    df = pd.DataFrame(
        data=data[1:], 
        columns=data[0])

    return { "data": df.iloc[row].to_dict()}

@app.get("/api/v1/experiment-observations/")
async def get_log(user_id: str) -> dict:

    user_id = 'AAA'

    # Pull user data
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
    sheet_range = "Users!A1:D234"
    data = read_data_from_google_sheet(
        google_sheets_service=google_sheets_service, 
        sheet_id = sheet_id, 
        sheet_range = sheet_range)
    df_users = pd.DataFrame(
        data=data[1:], 
        columns=data[0])
    
    # Pull observations data
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
    sheet_range = "Observations!A1:E234"
    data = read_data_from_google_sheet(
        google_sheets_service=google_sheets_service, 
        sheet_id = sheet_id, 
        sheet_range = sheet_range)
    df_observations = pd.DataFrame(
        data=data[1:], 
        columns=data[0])
    
    # Create dict_response
    dict_response = {
        'first_name': df_users['first_name'][df_users['id'] == user_id].get(0),
        'observations': df_observations[['experiment_name','question','answer']][df_observations['user_id'] == user_id].to_dict('records')}

    return dict_response

# %% Run app
if __name__ == "__main__":
    
    # Development (reload on code changes)
    uvicorn.run("main:app", reload=True, host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")

    # Production
    # uvicorn.run("main:app", host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")

    
