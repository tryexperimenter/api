# %% Set Up

# %%%Import standard modules
import os
import uvicorn
import json
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import dotenv_values # pip install python-dotenv
from datetime import datetime

# %%% Import custom modules
from google_sheets_functions import create_google_sheets_service, get_df_from_google_sheet
from data_processing_functions import get_experimenter_log_helper

# %%% Enable autoreload (only use when testing prior to launching server (locally or in production))
# If you change code in custom functions, you'll need to reload them. Otherwise, you'll have to close out of Python and reimport everything.
# https://switowski.com/blog/ipython-autoreload/
# %load_ext autoreload
# %autoreload 2

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
async def get_google_sheets_data(row: int) -> dict:

    # Set data source
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
    sheet_range = "Sheet1!A1:B234"

    # Read data from Google Sheets
    df = get_df_from_google_sheet(
        google_sheets_service=google_sheets_service, 
        sheet_id = sheet_id, 
        sheet_range = sheet_range)

    return { "data": df.iloc[row].to_dict()}

@app.get("/v1/experimenter-log/")
async def get_experimenter_log(log_id: str):

    return get_experimenter_log_helper(log_id = log_id, google_sheets_service = google_sheets_service)

# %% Run app
if __name__ == "__main__":
    
    # Development (reload on code changes)
    uvicorn.run("main:app", reload=True, host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")

    # Production
    # uvicorn.run("main:app", host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")