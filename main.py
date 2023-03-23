# %% Set Up

# %%%Import standard modules
import os, sys
import uvicorn
import json
import pandas as pd
from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from dotenv import dotenv_values # pip install python-dotenv
from datetime import datetime
# honeybadger.io: monitor errors in production
from honeybadger import honeybadger 
from honeybadger.contrib.fastapi import HoneybadgerRoute

# %%% Import custom modules
sys.path.append("./functions")
from google_sheets_functions import create_google_sheets_service, get_df_from_google_sheet
from data_processing_functions import get_experimenter_log_helper
from logging_functions import get_logger
from json_response_processing_functions import create_json_response

# %%% Set up logging
if 'logger' not in locals():
    logger = get_logger(logger_name = "api")

# %%% Enable autoreload (only use when testing prior to launching server (locally or in production))
# If you change code in custom functions, you'll need to reload them. Otherwise, you'll have to close out of Python and reimport everything.
# https://switowski.com/blog/ipython-autoreload/
# %load_ext autoreload
# %autoreload 2

# %%% Load environment variables
logger.info("Loading environment variables")

## Load variables from .env file or OS environment variables
env_vars = {
    **dotenv_values(".env"),
    **os.environ,  # override loaded values with environment variables
}

## Assign environment variables to local variables
# Convert string environment variables to JSON
environment=env_vars.get('ENVIRONMENT')
google_sheets_service_account_info = json.loads(env_vars.get('GOOGLE_SHEETS_API_SERVICE_ACCOUNT'))
honeybadger_api_key = env_vars.get('HONEYBADGER_API_KEY')

# %%% Create service accounts
logger.info("Configure Honeybadger monitoring")
honeybadger.configure(
    api_key=honeybadger_api_key,
    environment=environment)
logger.info("Creating Google Sheets service account")
google_sheets_service = create_google_sheets_service(service_account_info = google_sheets_service_account_info, logger = logger)



# %% Set up FastAPI
logger.info("Setting up FastAPI")

app = FastAPI(
    # disable api.tryexperimenter.com/docs and /redoc so that anyone who has the endpoint can't see the docs
    docs_url=None, redoc_url=None 
)

origins = [
    "http://localhost:3000",
    "localhost:3000",
    "app.tryexperimenter.com",
    #"*" # allow all origins (not particularly safe)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Honeybadger monitoring (https://docs.honeybadger.io/lib/python/?projectToken=hbp_ovXY9oSgRtyAvxHA9XSntJlClDLE2Q3PXxDl#fastapi-advanced-usage-)
app.router.route_class = HoneybadgerRoute
router = APIRouter(route_class=HoneybadgerRoute)

# %% Define routes

@app.get("/")
def home():

    logger.info("Endpoint called: /")

    return {"message": "Success!"}

@app.get("/sample_error/")
async def get_error() -> dict:

    logger.info(f"Endpoint called: /sample_error/")

    a = 2/0

    return { "message": "Error: " + a}

@app.get("/user/")
async def get_log(id: int) -> dict:

    logger.info(f"Endpoint called: /user/?id={id}")

    return { "message": "The user id is: " + str(id)}


@app.get("/google-sheets/")
async def get_google_sheets_data(row: int) -> dict:

    logger.info(f"Endpoint called: /googlesheets/?row={row}")

    # Set data source
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
    sheet_range = "Sheet1!A1:B234"

    # Read data from Google Sheets
    df = get_df_from_google_sheet(
        google_sheets_service=google_sheets_service, 
        sheet_id = sheet_id, 
        sheet_range = sheet_range,
        logger = logger)

    return { "data": df.iloc[row].to_dict()}

@app.get("/v1/experimenter-log/")
async def get_experimenter_log(log_id: str):

    logger.info(f"Endpoint called: /v1/experimenter-log/?log_id={log_id}")

    try:
        logger.info("Calling get_experimenter_log_helper()")
        dict_response = get_experimenter_log_helper(log_id = log_id, google_sheets_service = google_sheets_service, logger = logger)
        logger.info("Calling create_json_response()")
        json_response = create_json_response(dict_response = dict_response, logger = logger)
        logger.info("Returning json_response")
        return json_response
    
    except Exception as e:
        
        error_class = f"API | /v1/experimenter-log/?log_id={log_id}"
        error_message = f"Error with /v1/experimenter-log/?log_id={log_id}; Error: {e}"
        logger.error(error_message)
        honeybadger.notify(error_class=error_class, error_message=error_message)

        return {"error": "true", "message": f"Error collecting Experimenter Log data for log_id: {log_id}"}

# %% Run app
if __name__ == "__main__":
    
    # Development (reload on code changes)
    # uvicorn.run("main:app", reload=True, host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")

    # Production
    uvicorn.run("main:app", host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")
