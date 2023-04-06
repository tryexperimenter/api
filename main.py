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
# https://www.honeybadger.io/blog/honeybadger-fastapi-python/ and https://docs.honeybadger.io/lib/python/#fastapi-advanced-usage-
from honeybadger import honeybadger 
from honeybadger.contrib.fastapi import HoneybadgerRoute
import traceback

# %%% Import custom modules
sys.path.append("./functions")
from data_processing_functions import get_experimenter_log_helper
from logging_functions import get_logger
from json_response_processing_functions import create_json_response
from analytics_functions import log_api_call
from supabase_db_functions import create_supabase_client

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
honeybadger_api_key = env_vars.get('HONEYBADGER_API_KEY')
supabase_url: str = env_vars.get("SUPABASE_URL")
supabase_public_api_key: str = env_vars.get("SUPABASE_PUBLIC_API_KEY")

# %%% Create service accounts
logger.info("Configure Honeybadger monitoring")
honeybadger.configure(
    api_key=honeybadger_api_key,
    environment=environment)
logger.info("Creating Supabase Client")
supabase_client = create_supabase_client(supabase_url=supabase_url, supabase_public_api_key=supabase_public_api_key, logger=logger)



# %% Set up FastAPI
logger.info("Setting up FastAPI")

app = FastAPI(
    # disable api.tryexperimenter.com/docs and /redoc so that anyone who has the endpoint can't see the docs
    docs_url=None, redoc_url=None 
)

if environment == "production":

    origins = [
        "https://app.tryexperimenter.com",
    ]

elif environment == "development":

    origins = [
        "http://localhost:3000",
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

@app.get("/v1/experimenter-log/")
async def get_experimenter_log(log_id: str):

    # Log API call
    endpoint = f"/v1/experimenter-log/?log_id={log_id}"
    logger.info(f"Endpoint called: {endpoint}")
    # log_api_call(endpoint=endpoint, supabase_client=supabase_client, logger=logger)

    try:
        logger.info("Calling get_experimenter_log_helper()")
        dict_response = get_experimenter_log_helper(public_user_id = log_id, supabase_client = supabase_client, logger = logger)

        logger.info("Calling create_json_response()")
        json_response = create_json_response(dict_response = dict_response, logger = logger)

        logger.info("Returning json_response")
        return json_response
    
    except Exception as e:
        
        error_class = f"API | /v1/experimenter-log/?log_id={log_id}"
        error_message = f"Error with /v1/experimenter-log/?log_id={log_id}; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)

        return {"error": "True", "end_user_error_message": f"Error collecting Experimenter Log data for log_id: {log_id}"}

# %% Run app
if __name__ == "__main__":

    # Start FastAPI server

    if environment == "production":

        # Production
        uvicorn.run("main:app", host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")

    elif environment == "development":

        # Development (reload on code changes)
        uvicorn.run("main:app", reload=True, host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")
