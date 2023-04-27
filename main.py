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
from postgresql_db_functions import create_db_connection
from standard_processes_functions import schedule_messages

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
db_connection_parameters: dict = json.loads(env_vars.get("PROD_DB_CONNECTION_PARAMETERS"))
sendgrid_api_key = env_vars.get('SENDGRID_API_KEY')
short_io_api_key = env_vars.get('SHORT_IO_API_KEY')


# %%% Create service accounts
logger.info("Configure Honeybadger monitoring")
honeybadger.configure(
    api_key=honeybadger_api_key,
    environment=environment)



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

@app.get("/v1/schedule_messages/")
async def api_schedule_messages(auth_code: str):

    if auth_code == "rFLrsTdXGcA8VyoyaBMY-L*mMe@enU": 

        db_conn = None

        try:

            # Get database connection
            db_conn = create_db_connection(db_connection_parameters, logger)

            # Log API call
            endpoint = f"/v1/schedule_messages/?auth_code={auth_code}"
            logger.info(f"Endpoint called: {endpoint}")
            log_api_call(environment=environment, endpoint=endpoint, db_conn=db_conn, logger = logger)

            # Schedule messages
            logger.info("Calling schedule_messages()")
            dict_response = schedule_messages(db_conn = db_conn, sendgrid_api_key = sendgrid_api_key, short_io_api_key = short_io_api_key, logger = logger)

            # TODO: If we want to return something: Format response as JSON
            logger.info("Calling create_json_response()")
            json_response = create_json_response(dict_response = dict_response, logger = logger)

            logger.info("Returning json_response")
            return json_response
        
        except Exception as e:
            
            error_class = f"API | /v1/schedule_messages/?auth_code={auth_code}"
            error_message = f"Error with /v1/schedule_messages/?auth_code={auth_code}; Error: {e}"
            logger.error(error_message)
            logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
            honeybadger.notify(error_class=error_class, error_message=error_message)

            return {"error": "True", "error_message": f"Error running /v1/schedule_messages/?auth_code={auth_code}. Check logs for more info."}
        
        finally:
            if db_conn is not None:
                db_conn.close()

    else:

        return {"error": "True", "message": f"authorization code incorrect: {auth_code}"}

@app.get("/v1/experimenter-log/")
async def get_experimenter_log(public_user_id: str):

    db_conn = None

    try:

        # Get database connection
        db_conn = create_db_connection(db_connection_parameters, logger)

        # Log API call
        endpoint = f"/v1/experimenter-log/?public_user_id={public_user_id}"
        logger.info(f"Endpoint called: {endpoint}")
        log_api_call(environment=environment, endpoint=endpoint, db_conn=db_conn, logger=logger)

        # Get experimenter log data
        logger.info("Calling get_experimenter_log_helper()")
        dict_response = get_experimenter_log_helper(public_user_id = public_user_id, db_conn = db_conn, logger = logger)

        # Format response as JSON
        logger.info("Calling create_json_response()")
        json_response = create_json_response(dict_response = dict_response, logger = logger)

        logger.info("Returning json_response")
        return json_response
    
    except Exception as e:
        
        error_class = f"API | /v1/experimenter-log/?public_user_id={public_user_id}"
        error_message = f"Error with /v1/experimenter-log/?public_user_id={public_user_id}; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)

        return {"error": "True", "end_user_error_message": f"Error collecting Experimenter Log data for public_user_id: {public_user_id}"}
    
    finally:
        if db_conn is not None:
            db_conn.close()

# %% Run app
if __name__ == "__main__":

    # Start FastAPI server

    if environment == "production":

        # Production
        uvicorn.run("main:app", host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")

    elif environment == "development":

        # Development (reload on code changes)
        uvicorn.run("main:app", reload=True, host="0.0.0.0", port=os.getenv("PORT", default=5000), log_level="info")
