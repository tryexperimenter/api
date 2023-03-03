# %% Set Up

# %%%Import standard modules
import os
import uvicorn
# import json
# import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
# from dotenv import dotenv_values

# # # %%% Import custom modules
# # from config import settings # Environment variables (https://itsjoshcampos.codes/fast-api-environment-variables)
# from google_sheets_functions import read_data_from_google_sheet

# # # %%% Load environment variables

# ## Load variables from .env file or OS environment variables
# env_vars = {
#     **dotenv_values(".env"),
#     **os.environ,  # override loaded values with environment variables
# }

# ## Assign environment variables to local variables
# # Convert string environment variables to JSON
# service_account_info = json.loads(env_vars.get('GOOGLE_SHEETS_API_SERVICE_ACCOUNT'))

# %% Set up FastAPI

app = FastAPI()

# origins = [
#     "http://localhost:3000",
#     "localhost:3000",
#     "*" # allow all origins (not particularly safe)
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"]
# )

# %% Define routes

@app.get("/")
def home():
    return {"message": "Welcome to your FastAPI starter."}

# @app.get("/user/")
# async def get_log(id: int) -> dict:

#     return { "message": "The user id is: " + str(id)}

# @app.get("/google_sheets/")
# async def get_log(row: int) -> dict:

#     # Set data source
#     sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"
#     sheet_range = "Sheet1!A1:B234"

#     # Read data from Google Sheets
#     data = read_data_from_google_sheet(
#         service_account_info=service_account_info, 
#         sheet_id = sheet_id, 
#         sheet_range = sheet_range)

#     # Convert to dataframe
#     df = pd.DataFrame(
#         data=data[1:], 
#         columns=data[0])

#     return { "data": df.iloc[row].to_dict()}

# %% Run app
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
