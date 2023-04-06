from supabase import create_client, Client
import pandas as pd
from honeybadger import honeybadger
import traceback

# %%Supabase / Python Overview and Sample Code

# Documentation for connecting and querying tables https://supabase.com/docs/reference/python/initializing
# Documentation for Supabase / Postgresql functions to return the results of complicated queries https://supabase.com/docs/guides/database/functions

# ## Load variables from .env file or OS environment variables
# import os
# from dotenv import dotenv_values # pip install python-dotenv
# env_vars = {
#     **dotenv_values(r"C:/Users/trist/experimenter/api/.env"),
#     **os.environ,  # override loaded values with environment variables
# }

# ## Intialize Supabase client
# supabase_url: str = env_vars.get("SUPABASE_URL")
# supabase_public_api_key: str = env_vars.get("SUPABASE_PUBLIC_API_KEY")
# supabase_client: Client = create_client(supabase_url, supabase_public_api_key)

# ## Directly query a database table and return a pandas dataframe
# response = supabase_client.table("users").select("*").execute()
# df = pd.DataFrame(response.data)

# ## Directly insert data into a database table
# response = supabase_client.table("api_calls").insert({"environment": environment, "endpoint": endpoint}).execute()

# ## Call users() postgres function with no parameters given
# response = supabase_client.rpc(fn = "users", params = {}).execute()

# ## Call get_user() postgres function with email parameter given
# response = supabase_client.rpc(fn = "get_user", params = {"email":"santa@gmail.com"}).execute()

# Create Supabase Client
def create_supabase_client(supabase_url, supabase_public_api_key, logger):

    try: 

        return create_client(supabase_url, supabase_public_api_key)

    except Exception as e:

        error_class = f"API | create_supabase_client()"
        error_message = f"create_supabase_client() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)


# %% Functions to query Supabase

def supabase_get_experimenter_log_data(public_user_id: str, supabase_client: Client, logger):

    try: 

        # Call the get_experimenter_log_data() postgres function defined in Supabase
        # Case 1: Public_user_id is not found / not active -- returns no rows
        # Case 2: User has no experiments -- returns one row with just user's info
        # Case 3: User has experiments -- returns rows for every experiment / observation prompt combination
        # Case 4: User has experiments and has observations -- returns rows for every experiment / observation prompt combination with observation column filled out
        response = supabase_client.rpc(
            fn = "get_experimenter_log_data", 
            params = {"public_user_id": public_user_id}).execute()
        
        return pd.DataFrame(response.data)
    
    except Exception as e:

        error_class = f"API | supabase_get_experimenter_log_data()"
        error_message = f"supabase_get_experimenter_log_data() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)
        raise Exception(error_message)

