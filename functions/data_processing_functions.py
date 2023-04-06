import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
from supabase_db_functions import supabase_get_experimenter_log_data
from honeybadger import honeybadger
import traceback

## Load variables from .env file or OS environment variables
from supabase import create_client, Client
import os
from dotenv import dotenv_values # pip install python-dotenv
env_vars = {
    **dotenv_values(r"C:/Users/trist/experimenter/api/.env"),
    **os.environ,  # override loaded values with environment variables
}

## Intialize Supabase client
supabase_url: str = env_vars.get("SUPABASE_URL")
supabase_public_api_key: str = env_vars.get("SUPABASE_PUBLIC_API_KEY")
supabase_client: Client = create_client(supabase_url, supabase_public_api_key)

#Akua: 255243
#Tristan: f1c14b

response = supabase_client.rpc(
            fn = "get_experimenter_log_data", 
            params = {"public_user_id": "f1c14b"}).execute()

df = pd.DataFrame(response.data)         

def get_experimenter_log_helper(public_user_id, supabase_client, logger):

# Deal with each case:
# 1. public_user_id not found
# 2. public_user_id found but no experiments to display
# 3. public_user_id found and experiments to display
# Probably need to do multiple with statements (first for user_lookups, then for experiments, then for observations)

    try:

        ## Pull data from Supabase
        logger.info("Started pulling data from Supabase")
        df = supabase_get_experimenter_log_data(public_user_id = public_user_id, supabase_client=supabase_client, logger=logger)
        logger.info("Finished pulling data from Supabase")

        ## CASE 1: Public_user_id was not found / not active
        # Test: No rows in df
        # Outcome: Return error 
        if len(df) == 0:

            info_message = f"user_lookups table did not contain active public_user_id of '{public_user_id}'"
            logger.info(info_message)

            sleep(3) # Sleep to prevent brute force attacks

            return {"error": "True", "end_user_error_message": f"Error collecting Experimenter Log data for public_user_id: {public_user_id}"}

        ## Initialize response dictionary
        dict_response = {}
        dict_response["public_user_id"] = public_user_id
        dict_response["first_name"] = df.first_name.get(0)
        dict_response['error'] = "False"

        ## CASE 2: User exists but has no assigned experiments
        # Test:  df has just one row with just user's info (e.g., experiment_group = None)
        # Outcome: Return early with experiments_to_display = False 
        if len(df) == 1 and df.experiment.get(0) == None:
            dict_response['experiments_to_display'] = "False"
            logger.info(f"No experiments to display for public_user_id: {public_user_id}")

            return dict_response

        ## Format data
        logger.info("Formatting data")

        # Convert datetime strings to datetime objects
        date_vars = ["display_datetime"]
        for date_var in date_vars:
            exec(f"df['{date_var}'] = pd.to_datetime(df['{date_var}'], infer_datetime_format=True)")


        ## Restrict data
        logger.info("Restricting data")

        # Restrict to experiment_sub_groups that should be visible to user (e.g., action_datetime is before today at local timezone??)
        
        df_users_experiment_sub_groups.query('visible_to_user == "1"', inplace=True)

        # Replace missing values (NaN) with "" as NaN is not acceptable in final JSON output
        df.replace({np.nan: ""}, inplace = True)

        # Update response dictionary
        dict_response["days_of_experimenting"] = 1 # placeholder if we have not yet assigned experiments
        dict_response['experiments_to_display'] = "True"
        dict_response["days_of_experimenting"] = max(1, (datetime.today() - df['assigned_date'].min()).days + 1) # Add 1 to include today, take max in case there are no assigned dates
        dict_response['experiments_to_display'] = "True"
        
        # Initialize helper array
        array_experiment_groups = []

        # Collect data for each experiment group
        for experiment_group_id in df.experiment_group_id.unique():

            df_experiment_group = df.query("experiment_group_id == '" + experiment_group_id  + "'").reset_index()

            array_experiment_sub_groups = []

            # Assemble for each experiment in each experiment sub group
            for experiment_sub_group_id in df_experiment_group.experiment_sub_group_id.unique():
                
                df_experiment_sub_group = df_experiment_group.query("experiment_sub_group_id == '" + experiment_sub_group_id  + "'").reset_index()

                # Generate data for all associated experiments with this experiment sub group
                # Methodology: https://stackoverflow.com/questions/55004985/convert-pandas-dataframe-to-json-with-columns-as-key
                # Output Sample: [{'experiment_id': 'e1', 'experiment': 'Celebrate something.', 'data': [{'observation_prompt': 'What makes you happier at work?', 'observation': 'My observation for o1.'}, {'observation_prompt': 'What makes you successful at work?', 'observation': 'My observation for o7.'}]}, {'experiment_id': 'e2', 'experiment': 'Seek feedback from someone on your team.', 'data': [{'observation_prompt': 'What makes you happier at work?', 'observation': 'My observation for o2.'}, {'observation_prompt': 'What makes you successful at work?', 'observation': 'My observation for o8.'}]}, {'experiment_id': 'e3', 'experiment': 'Keep quiet for 10-15 minutes in a meeting (or until someone asks for your input).', 'data': [{'observation_prompt': 'What makes you happier at work?', 'observation': 'My observation for o3.'}, {'observation_prompt': 'What makes you successful at work?', 'observation': 'My observation for o9.'}]}]
                primary_cols = ['experiment_id', 'experiment']
                data_cols = ['observation_prompt', 'observation']
                dict_experiments = (df_experiment_sub_group.groupby(primary_cols)[data_cols]
                    .apply(lambda x: x.to_dict('records'))
                    .reset_index(name='observations')
                    .to_dict(orient='records'))

                # For each experiment, set observations to "None" if there are no observations 
                for index in range(0, len(dict_experiments)):
                    if dict_experiments[index]['observations'] == [{'observation_prompt': '', 'observation': ''}]:
                        dict_experiments[index]['observations'] = "None"
                
                # Add experiments / observations for this sub_group
                array_experiment_sub_groups.append(
                    {"experiment_sub_group_id": experiment_sub_group_id,
                    "experiment_sub_group": df_experiment_sub_group.experiment_sub_group.get(0),
                    "experiment_sub_group_assigned_date": df_experiment_sub_group.assigned_date.get(0).strftime("%B %#d, %Y"),
                    "experiments": dict_experiments}
                )

            # Add data for particular experiment group
            array_experiment_groups.append(
                {"experiment_group_id": experiment_group_id,
                "experiment_group": df_experiment_group.experiment_group.get(0),
                "experiment_sub_groups": array_experiment_sub_groups}
            )

        # Add experiment groups, sub_groups, experiments, and observations to the response dictionary
        dict_response['experiment_groups'] = array_experiment_groups

        logger.info(f"Successfully ran get_experimenter_log_helper() for public_user_id: {public_user_id}")
        return dict_response
    
    # Catch any exceptions as we tried to execute the function
    except Exception as e:

        error_class = f"API | get_experimenter_log_helper()"
        error_message = f"Error with /v1/experimenter-log/?public_user_id={public_user_id}; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)

        return {"error": "True", "end_user_error_message": f"Error collecting Experimenter Log data for public_user_id: {public_user_id}"}
    
    
