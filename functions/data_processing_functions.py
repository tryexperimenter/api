import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
import pytz
from honeybadger import honeybadger
import traceback
from supabase_db_functions import supabase_get_experimenter_log_data


def get_experimenter_log_helper(public_user_id, supabase_client, logger):

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
        dict_response['experiments_to_display'] = "True"
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

        # Replace missing values (NaN) with "" as NaN is not acceptable in final JSON output
        df.replace({np.nan: ""}, inplace = True)

        ## CASE 3: User exists with assigned experiments (and potentially observations)
        # Outcome: Return dict_response with all of the experiments and observations for the user

        ## Update response dictionary
        dict_response["days_of_experimenting"] = (pytz.timezone('UTC').localize(datetime.utcnow()) - df['display_datetime'].min()).days + 1 # Add 1 to include today, take max in case there are no assigned dates
        
        ## Prepare dict_response with all of the experiments and observations for the user

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
                    "experiment_sub_group_display_date": df_experiment_sub_group.display_datetime.get(0).strftime("%B %#d, %Y"),
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
    
    
