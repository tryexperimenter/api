import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
from google_sheets_functions import get_df_from_google_sheet
from honeybadger import honeybadger
import traceback

# add some sample text

def get_experimenter_log_helper(log_id, google_sheets_service, logger):

    try:
        ## Pull data

        # Set Experimenter Database sheet_id
        sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"

        # Pull data (note: we can't loop through tuples of sheet_ranges and use exec() without throwing an error that the df_users doesn't exist later on... probably because it takes too long to return)
        logger.info("Started pulling data from Google Sheets")
        df_users_logs = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'users_logs!A1:Z1000', logger = logger)    
        df_users = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'users!A1:Z1000', logger = logger)
        df_experiment_groups = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'experiment_groups!A1:Z1000', logger = logger)
        df_experiment_sub_groups = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'experiment_sub_groups!A1:Z1000', logger = logger)
        df_users_experiment_sub_groups = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'users_experiment_sub_groups!A1:Z1000', logger = logger)
        df_experiments = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'experiments!A1:Z1000', logger = logger)
        df_observation_prompts = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'observation_prompts!A1:Z1000', logger = logger)
        df_observations = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'observations!A1:Z1000', logger = logger)
        logger.info("Finished pulling data from Google Sheets")

        # Rename id to table_name_id (e.g., id > user_id)
        logger.info("Renaming id columns")
        tuples = [
            ['users', 'user_id'],
            ['experiment_groups', 'experiment_group_id'],
            ['experiment_sub_groups', 'experiment_sub_group_id'],
            ['experiments', 'experiment_id'],
            ['observation_prompts', 'observation_prompt_id'],
            ['observations', 'observation_id'],
        ]
        for tuple in tuples:
            exec(f"df_{tuple[0]}.rename(columns={{'id': '{tuple[1]}'}}, inplace=True)")

        ## Format data
        logger.info("Formatting data")
        tuples = [
            ['df_users_experiment_sub_groups', 'assigned_date'],
            ['df_observations', 'observation_date'],
        ]
        for tuple in tuples:
            exec(f"{tuple[0]}['{tuple[1]}'] = pd.to_datetime({tuple[0]}['{tuple[1]}'], infer_datetime_format=True)")

        ## Restrict data
        logger.info("Restricting data")

        # Restrict to relevant log
        df_users_logs = df_users_logs[df_users_logs['log_id'] == log_id]

        if len(df_users_logs) == 0:

            info_message = f"log_id not found in df_users_logs for log_id: {log_id}"
            logger.info(info_message)

            sleep(3) # Sleep to prevent brute force attacks

            return {"error": "True", "end_user_error_message": f"Error collecting Experimenter Log data for log_id: {log_id}"}

        # Restrict to experiment groups that should be visible to user
        # Note that all data from Google Sheets is read in as string, so we test equality with "1"
        df_users_experiment_sub_groups.query('visible_to_user == "1"', inplace=True)

        ## Construct response df
        logger.info("Constructing response df")
        tuples = [
            #[left df, right df, join variables]
            ['df_users_logs', 'df_users', ['user_id']],
            ['df', 'df_users_experiment_sub_groups', ['user_id']],
            ['df', 'df_experiment_sub_groups', ['experiment_sub_group_id']],
            ['df', 'df_experiment_groups', ['experiment_group_id']],
            ['df', 'df_experiments', ['experiment_sub_group_id']],
            ['df', 'df_observation_prompts', ['experiment_id']],
            ['df', 'df_observations', ['user_id','experiment_id','observation_prompt_id']],
        ]
        for tup in tuples:
            df = eval(f"pd.merge({tup[0]}, {tup[1]}, left_on={tup[2]}, right_on={tup[2]}, how='left')")

        # Replace missing values (NaN) with "" as NaN is not acceptable in final JSON output
        df.replace({np.nan: ""}, inplace = True)

        # Initialize response dictionary
        dict_response = {}
        dict_response["log_id"] = log_id
        dict_response["first_name"] = df.first_name.get(0)
        dict_response["days_of_experimenting"] = max(1, (datetime.today() - df['assigned_date'].min()).days + 1) # Add 1 to include today, take max in case there are no assigned dates
        dict_response['experiments_to_display'] = "True"
        dict_response['error'] = "False"
        array_experiment_groups = []

        # Return if no experiments found for user (e.g., every experiment id is '')
        experiment_ids = df.experiment_id.unique() 
        if len(experiment_ids) == 1 and experiment_ids[0] == '':
            dict_response['experiments_to_display'] = "False"
            logger.info(f"No experiments to display for log_id: {log_id}")
            return dict_response

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

        logger.info(f"Successfully ran get_experimenter_log_helper() for log_id: {log_id}")
        return dict_response
    
    # Catch any exceptions as we tried to execute the function
    except Exception as e:

        error_class = f"API | get_experimenter_log_helper()"
        error_message = f"Error with /v1/experimenter-log/?log_id={log_id}; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)

        return {"error": "True", "end_user_error_message": f"Error collecting Experimenter Log data for log_id: {log_id}"}
    
    
