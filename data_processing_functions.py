import pandas as pd
import threading
from datetime import datetime
from google_sheets_functions import get_df_from_google_sheet

def get_user_observations_helper(user_id, google_sheets_service):

    ## Pull data

    # Set Experimenter Database sheet_id
    sheet_id = "10Lt6tlYRfFSg5KBmF-xCOvdh6shfa1yuvgD2J5z6rbU"

    # Pull data (note: we can't use exec() without throwing an error that the df_users doesn't exist later on... probably because it takes too long to return)
    df_users = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'users!A1:Z1000')
    df_experiment_groups = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'experiment_groups!A1:Z1000')
    df_users_experiment_groups = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'users_experiment_groups!A1:Z1000')
    df_experiments = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'experiments!A1:Z1000')
    df_observation_prompts = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'observation_prompts!A1:Z1000')
    df_observations = get_df_from_google_sheet(google_sheets_service=google_sheets_service, sheet_id = sheet_id, sheet_range = 'observations!A1:Z1000')

    # Rename id to table_name_id (e.g., id > user_id)
    tuples = [
        ['users', 'user_id'],
        ['experiment_groups', 'experiment_group_id'],
        ['users_experiment_groups', 'not_applicable'],
        ['experiments', 'experiment_id'],
        ['observation_prompts', 'observation_prompt_id'],
        ['observations', 'observation_id'],
    ]
    for tuple in tuples:
        exec(f"df_{tuple[0]}.rename(columns={{'id': '{tuple[1]}'}}, inplace=True)")

    ## Format data
    tuples = [
        ['df_users_experiment_groups', 'assigned_date'],
        ['df_observations', 'observation_date'],
    ]
    for tuple in tuples:
        exec(f"{tuple[0]}['{tuple[1]}'] = pd.to_datetime({tuple[0]}['{tuple[1]}'], infer_datetime_format=True)")

    ## Restrict data

    # Restrict to relevant user
    df_users = df_users[df_users['user_id'] == user_id]

    # Restrict to experiment groups that have already been assigned
    today = pd.Timestamp(datetime.today())
    df_users_experiment_groups.query('assigned_date <= @today', inplace=True)


    ## Construct response df
    tuples = [
        ['df_users', 'df_users_experiment_groups', ['user_id']],
        ['df', 'df_experiment_groups', ['experiment_group_id']],
        ['df', 'df_experiments', ['experiment_group_id']],
        ['df', 'df_observation_prompts', ['experiment_id']],
        ['df', 'df_observations', ['user_id','experiment_id','observation_prompt_id']],
    ]
    for tup in tuples:
        df = eval(f"pd.merge({tup[0]}, {tup[1]}, left_on={tup[2]}, right_on={tup[2]}, how='left')")

    # Create dict_response
    dict_response = {
        'first_name': df['first_name'].get(0),
        'observations': df.to_dict('records')}
    
    return dict_response