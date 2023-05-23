import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
import pytz
from honeybadger import honeybadger
import traceback
import smartypants

# Custom imports
from postgresql_db_functions import create_db_connection, execute_sql_return_df


def get_experimenter_log_helper(public_user_id, db_connection_parameters, logger):

    try:

        # Get database connection
        db_conn = None # initialize db_conn as None so that the finally block doesn't error out if the db_conn variable doesn't exist
        db_conn = create_db_connection(db_connection_parameters, logger)

        ## Retrieve experimenter log data from database
        logger.info("Retrieve experimenter log data from database")

        # Define sql query (use parameters rather than f-string to avoid SQL injection)
        sql_params = {'public_user_id': public_user_id}
        sql_statement = """
/*
Case 1: Public_user_id is not found / not active -- returns no rows
Case 2: User has no experiments -- returns one row with just user's info
Case 3: User has experiments -- returns rows for every experiment / observation prompt combination
Case 4: User has experiments and has observations -- returns rows for every experiment / observation prompt combination with observation column filled out
*/

-- User info associated with the public_user_id
WITH identified_user AS
(SELECT
 	u.id AS user_id,
	u.first_name
FROM 
	user_lookups ul, 
	users u
WHERE
	ul.public_user_id = %(public_user_id)s AND -- restrict to the public_user_id
    ul.status = 'active' AND -- ensure the public_user_id is active 
    u.id = ul.user_id), -- restrict to the user associated with the public_user_id

-- All of the experiments, etc. assigned to the user
assigned_experiments AS (
SELECT
	iu.user_id,
	g.id AS group_id,
 	g.group_name, 
	sg.id AS sub_group_id,
 	sg.sub_group_name, 
	ep.id AS experiment_prompt_id,
 	ep.experiment_prompt, 
	op.id AS observation_prompt_id,
 	op.observation_prompt, 
	sga.action_datetime AS display_datetime,
	ep.display_order AS ep_display_order,
	op.display_order AS op_display_order
FROM
	identified_user iu,
	sub_group_actions sga, 
	sub_group_action_templates sgat,
	sub_groups sg, 
	groups g,
	experiment_prompts ep,
	observation_prompts op
WHERE
	sga.user_id = iu.user_id AND sga.status = 'display_after_action_datetime' AND -- restrict to just the sub_group_actions for the user that are flagged to be displayed
	sga.action_datetime < NOW() AT TIME ZONE 'UTC' AND -- restrict to just the sub_group_actions where the action_datetime has already passed (and thus they should be displayed)
	sgat.id = sga.sub_group_action_template_id AND -- identify the sub_group_action_template so that we can identify the sub_group
	sg.id = sgat.sub_group_id AND --restrict to just the relevant sub_groups
	g.id = sg.group_id AND -- restrict to just the relevant groups
	ep.sub_group_id = sg.id AND -- restrict to just the relevant experiment_prompts
	op.experiment_prompt_id = ep.id -- restrict to just the relevant observation prompts
),

-- All of the observations made by the user
user_observations AS (
SELECT 
	o.observation_prompt_id,
	o.id AS observation_id,
	o.observation
FROM
	assigned_experiments ae,
	observations o
WHERE
	o.observation_prompt_id = ae.observation_prompt_id AND -- restrict to observations for relevant observation prompts
	o.user_id = ae.user_id AND -- restrict to observations by the user
	o.status = 'active' -- restrict to active observations
)

--Combine user info, experiment info, and observations
--Note: we do left joins so that we return data if there is an identified user but no experiments (experiments, but no observations)
SELECT
	iu.first_name, 
	ae.display_datetime,
	ae.group_id,
 	ae.group_name, 
	ae.sub_group_id,
 	ae.sub_group_name, 
	ae.experiment_prompt_id,
 	ae.experiment_prompt, 
	ae.observation_prompt_id,
 	ae.observation_prompt, 
	uo.observation_id,
	uo.observation
FROM identified_user iu
LEFT JOIN assigned_experiments ae ON iu.user_id = ae.user_id
LEFT JOIN user_observations uo ON ae.observation_prompt_id = uo.observation_prompt_id
ORDER BY
	display_datetime DESC, -- the most recent exeperiments are shown first
	ep_display_order,
	op_display_order;"""

        # Pull data from database
        df = execute_sql_return_df(sql_statement = sql_statement, sql_params = sql_params, db_conn = db_conn, logger = logger)
        logger.info("Finished retrieving experimenter log data from database")

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

        ## CASE 2: User exists but has no assigned group
        # Test:  df has just one row with just user's info (e.g., group = None)
        # Outcome: Return early with experiments_to_display = False 
        if len(df) == 1 and df.group_name.get(0) == None:
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

        # Convert "dumb" quotes to HTML "curly" quotes
        text_vars = ["group_name", "sub_group_name", "experiment_prompt", "observation_prompt", "observation"]
        for text_var in text_vars:
            df[text_var] = df[text_var].apply(lambda x: smartypants.smartypants(x))

        ## CASE 3: User exists with assigned experiments (and potentially observations)
        # Outcome: Return dict_response with all of the experiments and observations for the user

        ## Update response dictionary
        dict_response["days_of_experimenting"] = (pytz.timezone('UTC').localize(datetime.utcnow()) - df['display_datetime'].min()).days + 1 # Add 1 to include today, take max in case there are no assigned dates
        
        ## Prepare dict_response with all of the experiments and observations for the user

        # Initialize helper array
        array_groups = []

        # Collect data for each experiment group
        for group_id in df.group_id.unique():

            df_group = df.query("group_id == '" + group_id  + "'").reset_index()

            array_sub_groups = []

            # Assemble for each experiment in each experiment sub group
            for sub_group_id in df_group.sub_group_id.unique():
                
                df_sub_group = df_group.query("sub_group_id == '" + sub_group_id  + "'").reset_index()

                # Generate data for all associated experiments with this experiment sub group
                # Methodology: https://stackoverflow.com/questions/55004985/convert-pandas-dataframe-to-json-with-columns-as-key
                # Output Sample: [{'experiment_id': 'e1', 'experiment': 'Celebrate something.', 'data': [{'observation_prompt': 'What makes you happier at work?', 'observation': 'My observation for o1.'}, {'observation_prompt': 'What makes you successful at work?', 'observation': 'My observation for o7.'}]}, {'experiment_id': 'e2', 'experiment': 'Seek feedback from someone on your team.', 'data': [{'observation_prompt': 'What makes you happier at work?', 'observation': 'My observation for o2.'}, {'observation_prompt': 'What makes you successful at work?', 'observation': 'My observation for o8.'}]}, {'experiment_id': 'e3', 'experiment': 'Keep quiet for 10-15 minutes in a meeting (or until someone asks for your input).', 'data': [{'observation_prompt': 'What makes you happier at work?', 'observation': 'My observation for o3.'}, {'observation_prompt': 'What makes you successful at work?', 'observation': 'My observation for o9.'}]}]
                primary_cols = ['experiment_prompt_id', 'experiment_prompt']
                data_cols = ['observation_prompt_id', 'observation_prompt', 'observation']
                dict_experiments = (df_sub_group.groupby(primary_cols)[data_cols]
                    .apply(lambda x: x.to_dict('records'))
                    .reset_index(name='observations')
                    .to_dict(orient='records'))

                # For each experiment, set observations to "None" if there are no observation prompts / observations 
                for index in range(0, len(dict_experiments)):
                    if dict_experiments[index]['observations'] == [{'observation_prompt_id': '', 'observation_prompt': '', 'observation': ''}]:
                        dict_experiments[index]['observations'] = "None"
                
                # Add experiments / observations for this sub_group
                array_sub_groups.append(
                    {"sub_group_id": sub_group_id,
                    "sub_group_name": df_sub_group.sub_group_name.get(0),
                    "sub_group_display_date": df_sub_group.display_datetime.get(0).strftime("%B %#d, %Y"),
                    "experiments": dict_experiments}
                )

            # Add data for particular experiment group
            array_groups.append(
                {"group_id": group_id,
                "group_name": df_group.group_name.get(0),
                "sub_groups": array_sub_groups}
            )

        # Add experiment groups, sub_groups, experiments, and observations to the response dictionary
        dict_response['groups'] = array_groups

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
    
    finally:

        # Close database connection if it exists    
        if db_conn is not None:
            db_conn.close()   
    
    
