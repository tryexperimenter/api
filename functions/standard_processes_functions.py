import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
import pytz
from honeybadger import honeybadger
import traceback
import smartypants

# Custom imports
from postgresql_db_functions import db_return_df_from_arbitrary_sql_statement
from short_io_functions import generate_short_url


# Schedule messages to be sent out in the next 48 hours
def schedule_messages(db_conn, sendgrid_api_key, short_io_api_key, logger) -> dict:

    try:

        ## Identify messages to schedule

        # Define sql query
        sql_statement = """
SELECT
	sg.id AS sub_group_id,
	u.first_name,
	u.email AS user_email,
    u.url_stub_experimenter_log,
	g.group_name,
	sg.sub_group_name,
	sgat.email_subject,
	sgat.email_body,
    sga.action_datetime
FROM 
	sub_group_actions sga, 
	group_assignments ga, 
	sub_group_action_templates sgat, 
	sub_groups sg, 
	groups g, 
	users u
WHERE
	sga.status IN ('message_to_be_scheduled', 'message_failed_to_schedule') AND -- restrict to messages that need to be scheduled
	sga.action_datetime BETWEEN NOW() AND NOW() + interval '1000 hours' AND -- restrict to the message needs to be sent in the next 48 hours
	ga.status = 'active' AND -- restrict to messages for groups where the user is an active participant (not paused, canceled). note that we should set sub_group_actions.status = 'canceled' if the user pauses a group, so this is just a double precaution
	sgat.id = sga.sub_group_action_template_id AND -- pull on sub_group_action_templates info (email_subject, email_body, sub_group_id)
	sg.id = sgat.sub_group_id AND -- pull on sub_groups info (sub_group_name, group_id)
	g.id = sg.group_id AND -- pull on groups info (group_name)
	u.id = sga.user_id; -- pull on user info (email, first_name)"""
        logger.info(f"Messages to Schedule SQL Statement: {sql_statement}")

        # Pull data from database
        df_messages = db_return_df_from_arbitrary_sql_statement(sql_statement = sql_statement, db_conn = db_conn, logger = logger)

        # If no messages to schedule, return empty dictionary
        if len(df_messages) == 0:
            logger.info("No messages to schedule")
            return {"message": "No messages to schedule"}

        ## Identify experiment prompts to include in messages

        # Define sql query (pull experiment prompts for each sub_group_id associated with a message we want to schedule)
        sub_group_ids = df_messages['sub_group_id'].unique()
        sub_group_ids = "'" + '\',\''.join(sub_group_ids) + "'" # generates a string of sub_group_ids separated by commas to use in a sql query (e.g., '66cb527749c57fb78d6f','3166b227e57b89f3d68d','0644ddb2baea156e84b8','f8b84bf9de73b95f5702','63c09c65c6b1822f11ad')
        sql_statement = f"""
SELECT 
	sub_group_id,
	experiment_prompt,
	display_order
FROM experiment_prompts
WHERE sub_group_id IN ({sub_group_ids});"""
        logger.info(f"Experiment Prompts SQL Statement: {sql_statement}")

        # Pull data from database
        df_experiment_prompts = db_return_df_from_arbitrary_sql_statement(sql_statement = sql_statement, db_conn = db_conn, logger = logger)        

        # Create dictionary of experiment prompts (each sub_group_id is a key, and the value is an ordered list of experiment prompts)
        # Example
        # In: df = pd.DataFrame([{"sub_group_id":"66cb527749c57fb78d6f","experiment_prompt":"Seek input from a total stranger.","display_order":1},{"sub_group_id":"66cb527749c57fb78d6f","experiment_prompt":"Ask for a favor.","display_order":2},{"sub_group_id":"66cb527749c57fb78d6f","experiment_prompt":"Discuss an \"undiscussable\" issue.","display_order":3},{"sub_group_id":"3166b227e57b89f3d68d","experiment_prompt":"Learn something from a mistake.","display_order":1},{"sub_group_id":"3166b227e57b89f3d68d","experiment_prompt":"Play devil's advocate.","display_order":2},{"sub_group_id":"3166b227e57b89f3d68d","experiment_prompt":"Practice active listening.","display_order":3},{"sub_group_id":"0644ddb2baea156e84b8","experiment_prompt":"Ask \"why do you think this is important?\"","display_order":1},{"sub_group_id":"0644ddb2baea156e84b8","experiment_prompt":"Get a (virtual) coffee with a colleague you don't know that well.","display_order":2},{"sub_group_id":"0644ddb2baea156e84b8","experiment_prompt":"Celebrate a colleague (perhaps even publicly).","display_order":3},{"sub_group_id":"63c09c65c6b1822f11ad","experiment_prompt":"Take an hour (or two) to do something you've been putting off.","display_order":1},{"sub_group_id":"63c09c65c6b1822f11ad","experiment_prompt":"When someone does something that doesn't make sense, take 1 minute think about why they did it.","display_order":2},{"sub_group_id":"63c09c65c6b1822f11ad","experiment_prompt":"Cross one thing off your to do list without actually doing it.","display_order":3}])
        # Out: {'0644ddb2baea156e84b8': ['Ask "why do you think this is important?"', "Get a (virtual) coffee with a colleague you don't know that well.", 'Celebrate a colleague (perhaps even publicly).'], '3166b227e57b89f3d68d': ['Learn something from a mistake.', "Play devil's advocate.", 'Practice active listening.'], '63c09c65c6b1822f11ad': ["Take an hour (or two) to do something you've been putting off.", "When someone does something that doesn't make sense, take 1 minute think about why they did it.", 'Cross one thing off your to do list without actually doing it.'], '66cb527749c57fb78d6f': ['Seek input from a total stranger.', 'Ask for a favor.', 'Discuss an "undiscussable" issue.']}
        df_experiment_prompts = df_experiment_prompts.sort_values(
            by=['sub_group_id', 'display_order'],
            ascending=[True, True])
        dict_experiment_prompts = df_experiment_prompts.groupby('sub_group_id').agg(list).to_dict()['experiment_prompt']
        
        # TODO: Need to loop through and create experiment_prompt_1, experiment_prompt_2, etc. variables so that we can do text replacement
        # Nahh... create experiment_prompts = dict['0644ddb2baea156e84b8'] and then use experiment_prompts[0], experiment_prompts[1], etc.
        counter = 1
        experiment_prompts = dict['0644ddb2baea156e84b8']
        for experiment_prompt in experiment_prompts:
            exec(f"experiment_prompt_{counter} = '{experiment_prompt}'")
            

        ## Generate URLs to include in messages
        logger.info(f"""Generating URLs to include in messages""")

        # Prepare experiments_week column (when using Tally.co for recording reflections)
        df['experiment_week'] = df['sub_group_name'].str.split('Week ', expand=True)[1].astype(int)

        # Long reflection URL
        df['url_long_record_observations'] = df.apply(
            lambda row: f"https://tryexperimenter.com/observations?first_name={row['first_name']}&user_email={row['user_email']}&experiments=wk{row['experiment_week']}&url_stub_experimenter_log={row['url_stub_experimenter_log']}", 
            axis=1)

        # Reflection URL
        df['url_record_observations'] = df.apply(
            lambda row: generate_short_url(
                long_url = row['url_long_record_observations'], 
                short_io_api_key=short_io_api_key),
            axis=1)

        # Long reflection URL (prior week)
        df['url_long_record_observations_prior_week'] = df.apply(
            lambda row: f"https://tryexperimenter.com/observations?first_name={row['first_name']}&user_email={row['user_email']}&experiments=wk{row['experiment_week']-1}&url_stub_experimenter_log={row['url_stub_experimenter_log']}", 
            axis=1)

        # Reflection URL (prior week)
        df['url_record_observations_prior_week'] = df.apply(
            lambda row: generate_short_url(
                long_url = row['url_long_record_observations_prior_week'], 
                short_io_api_key=short_io_api_key),
            axis=1)

        # Experimenter Log URL
        df['url_experimenter_log'] = df.apply(
            lambda row: f"tryexperimenter.com/{row['url_stub_experimenter_log']}",
            axis=1)

        ## Return df as dictionary
        df = df.drop(columns = ['action_datetime'])
        df = df_experiments
        return df.to_dict(orient='records')

    except Exception as e:

        error_class = f"API | schedule_messages()"
        error_message = f"schedule_messages() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)
