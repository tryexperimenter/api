# import pandas as pd
# import numpy as np
# from datetime import datetime
# import pytz
# import smartypants
# from time import sleep
from honeybadger import honeybadger
import traceback
from sendgrid import SendGridAPIClient
from datetime import datetime

# Custom imports
from postgresql_db_functions import create_db_connection, execute_sql_return_df, executemany_sql_return_status_message
from short_io_functions import generate_short_url
from sendgrid_functions import send_email


# Schedule messages to be sent out in the next 72 hours
def schedule_messages(db_connection_parameters, sendgrid_api_key, short_io_api_key, logger) -> dict:

    try:

        # Get database connection
        db_conn = None # initialize db_conn as None so that the finally block doesn't error out if the db_conn variable doesn't exist
        db_conn = create_db_connection(db_connection_parameters, logger)

        ## Create connection to SendGrid (for sending emails)
        # Note that you'll need to enable all of the actions you want to take in SendGrid's UI when you create the API key (e.g., scheduledule sends)
        sendgrid_client = SendGridAPIClient(sendgrid_api_key)

        ## Identify messages to schedule
        logger.info("Identify messages to schedule")

        # Define sql query (use parameters rather than f-string to avoid SQL injection)
        sql_params = None
        sql_statement = """
SELECT
    sga.id AS sub_group_action_id,
	sg.id AS sub_group_id,
	u.first_name,
	u.email AS user_email,
    u.url_stub_experimenter_log,
	g.group_name,
	sg.sub_group_name,
	sgat.email_subject,
	sgat.email_body,
    sga.action_datetime,
    sga.status
FROM 
	sub_group_actions sga, 
	sub_group_action_templates sgat, 
	sub_groups sg, 
	groups g, 
	users u,
	group_assignments ga
WHERE
	sga.status IN ('message_to_be_scheduled', 'message_failed_to_schedule') AND -- restrict to messages that need to be scheduled
	sga.action_datetime BETWEEN NOW() + interval '30 minutes' AND NOW() + interval '72 hours' AND -- restrict to the message needs to be sent in the next 72 hours (the furtherest in advance that SendGrid will schedule an email), but no earlier than 30 minutes from now (to allow time for the message to be scheduled and SendGrid's 15 minute cutoff for scheduled messages)
	sgat.id = sga.sub_group_action_template_id AND -- pull on sub_group_action_templates info (email_subject, email_body, sub_group_id)
	sg.id = sgat.sub_group_id AND -- pull on sub_groups info (sub_group_name, group_id)
	g.id = sg.group_id AND -- pull on groups info (group_name)
	u.id = sga.user_id AND -- pull on user info (email, first_name)
	ga.status = 'active' AND ga.user_id = u.id AND ga.group_id = g.id; -- restrict to messages for groups where the user is an active participant (not paused, canceled). note that we should set sub_group_actions.status = 'canceled' if the user pauses a group, so this is just a double precaution"""

        # Pull data from database
        df_messages = execute_sql_return_df(sql_statement = sql_statement, sql_params = sql_params, db_conn = db_conn, logger = logger)

        ## If no messages to schedule, send status email and return message
        if len(df_messages) == 0:
            
            logger.info("No messages to schedule")

            send_email(
                from_email = 'experiments@tryexperimenter.com', 
                from_display_name = 'Experimenter',
                to_email = 'tristan@tryexperimenter.com', 
                subject = f'schedule_messages() - {datetime.now().strftime("%Y-%m-%d")} - no messages to schedule', 
                message_text_html = 'There were no messages to schedule.', 
                add_unsubscribe_link = False,
                sendgrid_client = sendgrid_client, 
                logger = logger)

            return {"message": "No messages to schedule."}

        # Select the sender email address
        df_messages['sender_email'] = 'experiments@tryexperimenter.com'
        df_messages['sender_display_name'] = 'Experimenter'

        # Initialize "status_note" column to record why a message failed to schedule
        df_messages['status_note'] = ''

        ## Identify experiment prompts to include in messages
        logger.info("Identify experiment prompts to include in messages")

        # Define sql query (pull experiment prompts for each sub_group_id associated with a message we want to schedule)
        sub_group_ids = tuple(df_messages['sub_group_id'].unique())
        sql_params = {'sub_group_ids': sub_group_ids}
        sql_statement = """
SELECT 
	sub_group_id,
	experiment_prompt,
	display_order
FROM experiment_prompts
WHERE sub_group_id IN %(sub_group_ids)s;"""

        # Pull data from database
        logger.info(f"sub_group_ids: {sub_group_ids}")
        df_experiment_prompts = execute_sql_return_df(sql_statement = sql_statement, sql_params = sql_params, db_conn = db_conn, logger = logger)        

        # Create dictionary of experiment prompts (each sub_group_id is a key, and the value is an ordered list of experiment prompts)
        # Example
        # In: df_experiment_prompts = pd.DataFrame([{"sub_group_id":"66cb527749c57fb78d6f","experiment_prompt":"Seek input from a total stranger.","display_order":1},{"sub_group_id":"66cb527749c57fb78d6f","experiment_prompt":"Ask for a favor.","display_order":2},{"sub_group_id":"66cb527749c57fb78d6f","experiment_prompt":"Discuss an \"undiscussable\" issue.","display_order":3},{"sub_group_id":"3166b227e57b89f3d68d","experiment_prompt":"Learn something from a mistake.","display_order":1},{"sub_group_id":"3166b227e57b89f3d68d","experiment_prompt":"Play devil's advocate.","display_order":2},{"sub_group_id":"3166b227e57b89f3d68d","experiment_prompt":"Practice active listening.","display_order":3},{"sub_group_id":"0644ddb2baea156e84b8","experiment_prompt":"Ask \"why do you think this is important?\"","display_order":1},{"sub_group_id":"0644ddb2baea156e84b8","experiment_prompt":"Get a (virtual) coffee with a colleague you don't know that well.","display_order":2},{"sub_group_id":"0644ddb2baea156e84b8","experiment_prompt":"Celebrate a colleague (perhaps even publicly).","display_order":3},{"sub_group_id":"63c09c65c6b1822f11ad","experiment_prompt":"Take an hour (or two) to do something you've been putting off.","display_order":1},{"sub_group_id":"63c09c65c6b1822f11ad","experiment_prompt":"When someone does something that doesn't make sense, take 1 minute think about why they did it.","display_order":2},{"sub_group_id":"63c09c65c6b1822f11ad","experiment_prompt":"Cross one thing off your to do list without actually doing it.","display_order":3}])
        # Out: dict_experiment_prompts = {'0644ddb2baea156e84b8': ['Ask "why do you think this is important?"', "Get a (virtual) coffee with a colleague you don't know that well.", 'Celebrate a colleague (perhaps even publicly).'], '3166b227e57b89f3d68d': ['Learn something from a mistake.', "Play devil's advocate.", 'Practice active listening.'], '63c09c65c6b1822f11ad': ["Take an hour (or two) to do something you've been putting off.", "When someone does something that doesn't make sense, take 1 minute think about why they did it.", 'Cross one thing off your to do list without actually doing it.'], '66cb527749c57fb78d6f': ['Seek input from a total stranger.', 'Ask for a favor.', 'Discuss an "undiscussable" issue.']}
        df_experiment_prompts = df_experiment_prompts.sort_values(
            by=['sub_group_id', 'display_order'],
            ascending=[True, True])
        dict_experiment_prompts = df_experiment_prompts.groupby('sub_group_id').agg(list).to_dict()['experiment_prompt']
        
        ## Generate URLs to include in messages
        logger.info(f"""Generating URLs to include in messages""")

        # Prepare experiments_week column (when using Tally.co for recording reflections)
        df_messages['experiment_week'] = df_messages['sub_group_name'].str.split('Week ', expand=True)[1].astype(int)

        # Long reflection URL
        df_messages['url_long_record_observations'] = df_messages.apply(
            lambda row: f"https://tryexperimenter.com/observations?first_name={row['first_name']}&user_email={row['user_email']}&experiments=wk{row['experiment_week']}&url_stub_experimenter_log={row['url_stub_experimenter_log']}", 
            axis=1)

        # Reflection URL
        df_messages['url_record_observations'] = df_messages.apply(
            lambda row: generate_short_url(
                long_url = row['url_long_record_observations'], 
                short_io_api_key=short_io_api_key),
            axis=1)

        # Long reflection URL (prior week)
        df_messages['url_long_record_observations_prior_week'] = df_messages.apply(
            lambda row: f"https://tryexperimenter.com/observations?first_name={row['first_name']}&user_email={row['user_email']}&experiments=wk{row['experiment_week']-1}&url_stub_experimenter_log={row['url_stub_experimenter_log']}", 
            axis=1)

        # Reflection URL (prior week)
        df_messages['url_record_observations_prior_week'] = df_messages.apply(
            lambda row: generate_short_url(
                long_url = row['url_long_record_observations_prior_week'], 
                short_io_api_key=short_io_api_key),
            axis=1)

        # Experimenter Log URL
        df_messages['url_experimenter_log'] = df_messages.apply(
            lambda row: f"tryexperimenter.com/{row['url_stub_experimenter_log']}",
            axis=1)

        ## Fill in variables in email_subject, email_body and schedule email
        logger.info(f"""Filling in variables in email_subject, email_body and scheduling emails""")
        for index, (
            sub_group_id,
            sub_group_action_id,
            sender_email,
            sender_display_name,
            user_email,
            first_name,
            group_name,
            sub_group_name,
            email_subject,
            email_body,
            action_datetime,
            url_record_observations, 
            url_record_observations_prior_week,
            url_experimenter_log,
            ) in \
        enumerate(zip(
            df_messages['sub_group_id'],
            df_messages['sub_group_action_id'],
            df_messages['sender_email'],
            df_messages['sender_display_name'],
            df_messages['user_email'],
            df_messages['first_name'],
            df_messages['group_name'],
            df_messages['sub_group_name'],
            df_messages['email_subject'],
            df_messages['email_body'],
            df_messages['action_datetime'],
            df_messages['url_record_observations'],
            df_messages['url_record_observations_prior_week'],
            df_messages['url_experimenter_log'],
        )):

            logger.info(f"Scheduling email for user_email: {user_email}; sub_group_action_id: {sub_group_action_id}")

            # Initialize email_ready_to_schedule
            email_ready_to_schedule = False

            # Replace '' with 'ERROR!!!' for each variable we want to use in email_subject, email_body so that we can catch variable replacements that will just be empty strings (e.g., "Hey {first_name}!" gets turned into  "Hey !" if first_name is ''))})
            if first_name == '': first_name = 'ERROR!!!'
            if group_name == '': group_name = 'ERROR!!!'
            if sub_group_name == '': sub_group_name = 'ERROR!!!'
            if url_record_observations == '': url_record_observations = 'ERROR!!!'
            if url_record_observations_prior_week == '': url_record_observations_prior_week = 'ERROR!!!'
            if url_experimenter_log == '': url_experimenter_log = 'ERROR!!!'

            # Create experiment_prompts list (referenced in email_body as {experiment_prompts[0]}, {experiment_prompts[1]}, etc. and filled in below using email_body = email_body.format(**locals()))
            try:

                experiment_prompts = dict_experiment_prompts[sub_group_id]

            except KeyError: # If there are no experiment prompts for this sub_group_id (e.g., it's a rest week)

                logger.info(f"""No experiment prompts for sub_group_id {sub_group_id}""")

            # Fill in variables in email_subject, email_body
            try:

                email_subject = email_subject.format(**locals())
                df_messages.loc[index, ['email_subject']] = email_subject

                email_body = email_body.format(**locals())
                df_messages.loc[index, ['email_body']] = email_body

                if "ERROR!!!" in email_subject: raise ValueError(f"""email_subject: we attempted to replace a variable with an empty string""")

                if "ERROR!!!" in email_body: raise ValueError(f"""email_body: we attempted to replace a variable with an empty string""")
            
                # If we made it this far, email is ready to schedule
                email_ready_to_schedule = True

            except Exception as e:

                # Log error
                error_message = f"schedule_messages() error filling in email_subject, email_body for sub_group_action_id = {sub_group_action_id}; Error: {e}"
                logger.error(error_message)
                logger.error(traceback.format_exc())

                # Update status, status_note for error
                df_messages.loc[index, ['status']] = 'message_failed_to_schedule'
                df_messages.loc[index, ['status_note']] = error_message

            # Schedule email
            try:

                if email_ready_to_schedule: # If there was no error filling in email_subject, email_body 

                    # Schedule email
                    dict_response = send_email(
                        datetime_utc_to_send = action_datetime,
                        from_email = sender_email, 
                        from_display_name = sender_display_name,
                        to_email = user_email, 
                        subject = email_subject, 
                        message_text_html = email_body, 
                        add_unsubscribe_link = True,
                        sendgrid_client = sendgrid_client, 
                        logger = logger)
                    
                    # Update df_messages for outcome of attempt to schedule email
                    if dict_response['message_successfully_processed'] == True:
                        df_messages.loc[index, ['status']] = 'message_scheduled'
                        df_messages.loc[index, ['enqueued_datetime']] = dict_response['enqueued_datetime']
                        df_messages.loc[index, ['batch_id']] = dict_response['batch_id']
                        df_messages.loc[index, ['x_message_id']] = dict_response['x_message_id']
                        
                    else:
                        df_messages.loc[index, ['status']] = 'message_failed_to_schedule'
                        df_messages.loc[index, ['status_note']] = dict_response['error_message']                    

            except Exception as e:

                # Log error
                error_message = f"schedule_messages() error scheduling email for sub_group_action_id = {sub_group_action_id}; Error: {e}"
                logger.error(error_message)
                logger.error(traceback.format_exc())

                # Update df_messages for outcome of attempt to schedule email
                df_messages.loc[index, ['status']] = 'message_failed_to_schedule'
                df_messages.loc[index, ['status_note']] = error_message


        ## Update sub_group_actions table
        logger.info(f"Update sub_group_actions table")

        try:

            tuples = [tuple(x) for x in df_messages[['status', 'sub_group_action_id']].values]

            sql_statement = 'UPDATE sub_group_actions SET status = %s WHERE id = %s;'

            response = executemany_sql_return_status_message(sql_statement, tuples, db_conn, logger)

            logger.info(f"Update sub_group_actions table response: {response}")

        except Exception as e:

            error_message = f"schedule_messages() error updating sub_group_actions table; Error: {e}"
            logger.error(error_message)
            logger.error(traceback.format_exc())


        ## Update sub_group_action_emails table
        logger.info(f"Update sub_group_action_emails table")

        try:

            # Only add rows for messages that were successfully scheduled
            df_scheduled_messages = df_messages[df_messages['status'] == 'message_scheduled']

            if len(df_scheduled_messages) > 0:

                tuples = [tuple(x) for x in df_scheduled_messages[[
                    'sub_group_action_id',
                    'status',
                    'x_message_id',
                    'batch_id',
                    'sender_email',
                    'user_email',
                    'email_subject',
                    'email_body',
                    'enqueued_datetime',
                    'action_datetime']].values]

                sql_statement = '''INSERT INTO sub_group_action_emails (
sub_group_action_id, 
status, 
twilio_x_message_id, 
twilio_batch_id, 
sender, 
recipient, 
email_subject, 
email_body, 
enqueued_datetime, 
scheduled_datetime)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''

                response = executemany_sql_return_status_message(sql_statement, tuples, db_conn, logger)

                logger.info(f"Update sub_group_action_emails table response: {response}")
            else:

                logger.info(f"No rows to add to sub_group_action_emails table")

        except Exception as e:
                
            # Log error
            error_message = f"schedule_messages() error updating sub_group_action_emails table; Error: {e}"
            logger.error(error_message)
            logger.error(traceback.format_exc())
                
        ## Send status email update, return outcome of schedule_messages()
        logger.info(f"Send status email update, return outcome of schedule_messages()")

        try:

            # Define status message subject
            status_message_subject = f'schedule_messages() - {datetime.now().strftime("%Y-%m-%d")} - scheduled: {len(df_messages[df_messages["status"] == "message_scheduled"])}, failed: {len(df_messages[df_messages["status"] != "message_scheduled"])}'
            logger.info(f"status_message_subject: {status_message_subject}")

            # Add basic info on all scheduled messages to status_message_text
            df_scheduled_messages = df_messages[df_messages['status'] == 'message_scheduled']
            status_message_text = f"Scheduled messages: {len(df_scheduled_messages)} <br><br>"
            if len(df_scheduled_messages) > 0:
                for (
                    user_email,
                    email_subject,
                    action_datetime,
                    ) in \
                zip(
                    df_scheduled_messages['user_email'],
                    df_scheduled_messages['email_subject'],
                    df_scheduled_messages['action_datetime'],
                    ):

                    status_message_text += f"{user_email} - {email_subject} - {action_datetime} UTC<br>"

            # Add basic info on all failed messages to status_message_text
            df_failed_messages = df_messages[df_messages['status'] != 'message_scheduled']
            status_message_text += f"<br>Failed messages: {len(df_failed_messages)}<br><br>"
            if len(df_failed_messages) > 0:
                for (
                    user_email,
                    email_subject,
                    action_datetime,
                    status,
                    status_note
                    ) in \
                zip(
                    df_failed_messages['user_email'],
                    df_failed_messages['email_subject'],
                    df_failed_messages['action_datetime'],
                    df_failed_messages['status'],
                    df_failed_messages['status_note'],
                    ):

                    status_message_text += f"{user_email} - {email_subject} - {action_datetime} UTC - {status} - {status_note}<br>"
            logger.info(f"status_message_text: {status_message_text}")

            # Send email to experiments@tryexperimenter.com with summary of what happened
            dict_response = send_email(
                from_email = 'experiments@tryexperimenter.com', 
                from_display_name = 'Experimenter',
                to_email = 'tristan@tryexperimenter.com', 
                subject = status_message_subject, 
                message_text_html = status_message_text, 
                add_unsubscribe_link = False,
                sendgrid_client = sendgrid_client, 
                logger = logger)
            
            # Return API response
            return {"message": status_message_text}
                
        except Exception as e:

            # Log error
            error_message = f"schedule_messages() error recording outcome of schedule_message(); Error: {e}"
            logger.error(error_message)
            logger.error(traceback.format_exc())

    except Exception as e:

        error_class = f"API | schedule_messages()"
        error_message = f"schedule_messages() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)

    finally:

        # Close database connection if it exists    
        if db_conn is not None:
            db_conn.close()        