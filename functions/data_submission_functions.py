import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
import pytz
from honeybadger import honeybadger
import traceback
import smartypants

# Custom imports
from postgresql_db_functions import create_db_connection, execute_sql_return_df, execute_sql_return_status_message

# %% Retrieve user_id

def retrieve_user_id_from_public_user_id(
        public_user_id,
        db_conn,
        logger):
    
    try:

        # Define sql query (use parameters rather than f-string to avoid SQL injection)
        sql_params = {'public_user_id': public_user_id}
        sql_statement = """
SELECT id AS user_lookup_id, user_id, status
FROM user_lookups
WHERE public_user_id = %(public_user_id)s;"""

        # Execute sql query
        df = execute_sql_return_df(sql_statement=sql_statement, sql_params=sql_params, db_conn=db_conn, logger=logger)

        # Raise error if multiple user_ids found
        if len(df) > 1:
            raise ValueError(f"Multiple user_ids found for public_user_id: {public_user_id}")
        
        # Format return dictionary
        if len(df) == 0:

            logger.info(f"retrieve_user_id_from_public_user_id(): no user_id found")

            dict_return = {
                "user_lookup_id": None,
                "user_id": None,
                "status": None}
            
        elif len(df) == 1:
            logger.info(f"retrieve_user_id_from_public_user_id(): user_id found")
            dict_return = {
                "user_lookup_id": df['user_lookup_id'].iloc[0],
                "user_id": df['user_id'].iloc[0],
                "status": df['status'].iloc[0]}
        
        return dict_return
    
    # Catch any exceptions as we tried to execute the function
    except Exception as e:

        error_class = f"API | retrieve_user_id_from_public_user_id()"
        error_message = f"Error for public_user_id ={public_user_id}; Error: {e}"
        logger.error(error_class)
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)
    

# %% Submit observation

def submit_observation(
        public_user_id, 
        observation_prompt_id, 
        visibility, 
        observation, 
        db_connection_parameters,
        logger):
    
    try:

        # %%% Setup database connection

        db_conn = None # initialize db_conn as None so that the finally block doesn't error out if the db_conn variable doesn't exist
        db_conn = create_db_connection(db_connection_parameters, logger)

        # %%% Retrive user_id from public_user_id

        logger.info(f"Calling retrieve_user_id_from_public_user_id() for public_user_id: {public_user_id}")

        dict_user_id = retrieve_user_id_from_public_user_id(
            public_user_id=public_user_id,
            db_conn=db_conn,
            logger=logger)
        
        user_id = dict_user_id['user_id']
        
        # If user_id not found, return error message
        if user_id is None:

            raise ValueError(f"user_id not found for public_user_id: {public_user_id}")

        # %%% Update prior observation (if it exists) to status = 'inactive' and save observation_id

        logger.info(f"Update prior observation (if it exists) to status = 'inactive' and save observation_id")

        # Define sql query
        sql_params = {
            'user_id': user_id,
            'observation_prompt_id': observation_prompt_id}
        sql_statement = """
UPDATE observations
SET status = 'inactive'
WHERE 
	user_id = %(user_id)s AND
	observation_prompt_id = %(observation_prompt_id)s AND
	status = 'active' -- we only want to set any active observation to inactive
RETURNING id AS observation_id;"""

        # Execute sql query
        df = execute_sql_return_df(sql_statement=sql_statement, sql_params=sql_params, db_conn=db_conn, logger=logger)

        # Store observation_id if an update was made
        if len(df) > 0:
            
            observation_id = df['observation_id'].iloc[0]

            logger.info(f"Updated prior observation to status = 'inactive' for observation_id: {observation_id}")

        else:

            logger.info("No prior observation to update to status = 'inactive'")

            observation_id = None

        # %%% Add new observation to database

        logger.info(f"Add new observation to database")

        # Define sql query
        sql_params = {
            'user_id': user_id,
            'observation_prompt_id': observation_prompt_id,
            'visibility': visibility,
            'observation': observation}
        sql_statement = """
INSERT INTO observations(user_id, observation_prompt_id, observation, visibility)
VALUES (%(user_id)s, %(observation_prompt_id)s, %(observation)s, %(visibility)s);"""

        # Execute sql query
        response = execute_sql_return_status_message(sql_statement=sql_statement, sql_params=sql_params, db_conn=db_conn, logger=logger)

        # %%% Success creating new observation
        if (response["status"] == "success" and 
            response["status_message"] == "INSERT 0 1"):

            logger.info(f"Success: created new observation for public_user_id: {public_user_id} and observation_prompt_id: {observation_prompt_id}")

            return {"status": "success"}

        # %%% Failure creating new observation
        else:

            logger.info(f"Failure: did not create new observation for public_user_id: {public_user_id} and observation_prompt_id: {observation_prompt_id}")

            # If we set original observation to inactive, set it back to active since we did not create a new observation
            try:

                if observation_id is not None:

                    # Define sql query
                    sql_params = {
                        'observation_id': observation_id}
                    sql_statement = """
UPDATE observations
SET status = 'active'
WHERE id = %(observation_id)s;"""

                    # Execute sql query
                    response = execute_sql_return_status_message(sql_statement=sql_statement, sql_params=sql_params, db_conn=db_conn, logger=logger)

                    # Success
                    if (response["status"] == "success" and 
                        response["status_message"] == "UPDATE 1"):

                        logger.info(f"Success: updated original observation back to status = 'active' for observation_id: {observation_id}")

                    # Failure
                    if (response["status"] == "failure"):

                        raise ValueError(f"Failure: did not update original observation back to status = 'active' for observation_id: {observation_id}")

            except Exception as e:

                error_class = f"API | submit_observation()"
                error_message = f"Error: {e}"
                logger.error(error_message)
                logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
                honeybadger.notify(error_class=error_class, error_message=error_message)

            return {"status": "failure"}
    
    # Catch any exceptions as we tried to execute the function
    except Exception as e:

        error_class = f"API | submit_observation()"
        error_message = f"public_user_id: {public_user_id}, observation_prompt_id: {observation_prompt_id}, visibility: {visibility}, observation: {observation}; Error: {e}"
        logger.error(error_class)
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)

        return {"status": "failure"}
    
    finally:

        # Close database connection if it exists    
        if db_conn is not None:
            db_conn.close()   
    
    

