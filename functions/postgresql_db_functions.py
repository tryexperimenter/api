import psycopg2 # pip install psycopg2-binary
import pandas as pd
from honeybadger import honeybadger
import traceback

# %%Example Usage and Postgresql / Python Overview and 

# Documentation: https://www.postgresqltutorial.com/postgresql-python/

# ## Load variables from .env file or OS environment variables; create logger

# # Standard modules
# import psycopg2 # pip install psycopg2-binary
# import pandas as pd
# import os
# import json
# import sys
# import traceback
# from dotenv import dotenv_values # pip install python-dotenv

# # Custom modules
# sys.path.append("./functions")
# from logging_functions import get_logger

# # Load environment variables
# #Sample .env file: PROD_DB_CONNECTION_PARAMETERS={"db": "production_7crrss", "host": "dpg-ch3arqkimipg-a.ohio-postgres.render.com", "user": "admin", "password": "MXObCVERY0rEPyr", "port": "5432"}
# env_vars = {
#     **dotenv_values(r"C:/Users/trist/experimenter/api/.env"),
#     **os.environ,  # override loaded values with environment variables
# }
# db_connection_parameters = json.loads(env_vars.get('PROD_DB_CONNECTION_PARAMETERS'))

# # Create logger
# if 'logger' not in locals():
#     logger = get_logger(logger_name="api")

# ## Sample Function Calls (using functions defined below)
# # We use SQL parameters (rather than inserting user generated info into a SQL statement with f-strings) to prevent SQL injection attacks (https://www.psycopg.org/psycopg3/docs/basic/params.html)

# # SQL statement without parameters
# sql_params = None
# sql_statement = "SELECT * FROM userss;"

# # SQL statement with parameters
# sql_params = {'email': 'sample_user_1@gmail.com', 'first_name': 'Sample'}
# sql_statement = "SELECT * FROM users WHERE email = %(email)s AND first_name = %(first_name)s;"

# # SQL statement that includes an IN statement
# # The parameter used for the IN statement must be a tuple (e.g., tuple(df['email'].unique()))
# sql_params = {'emails': ('sample_user_1@gmail.com', 'sample_user_2')}
# sql_statement = "SELECT * FROM users WHERE email IN %(emails)s;"

# # Use functions

# db_conn = None # initialize db_conn as None so that the finally block doesn't error out if the db_conn variable doesn't exist

# try:

#     db_conn = create_db_connection(db_connection_parameters = db_connection_parameters, logger = logger)

#     df = execute_sql_return_df(sql_statement = sql_statement, sql_params = sql_params, db_conn = db_conn, logger = logger)

# except Exception as e:

#     # Whatever error handling you want to do
#     logger.error(e)

# finally:   
#     # Close database connection if it exists    
#     if db_conn is not None:
#         db_conn.close()


# ## Sample Use of Postgresql Functions

# db_conn = create_db_connection(db_connection_parameters = db_connection_parameters, logger = logger)

# # Postgresql Function Call (no parameters)
# # https://www.postgresqltutorial.com/postgresql-python/postgresql-python-call-postgresql-functions/
# # CREATE OR REPLACE FUNCTION get_all_users()
# # RETURNS setof users
# # LANGUAGE SQL
# # AS $$
# # SELECT * FROM users;
# # $$;
# with db_conn.cursor() as cursor:
#     cursor.callproc('get_all_users')
#     data = cursor.fetchall()
#     col_names = [desc[0] for desc in cursor.description]
#     df_func_no_parameters = pd.DataFrame(data, columns=col_names)

# # Postgresql Function Call (with parameters)
# # https://www.postgresqltutorial.com/postgresql-python/postgresql-python-call-postgresql-functions/
# # CREATE OR REPLACE FUNCTION get_user(email TEXT)
# # RETURNS setof users
# # LANGUAGE SQL
# # AS $$
# # SELECT * FROM users WHERE email = get_user.email;
# # $$;
# with db_conn.cursor() as cursor:
#     cursor.callproc('get_user', ['sampleuser@gmail.com'])
#     data = cursor.fetchall()
#     col_names = [desc[0] for desc in cursor.description]
#     df_func_with_parameters = pd.DataFrame(data, columns=col_names)

# # Close the connection
# if db_conn is not None:
#     db_conn.close()




# %% Create Database Connection
def create_db_connection(db_connection_parameters, logger):

    try: 

        return psycopg2.connect(
            database=db_connection_parameters['db'],
            host=db_connection_parameters['host'],
            user=db_connection_parameters['user'],
            password=db_connection_parameters['password'],
            port=db_connection_parameters['port'])


    except Exception as e:

        error_class = f"API | create_db_connection()"
        error_message = f"create_db_connection() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)


# %% Excute a SQL statement, return dataframe
# We use SQL parameters (rather than inserting user generated info into a SQL statement with f-strings) to prevent SQL injection attacks (https://www.psycopg.org/psycopg3/docs/basic/params.html)
def execute_sql_return_df(sql_statement, sql_params, db_conn, logger):

    try:

        with db_conn.cursor() as cursor:

            if sql_params is None:

                cursor.execute(sql_statement)

            else:

                cursor.execute(sql_statement, sql_params)

            db_conn.commit() # note that any other transactions using this db_conn will be committed as well

            data = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(data, columns=col_names)

    except Exception as e:

        db_conn.rollback() # rollback any changes made to the database during this failed transaction    

        error_class = f"API | execute_sql_return_df()"
        error_message = f"execute_sql_return_df() failed; Error: {e}, SQL Statement: {sql_statement}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)

# %% Excute a SQL statement, return status message (no data returned)
# We use SQL parameters (rather than inserting user generated info into a SQL statement with f-strings) to prevent SQL injection attacks (https://www.psycopg.org/psycopg3/docs/basic/params.html)
def execute_sql_return_status_message(sql_statement, sql_params, db_conn, logger):

    try:

        with db_conn.cursor() as cursor:

            if sql_params is None:

                cursor.execute(sql_statement)

            else:

                cursor.execute(sql_statement, sql_params)
            
            db_conn.commit() # note that any other transactions using this db_conn will be committed as well

            return {"status": "success", "status_message": cursor.statusmessage} # the message like 'INSERT 0 1' that is returned after running a postgresql command

    except Exception as e:

        db_conn.rollback() # rollback any changes made to the database during this failed transaction    

        error_class = f"API | execute_sql_return_status_message()"
        error_message = f"execute_sql_return_status_message() failed; Error: {e}, SQL Statement: {sql_statement}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        
        return {"status": "failure", "status_message": f"error: {e}"}


# %% Excute a SQL statement using executemany, return status message (no data returned)
# Using tuples in executemany should have psycopg2 do sanitation to prevent any sql injection attack
def executemany_sql_return_status_message(sql_statement, tuples, db_conn, logger):

    # Example:
    # tuples = [tuple(x) for x in df_messages[['status', 'sub_group_action_id']].values]
    # sql_statement = 'UPDATE sub_group_actions SET status = %s WHERE id = %s;'

    try:

        with db_conn.cursor() as cursor:

            cursor.executemany(sql_statement, tuples)

            db_conn.commit() # note that any other transactions using this db_conn will be committed as well

            return cursor.statusmessage # the message like 'INSERT 0 1' that is returned after running a postgresql command

    except Exception as e:

        db_conn.rollback() # rollback any changes made to the database during this failed transaction    

        error_class = f"API | execute_sql_return_status_message()"
        error_message = f"execute_sql_return_status_message() failed; Error: {e}, SQL Statement: {sql_statement}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)
