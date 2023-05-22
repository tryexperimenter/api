import psycopg2 # pip install psycopg2-binary
import pandas as pd
from honeybadger import honeybadger
import traceback

# %%Postgresql / Python Overview

# Documentation: https://www.postgresqltutorial.com/postgresql-python/

# ## Load variables from .env file or OS environment variables
# #Sample .env file: PROD_DB_CONNECTION_PARAMETERS={"db": "production_7crrss", "host": "dpg-ch3arqkimipg-a.ohio-postgres.render.com", "user": "admin", "password": "MXObCVERY0rEPyr", "port": "5432"}
# import psycopg2 # pip install psycopg2-binary
# import pandas as pd
# import os
# import json
# from dotenv import dotenv_values # pip install python-dotenv
# env_vars = {
#     **dotenv_values(r"C:/Users/trist/experimenter/api/.env"),
#     **os.environ,  # override loaded values with environment variables
# }
# db_connection_parameters = json.loads(env_vars.get('PROD_DB_CONNECTION_PARAMETERS'))

# ## Sample Postgresql Uses
# def sample_postgresql_uses(db_connection_parameters):

#     conn = None

#     try:

#         conn = psycopg2.connect(
#             database=db_connection_parameters['db'],
#             host=db_connection_parameters['host'],
#             user=db_connection_parameters['user'],
#             password=db_connection_parameters['password'],
#             port=db_connection_parameters['port'])
        
#         # Raw SQL
#         with conn.cursor() as cursor:
#             cursor.execute("SELECT * FROM users;")
#             data = cursor.fetchall()
#             col_names = [desc[0] for desc in cursor.description]
#             df_raw_sql = pd.DataFrame(data, columns=col_names)

#         # Postgresql Function Call (no parameters)
#         # https://www.postgresqltutorial.com/postgresql-python/postgresql-python-call-postgresql-functions/
#         # CREATE OR REPLACE FUNCTION get_all_users()
#         # RETURNS setof users
#         # LANGUAGE SQL
#         # AS $$
#         # SELECT * FROM users;
#         # $$;
#         with conn.cursor() as cursor:
#             cursor.callproc('get_all_users')
#             data = cursor.fetchall()
#             col_names = [desc[0] for desc in cursor.description]
#             df_func_no_parameters = pd.DataFrame(data, columns=col_names)

#         # Postgresql Function Call (with parameters)
#         # https://www.postgresqltutorial.com/postgresql-python/postgresql-python-call-postgresql-functions/
#         # CREATE OR REPLACE FUNCTION get_user(email TEXT)
#         # RETURNS setof users
#         # LANGUAGE SQL
#         # AS $$
#         # SELECT * FROM users WHERE email = get_user.email;
#         # $$;
#         with conn.cursor() as cursor:
#             cursor.callproc('get_user', ['tristanzucker@gmail.com'])
#             data = cursor.fetchall()
#             col_names = [desc[0] for desc in cursor.description]
#             df_func_with_parameters = pd.DataFrame(data, columns=col_names)

#         return [df_raw_sql, df_func_no_parameters, df_func_with_parameters]

#     # Close the connection
#     finally:
#         print("Closing the connection.")
#         if conn is not None:
#             conn.close()

# dfs = sample_postgresql_uses(db_connection_parameters)


## Create Database Connection
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


## Excute a SQL statement, return dataframe
def execute_sql_return_df(sql_statement, db_conn, logger):

    try:

        with db_conn.cursor() as cursor:

            cursor.execute(sql_statement)

            db_conn.commit() # note that any other transactions using this db_conn will be committed as well

            data = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(data, columns=col_names)

    except Exception as e:

        error_class = f"API | execute_sql_return_df()"
        error_message = f"execute_sql_return_df() failed; Error: {e}, SQL Statement: {sql_statement}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)

## Excute a SQL statement, return status message (no data returned)
def execute_sql_return_status_message(sql_statement, db_conn, logger):

    try:

        with db_conn.cursor() as cursor:

            cursor.execute(sql_statement)
            
            db_conn.commit() # note that any other transactions using this db_conn will be committed as well

            return cursor.statusmessage

    except Exception as e:

        error_class = f"API | execute_sql_return_status_message()"
        error_message = f"execute_sql_return_status_message() failed; Error: {e}, SQL Statement: {sql_statement}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)


## Excute a SQL statement using execute man, return status message (no data returned)
def executemany_sql_return_status_message(sql_statement, tuples, db_conn, logger):

    # Example:
    # tuples = [tuple(x) for x in df_messages[['status', 'sub_group_action_id']].values]
    # sql_statement = 'UPDATE sub_group_actions SET status = %s WHERE id = %s;'

    try:

        with db_conn.cursor() as cursor:

            cursor.executemany(sql_statement, tuples)

            db_conn.commit() # note that any other transactions using this db_conn will be committed as well

            return cursor.statusmessage

    except Exception as e:

        error_class = f"API | execute_sql_return_status_message()"
        error_message = f"execute_sql_return_status_message() failed; Error: {e}, SQL Statement: {sql_statement}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)
