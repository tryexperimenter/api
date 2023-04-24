import psycopg2 # pip install psycopg2-binary
import pandas as pd
from honeybadger import honeybadger
import traceback

# %%Postgresql / Python Overview

# Documentation: https://www.postgresqltutorial.com/postgresql-python/

# ## Load variables from .env file or OS environment variables
# #Sample .env file: PROD_DB_CONNECTION_PARAMETERS={"db": "production_7crrss", "host": "dpg-ch3arqkimipg-a.ohio-postgres.render.com", "user": "admin", "password": "MXObCVERY0rEPyr", "port": "5432"}
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


# Create Database Connection
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

# Get Experimenter Log Data
def db_get_experimenter_log_data(public_user_id: str, db_conn, logger):

    try:
        
        with db_conn.cursor() as cursor:
            # Call the get_experimenter_log_data() postgres function defined in Supabase
            # Case 1: Public_user_id is not found / not active -- returns no rows
            # Case 2: User has no experiments -- returns one row with just user's info
            # Case 3: User has experiments -- returns rows for every experiment / observation prompt combination
            # Case 4: User has experiments and has observations -- returns rows for every experiment / observation prompt combination with observation column filled out
            cursor.callproc('get_experimenter_log_data', [public_user_id])

            data = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            return pd.DataFrame(data, columns=col_names)

    except Exception as e:

        error_class = f"API | db_get_experimenter_log_data()"
        error_message = f"db_get_experimenter_log_data() failed; Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        
        raise Exception(error_message)

