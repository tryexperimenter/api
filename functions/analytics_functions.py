from datetime import datetime
from google_sheets_functions import append_data_to_google_sheet
from honeybadger import honeybadger
import traceback

# Custom imports
from postgresql_db_functions import create_db_connection, execute_sql_return_status_message

def log_api_call(environment, endpoint, db_connection_parameters, logger):

    try:

        # Get database connection
        db_conn = None # initialize db_conn as None so that the finally block doesn't error out if the db_conn variable doesn't exist
        db_conn = create_db_connection(db_connection_parameters, logger)

        logger.info(f"Logging api call in api_calls table: environment: {environment}, endpoint: {endpoint}")

        sql_statement = f"INSERT INTO api_calls (environment, endpoint) VALUES ('{environment}', '{endpoint}');"

        response = execute_sql_return_status_message(sql_statement, db_conn, logger)

        logger.info(f"Logging api call in api_calls table: response: {response}")

    # If there is an error, log it
    # Note that we are not raising an error because we don't want to interrupt the API call
    except Exception as e:

        error_class = f"API | log_api_call()"
        error_message = f"Error with log_api_call(); Error: {e}"
        logger.error(error_message)
        logger.error(traceback.format_exc()) # provide the full traceback of everything that caused the error
        honeybadger.notify(error_class=error_class, error_message=error_message)        

    finally:

        # Close database connection if it exists    
        if db_conn is not None:
            db_conn.close()        
