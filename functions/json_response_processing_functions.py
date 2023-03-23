import json
from honeybadger import honeybadger

def create_json_response(dict_response: dict, logger) -> dict:

    #json.dumps() turns Python single quotes into JSON formatted double quotes and returns a string. json.loads() turns it into an actual json object
    try:
        
        return json.loads(json.dumps(dict_response, allow_nan=False)) # FastAPI does a json.dumps(, allow_nan=False) and will throw an error if there are an NaN values, so we catch them earlier
    
    except Exception as e:

        error_class = f"API | create_json_response()"
        error_message = f"Error with create_json_response(); Error: {e}"
        logger.error(error_message)
        honeybadger.notify(error_class=error_class, error_message=error_message)