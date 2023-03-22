import json

def create_json_response(dict_response: dict, logger) -> dict:

    #json.dumps() turns Python single quotes into JSON formatted double quotes and returns a string. json.loads() turns it into an actual json object
    try:
        
        return json.loads(json.dumps(dict_response, allow_nan=False)) # FastAPI does a json.dumps(, allow_nan=False) and will throw an error if there are an NaN values, so we catch them earlier
    
    except Exception as e:
        logger.error(f"Error with converting dict_response to JSON object")
        logger.error(f"Error: {e}")
        raise TypeError("dict_response has NaN values")