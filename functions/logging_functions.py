import sys
import logging #no need to pip install loggin
from logging import FileHandler

# Log handler for writing to the console
def get_console_handler(log_format):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    return console_handler

# Log handler for writing to a file
def get_file_handler(log_file_path, log_format):
    file_handler = FileHandler(filename=log_file_path)
    file_handler.setFormatter(log_format)
    return file_handler

# Return a logger
def get_logger(log_file_path=None, logger_name='Default Logger'):

    # Create a logger
    logger = logging.getLogger(logger_name)

    # Set the log format
    log_format = logging.Formatter(
        "%(asctime)s — %(levelname)s: %(message)s")
    
    # Set level of logging (better to have too much log than not enough)
    logger.setLevel(logging.DEBUG)

    #Log to the console
    logger.addHandler(get_console_handler(log_format=log_format))

    #Log to a file
    if log_file_path is not None:
        logger.addHandler(
            get_file_handler(
                log_file_path=log_file_path, 
                log_format=log_format))
        
    #Don't send any logs to a parent logger (prevent duplicate log messages)
    logger.propagate = False

    return logger

# Close a logger (including releasing any file handlers so you can delete the files as necessary)
def close_logger(logger):

    if logger is None:
        return

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)
