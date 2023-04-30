import re

def text_has_curly_braces(text):
    if re.search(r'{(.*?)}', text):
        return True
    else:
        return False

def text_is_empty(text):
    if text == '' or text is None:
        return True
    else:
        return False

def text_has_only_whitespace(text):
    if re.search(r'^\s*$', text):
        return True
    else:
        return False

def text_is_too_long(text, max_size, logger):
    if len(text) > max_size:
        raise ValueError(f"""text_is_too_long. len(text) = {len(text)} > max_size = {max_size}""")
    else:
        logger.info("text is not too long")

#Validate text is okay to send to a user (not empty, no {variable_x} that haven't been replaced, etc.)
def validate_text(text, logger):
    if text_is_empty(text):
        raise ValueError("text_is_empty")
    elif text_has_only_whitespace(text):
        raise ValueError("text_has_only_whitespace")
    elif text_has_curly_braces(text):
        raise ValueError("text_has_curly_braces")
    else:
        logger.info("text is valid")