# Note that you'll need to enable all of the actions you want to take in SendGrid's UI when you create the API key (e.g., scheduledule sends)
# https://docs.sendgrid.com/api-reference/mail-send/mail-send

# %% Local Testing

# ## Imports
# import os
# from dotenv import dotenv_values # pip install python-dotenv
# from sendgrid import SendGridAPIClient
# from datetime import datetime, timedelta, timezone
# import pandas as pd
# # Custom functions
# from logging_functions import get_logger

# ## Set up logging
# if 'logger' not in locals():
#     logger = get_logger(logger_name = "api")

# ## Load variables from .env file or OS environment variables
# env_vars = {
#     **dotenv_values(r"C:/Users/trist/experimenter/api/.env"),
#     **os.environ,  # override loaded values with environment variables
# }

# ## Create client
# sendgrid_api_key = env_vars.get('SENDGRID_API_KEY')
# sendgrid_client = SendGridAPIClient(sendgrid_api_key)

# ## Testing

# # Send email
# datetime_utc_to_send =  datetime.utcnow() + timedelta(hours = 75) # you can schedule emails up to 72 hours in advance
# dict_response = send_email(
#     datetime_utc_to_send =  datetime_utc_to_send,
#     from_email = 'experiments@tryexperimenter.com', 
#     from_display_name = 'Experimenter',
#     to_email = 'tristan@tryexperimenter.com', 
#     subject = 'Test', 
#     message_text_html = "test", 
#     add_unsubscribe_link = True,
#     sendgrid_client = sendgrid_client, 
#     logger = logger)

# dict_response

# # Cancel scheduled email
# cancel_scheduled_emails_for_batch_id(
#     batch_id = 'NGJkNWU0OTctZTllNy0xMWVkLTg2NjAtNTZmYTA3MDc2NTlkLTU0MmY2ZDZiZA',
#     sendgrid_client = sendgrid_client,
#     logger = logger)


# %% Functions

def send_email(
    to_email, 
    subject, 
    message_text_html, 
    add_unsubscribe_link,
    sendgrid_client,
    from_email, 
    from_display_name, #e.g., 'Tristan from Experimenter' to show up as 'Tristan from Experimenter <experiments@tryexperimenter.com>'
    logger,
    datetime_utc_to_send=None,
    ):

    from email_validator import validate_email #https://pypi.org/project/email-validator/
    from sendgrid.helpers.mail import Mail, To, From, Subject, Bcc, Content, SendAt, BatchId
    from datetime import datetime, timezone

    #Custom functions
    from message_validation_functions import validate_text

    # Instantiate response dictionary
    dict_response = {}

    # Add unsubscribe link
    if add_unsubscribe_link:
        # Link is grey and underlined
        message_text_html += f"""<br><br><a href="https://www.tryexperimenter.com/unsubscribe" style="text-decoration: underline; color: #959595; cursor: pointer">Unsubscribe</a>"""

    # Validate text is okay to send to a user (not empty, no {variable_x} that haven't been replaced, etc.)
    validate_text(text=subject, logger=logger)
    validate_text(text=message_text_html, logger=logger)

    # Validate email address (throws exception if invalid or undeliverable)
    validate_email(to_email, check_deliverability=True)

    # Log what we're doing
    logger.info("***Sending email using SendGrid***")
    logger.info(f"from_email: {from_email}")
    logger.info(f"from_name: {from_display_name}")
    logger.info(f"to_email: {to_email}")
    logger.info(f"subject: {subject}")
    logger.info(f"message_text_html: {message_text_html}")

    # Create message
    message = Mail()

    # Set message details
    message.from_email = From(email=from_email, name=from_display_name)
    message.to = To(email=to_email)
    message.subject = Subject(subject)
    message.bcc = Bcc(email=from_email) #so that the from_email can have the email in their Gmail search (otherwise, it won't be seen)
    message.content = Content(mime_type="text/html", content = message_text_html)

    # Set time to send message
    # (a) Immediately
    if datetime_utc_to_send is None:

        logger.info(f"send_time (UTC): immediately")

    # (b) Scheduled
    else: 

        logger.info(f"send_time (UTC): {datetime_utc_to_send}")

        # Create batch_id (used to cancel scheduled messages)
        # https://docs.sendgrid.com/api-reference/cancel-scheduled-sends/create-a-batch-id
        batch_id = sendgrid_client.client.mail.batch.post()
        batch_id = eval(batch_id.body.decode())['batch_id'] #decode() converts bytes to string, eval() converts string to dictionary
        logger.info(f"batch_id: {batch_id}")
        message.batch_id = BatchId(batch_id)
        dict_response['batch_id'] = batch_id

        # Convert datetime to Unix timestamp (required by SendGrid: https://docs.sendgrid.com/api-reference/mail-send/mail-send)
        send_at_unix_timestamp = int(datetime_utc_to_send.replace(tzinfo=timezone.utc).timestamp())
        logger.info(f"send_at_unix_timestamp: {send_at_unix_timestamp}")
        message.send_at = SendAt(send_at_unix_timestamp)
    
    # Send / schedule email and record response
    try:
    
        response = sendgrid_client.send(message)
        # logger.info(response.status_code)
        # logger.info(response.body)
        # logger.info(response.headers)

        status_code = response.status_code
        dict_response['datetime_created'] = datetime.utcnow()
        
    

        # Success
        if status_code == 202:
            logger.info(f'''Email accepted by SendGrid with status_code: {status_code} (202 = success)''')    
            dict_response['message_successfully_processed'] = True
            dict_response['x_message_id'] = response.headers.get('X-Message-Id')

        # Failure
        else:
            logger.error(f"Email rejected by SendGrid with status code: {status_code}")
            dict_response['message_successfully_processed'] = False
            dict_response['error_message'] = f"SendGrid API status code: {status_code}"

    # Failure
    except Exception as e:

        logger.error(f"Email rejected by SendGrid with error: {e}")
        dict_response['message_successfully_processed'] = False
        dict_response['error_message'] = f"SendGrid error: {e}"

    return dict_response


def cancel_scheduled_emails_for_batch_id(
    batch_id, # e.g., ZmNlNGQwNDItYXYlZC0xMWVkLWIxNDEtYmFmYWU4MDE3YTQ5LTM2NzkxNDlkOA
    sendgrid_client,
    logger,
):

    logger.info("***Cancel Scheduled batch_id Emails***")
    logger.info(f"batch_id: {batch_id}")

    try:

        response = sendgrid_client.client.user.scheduled_sends.post(
            request_body= {
                "batch_id": batch_id,
                "status": "cancel"
            }
        )

        logger.info(f"API status_code (201=success): {response.status_code}")

    except Exception as e:
        logger.error(f"Error cancelling emails for batch_id ({batch_id}): {e}")
        response = e

    return response

# %%
