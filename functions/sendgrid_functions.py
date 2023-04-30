
# https://docs.sendgrid.com/api-reference/mail-send/mail-send

# Note that you'll need to enable all of the actions you want to take in SendGrid's UI when you create the API key (e.g., scheduledule sends)
# from sendgrid import SendGridAPIClient
# sendgrid_client = SendGridAPIClient(sendgrid_api_key)

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

    # Validate text is okay to send to a user (not empty, no {variable_x} that haven't been replaced, etc.)
    validate_text(text=subject, logger=logger)
    validate_text(text=message_text_html, logger=logger)

    # Instantiate response dictionary
    dict_response = {}

    # Add unsubscribe link
    if add_unsubscribe_link:
        # Link is grey and underlined
        message_text_html += f"""<br><br><a href="https://www.tryexperimenter.com/unsubscribe" style="text-decoration: underline; color: #959595; cursor: pointer">Unsubscribe</a>"""

    # Log what we're doing
    logger.info("***Sending email using SendGrid***")
    logger.info(f"from_email: {from_email}")
    logger.info(f"from_name: {from_display_name}")
    logger.info(f"to_email: {to_email}")
    logger.info(f"subject: {subject}")
    logger.info(f"message_text_html: {message_text_html}")

    # Validate email address (throws exception if invalid or undeliverable)
    validate_email(to_email, check_deliverability=True)

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
    
    # Send / schedule email
    # response = sendgrid_client.send(message)
    # dict_response['datetime_created'] = datetime.utcnow()
    # dict_response['status_code'] = response.status_code

    # Log response
    # logger.info(f'''SendGrid API response status_code: {dict_response['status_code']}''')

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
