# https://developers.short.io/reference/linkspost

import requests

def generate_short_url(long_url, short_io_api_key):

    short_domain = "link.tryexperimenter.com"

    url = "https://api.short.io/links"

    payload = {
        "domain": short_domain,
        "originalURL": long_url
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": short_io_api_key
    }

    # Generate short url
    response = requests.post(url, json=payload, headers=headers)

    # Raise error (if there is one)
    response.raise_for_status()

    # Extract and return the short url (without providing https://)
    short_url = short_domain + '/' + response.json().get('path')
    return short_url