# -*- coding: utf-8 -*-
"""
Internal utilities.
"""
import requests

def check_response(resp):
    """
    Make sure that a request succeeded.

    Args:
        resp: requests.Response

    Raises:
        requests.RequestException: if the response is not 200 (request failure)
        OR contains an 'error' field (query failure).
    """
    if resp.status_code != 200:
        print(resp.status_code)
        raise requests.RequestException('Request failed [%d]'%resp.status_code, response=resp)
    else:
        # it's possible for the request itself succeed, but the query to fail.
        j = resp.json()
        if 'error' in j:
            print(resp.status_code, j['error'])
            raise requests.RequestException('Query failed [%s]'%j['error'], response=resp)

