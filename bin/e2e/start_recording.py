#!/usr/bin/env python3
import argparse
import requests
from env import Action, get_url

help_msg = '''
Sends a request to mitmproxy to start recording requests which are coming from Kibana/OpenSearch.

Mitmproxy should be running with the e2e_request_recorder.py addon,
which is currently active for both 'local-dev' and 'local-dev-dual-comparison' configs.

If it doesn't, it's a no-op.

Technically, it sends a GET "/<index that can't exist>_start" request, which should always return a 400.

Result of the script is either:
a) Recording is ON! - recording (if enabled) either just started or was and still is ON
b) Error - recording mode stayed unchanged
'''

if __name__ == "__main__":
    argparse.ArgumentParser(description=help_msg, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    url = get_url(Action.START)
    print("-- Sending request: GET", url)
    resp = requests.get(url)
    if resp.status_code == 400:
        print("-- Recording is ON! (if it's enabled)")
    else:
        print("-- Error starting recording:", resp.text)