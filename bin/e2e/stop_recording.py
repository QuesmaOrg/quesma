#!/usr/bin/env python3
import argparse
import requests
from env import Action, get_url

help_msg = '''
Sends a request to mitmproxy to stop recording requests which are coming from Kibana/OpenSearch.

Mitmproxy should be running with the e2e_request_recorder.py addon,
which is currently active for both 'local-dev' and 'local-dev-dual-comparison' configs.

If it doesn't, it's a no-op.

Technically, it sends a GET "/<index that can't exist>_stop" request, which should always return a 400.

Result of the script is either:
a) Recording is OFF! - recording either just stopped or was and still is OFF
b) Error - recording mode stayed unchanged
'''

if __name__ == "__main__":
    argparse.ArgumentParser(description=help_msg, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args()
    url = get_url(Action.STOP)
    print("-- Sending request: GET", url)
    resp = requests.get(url)
    if resp.status_code == 400:
        print("-- Recording is OFF!")
    else:
        print("-- Error stopping recording:", resp.text)