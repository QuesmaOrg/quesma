from mitmproxy import http, ctx
from urllib.parse import urlparse
import json
import os
import itertools
from datetime import datetime

def remove_host_from_url(url: str) -> str:
    parsed_url = urlparse(url)
    # Combine the path, query, and fragment to get the full URL without the host
    return f"{parsed_url.path}{parsed_url.query and '?' + parsed_url.query or ''}{parsed_url.fragment and '#' + parsed_url.fragment or ''}"

def is_matching_path(url: str) -> bool:
    if url.startswith("/kibana_sample_data"): # Ignore internal indexes
        return True
    return False

LOG_FILE_QUERY_PREFIX = "/var/mitmproxy/requests/"

requestDir = os.path.join(LOG_FILE_QUERY_PREFIX, datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))
os.makedirs(requestDir, exist_ok=True)

print("Logging requests to directory", requestDir)

# this is a global counter
cont = itertools.count()



def next_file_name():
    global requestDir
    global cont
    value = next(cont)
    return f"{requestDir}/req-{value:05d}.json"


def response(flow: http.HTTPFlow) -> None:
    request, response = flow.request, flow.response
    request_path = remove_host_from_url(request.url)
    if not is_matching_path(request_path):
        return

    fname = next_file_name()
    print("Writing request to file", fname)
    with open(fname, "w") as f:
        data = {
            "request": {
                "method": request.method,
                "path": request_path,
                "headers": dict(request.headers),
                "body": request.content.decode('utf-8', errors='replace')
            },
            "response": {
                "headers": dict(response.headers),
                "body": response.content.decode('utf-8', errors='replace')
            }
        }
        json.dump(data, f, indent=4)
        f.write("\n")