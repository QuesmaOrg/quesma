from mitmproxy import http, ctx
from urllib.parse import urlparse
import json
import os

def remove_host_from_url(url: str) -> str:
    parsed_url = urlparse(url)
    # Combine the path, query, and fragment to get the full URL without the host
    return f"{parsed_url.path}{parsed_url.query and '?' + parsed_url.query or ''}{parsed_url.fragment and '#' + parsed_url.fragment or ''}"

def is_matching_path(url: str) -> bool:
    if url.startswith("/kibana_sample_data"): # Ignore internal indexes
        return True
    return False

LOG_FILE_QUERY_PREFIX = "/var/mitmproxy/requests/"
FILE_NAME = "recorded_traffic.json"

file_name = os.path.join(LOG_FILE_QUERY_PREFIX, FILE_NAME)

def response(flow: http.HTTPFlow) -> None:
    with open(file_name, "a") as f:
        request, response = flow.request, flow.response
        request_path = remove_host_from_url(request.url)
        if not is_matching_path(request_path):
            return
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