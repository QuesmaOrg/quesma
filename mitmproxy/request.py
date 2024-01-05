from mitmproxy import http
import re

def request(flow: http.HTTPFlow) -> None:
    # Regular expression for matching 'logs-*-*' pattern
    pattern = "logs-"

    # Checking if the URL matches the pattern
    if re.search(pattern, flow.request.pretty_url):
        print("URL matching 'logs-' pattern:", flow.request.pretty_url)