import glob
import os
import json
from urllib.parse import ParseResult, urlparse
from threading import Lock

from mitmproxy import http
from mitmproxy import io

LOG_DIR = "/var/mitmproxy/requests/"


class Writer:
    def __init__(self) -> None:
        self.w = io.FlowWriter()
        self.req_nr = 0
        self.lock = Lock() # only for self.req_nr
        # clean requests on (re)start
        for file in glob.glob(os.path.join(LOG_DIR, '*.http')):
            os.remove(file)

    def response(self, flow: http.HTTPFlow) -> None:
        self.w.add(flow)


writer = Writer()


def record_request(flow: http.HTTPFlow) -> None:
    with writer.lock:
        writer.req_nr += 1
        cur_req_nr = writer.req_nr

    with open(os.path.join(LOG_DIR, str(cur_req_nr) + '.http'), "ab") as ofile:
        url = urlparse(flow.request.url)
        trimmed_url_to_save = ParseResult('', '', *url[2:]).geturl() # save only e.g. /(index/)/_search
        ofile.write((trimmed_url_to_save + "\n").encode())

        body = flow.request.content.decode('utf-8')
        body_json = json.loads(body)
        ofile.write(json.dumps(body_json, indent=4).encode())

    writer.response(flow)


def request(flow: http.HTTPFlow) -> None:
    parsed_url = urlparse(flow.request.url)
    url_path = parsed_url.path
    search_methods = ['/_search', '/_async_search', '/_terms_enum']

    for method in search_methods:
        if url_path.endswith(method):
            # so far we skip requests with prefixes: . and /.
            # maybe that's to be changed
            if len(url_path) > 0 and url_path[0] == '.':
                break
            if len(url_path) > 1 and url_path[:2] == '/.':
                break
            record_request(flow)
            break
