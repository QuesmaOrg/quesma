from enum import Enum
import glob
import os
import json
from urllib.parse import ParseResult, urlparse
from threading import Lock

from mitmproxy import http
from mitmproxy import io

LOG_DIR = "/var/mitmproxy/requests/"
START_RECORDING_PATH = '/___quesma_e2e_recorder_start'
STOP_RECORDING_PATH  = '/___quesma_e2e_recorder_stop'
CLEAN_RECORDING_PATH = '/___quesma_e2e_recorder_clean'
SAVE_RECORDING_PATH  = '/___quesma_e2e_recorder_save'


class Writer:
    def __init__(self) -> None:
        filename = os.path.join(LOG_DIR, "requests.http")
        self.f: BinaryIO = open(filename, "wb")
        self.w = io.FlowWriter(self.f)
        self.saved_requests_nr = 0
        self.recording_on = False
        self.lock = Lock() # only for self.req_nr
        # clean requests on (re)start
        for file in glob.glob(os.path.join(LOG_DIR, '*.http')):
            os.remove(file)

    def response(self, flow: http.HTTPFlow) -> None:
        self.w.add(flow)


writer = Writer()


def record_request(flow: http.HTTPFlow) -> None:
    with writer.lock:
        writer.saved_requests_nr += 1
        cur_req_nr = writer.saved_requests_nr

    with open(os.path.join(LOG_DIR, str(cur_req_nr) + '.http'), "ab") as ofile:
        url = urlparse(flow.request.url)
        trimmed_url_to_save = ParseResult('', '', *url[2:]).geturl() # save only e.g. /(index/)/_search
        ofile.write((trimmed_url_to_save + "\n").encode())

        body = flow.request.content.decode('utf-8')
        body_json = json.loads(body)
        ofile.write(json.dumps(body_json, indent=4).encode())

    writer.response(flow)


def start_recording() -> None:
    print("----------------------- Starting e2e recording")
    with writer.lock:
        writer.recording_on = True
    print("----------------------- e2e recording started")


def stop_recording() -> None:
    print("----------------------- Stopping e2e recording")
    with writer.lock:
        writer.recording_on = False
    print("----------------------- e2e recording stopped")


def clean_recording() -> None:
    print("----------------------- Cleaning e2e requests")
    with writer.lock:
        for file in glob.glob(os.path.join(LOG_DIR, '*.http')):
            os.remove(file)
        writer.saved_requests_nr = 0
    print("----------------------- e2e requests cleaned")


def save_recording() -> None:
    print("----------------------- Saving recording")


def request(flow: http.HTTPFlow) -> None:
    parsed_url = urlparse(flow.request.url)
    url_path = parsed_url.path
    print("p", parsed_url, "u",url_path)

    meta_requests = { # url -> handler
        START_RECORDING_PATH: start_recording,
        STOP_RECORDING_PATH: stop_recording,
        CLEAN_RECORDING_PATH: clean_recording,
        SAVE_RECORDING_PATH: save_recording,
    }
    if url_path in meta_requests:
        meta_requests[url_path]()
        return

    with writer.lock:
        if not writer.recording_on:
            return

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
