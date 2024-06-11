import glob
import os
import json
from mitmproxy import http
from urllib.parse import urlparse
from typing import BinaryIO

from mitmproxy import http
from mitmproxy import io
import query

LOG_FILE_PREFIX = "/var/mitmproxy/requests/"
MITM_FILE = os.path.join(LOG_FILE_PREFIX, "requests.mitm")
TXT_FILE = os.path.join(LOG_FILE_PREFIX, "requests.txt")


class Writer:
    def __init__(self, path: str) -> None:
        self.f: BinaryIO = open(path, "ab")
        self.w = io.FlowWriter(self.f)
        self.req_nr = 1
        for file in glob.glob(os.path.join(LOG_FILE_PREFIX, '*.http')):
            os.remove(file)

    def response(self, flow: http.HTTPFlow) -> None:
        self.w.add(flow)

    def done(self):
        self.f.close()


writer = Writer(MITM_FILE)


def parse_json_body(index_name, method, body, ofile):
    try:
        json_body = json.loads(body)
        if 'query' in json_body:
            query.parsed_query_json(index_name, method, json_body['query'])
            query_body = json_body['query']
            filter_only = ('bool' in query_body and 'filter' in query_body['bool'])
            for field in ['must', 'must_not', 'should']:
                if field in query_body:
                    if len(query_body[field]) > 0:
                        filter_only = False
            if filter_only:
                ofile.write(b"\n Query filter:\n")
                query_body = query_body['bool']['filter']
            else:
                ofile.write(b"\n Query:\n")

            ofile.write(json.dumps(query_body, indent=2).encode())
    except:
        pass


def record_request(index_name, method, flow: http.HTTPFlow) -> None:
    with open(os.path.join(LOG_FILE_PREFIX, str(writer.req_nr) + '.http'), "ab") as ofile:
        writer.req_nr += 1 # TODO add atomic
        ofile.write(flow.request.pretty_url.encode() + "\n".encode())

        body = flow.request.content.decode('utf-8')
        body_json = json.loads(body)
        ofile.write(json.dumps(body_json, indent=4).encode())
        # parse_json_body(index_name, method, body, ofile)

    writer.response(flow)


def extract_index_name(parsed_url, method):
    result = parsed_url.path[:-len(method)]
    if result.startswith('/'):
        result = result[1:]

    result = result.replace('*', 'X')  # For convience, replace wildcard with X

    if len(result) == 0:
        return 'default'
    return result


def request(flow: http.HTTPFlow) -> None:
    parsed_url = urlparse(flow.request.url)
    url_path = parsed_url.path
    search_methods = ['/_search', '/_async_search', '/_terms_enum']

    for method in search_methods:
        if url_path.endswith(method):
            index_name = extract_index_name(parsed_url, method)
            print(index_name)
            if len(index_name) > 0 and index_name[0] != ".":
                record_request(index_name, method, flow)
            break
