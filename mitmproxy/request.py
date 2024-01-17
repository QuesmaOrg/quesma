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

def record_response(index_name, method, flow: http.HTTPFlow) -> None:
    with open(os.path.join(LOG_FILE_PREFIX, index_name + '.txt'), "ab") as ofile:
        ofile.write(flow.request.pretty_url.encode())

        ofile.write(b"\n Request:\n")
        if flow.request.content:
            ofile.write(flow.request.content)
        
        body = flow.request.content.decode('utf-8')
        parse_json_body(index_name, method, body, ofile)

        ofile.write(b"\n Response:\n")
        if flow.response.content:
            ofile.write(flow.response.content)

        # Add other separators etc. however you want
        ofile.write(b"\n-------\n")
    writer.response(flow)

def extract_index_name(parsed_url, method):
    result = parsed_url.path[:-len(method)]
    if result.startswith('/'):
        result = result[1:]

    result = result.replace('*', 'X') # For convience, replace wildcard with X
    if result.startswith('.'): # For convience, remove leading dot
        result = result[1:]
    
    if len(result) == 0:
        return 'default'
    return result


def response(flow: http.HTTPFlow) -> None:
    parsed_url = urlparse(flow.request.url)
    url_path = parsed_url.path
    search_methods = ['/_search', '/_eql/search', '/_query', '/_msearch', '/_async_search',
                      '/_pit', '/_terms_enum', '/_search/scroll', '_search/template', '/_msearch/template']
    
    for method in search_methods:
        if url_path.endswith(method):
            # Uncomment below to debug
            # print("ES Query detected, response", parsed_url)
            index_name = extract_index_name(parsed_url, method)
            record_response(index_name, method, flow)
            break


def request(flow: http.HTTPFlow) -> None:
    parsed_url = urlparse(flow.request.url)
    url_path = parsed_url.path
    search_methods = ['/_search', '/_eql/search', '/_query', '/_msearch', '/_async_search',
                      '/_pit', '/_terms_enum', '/_search/scroll', '_search/template', '/_msearch/template']
    
    for method in search_methods:
        if url_path.endswith(method):
            break # No-op on purpose for now
