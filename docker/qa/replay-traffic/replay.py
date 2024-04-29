import requests
import json
import jsondiff
import sys
import os
from datetime import datetime
import pathlib

URL_BASE = "http://mitmproxy:8080"

IGNORED_HTTP_HEADERS = [
    "x-quesma-headers-source", "x-quesma-source", "content-length", "transfer-encoding",
    "date", "x-opaque-id" # We maybe want to support them one day
]

# directory matches the repo structure
LOG_FILE_QUERY_PREFIX = "/docker/qa/results/"

logFile = None

def logger(*args):
    print(*args, file=logFile)

def store_evidence(request_id, suffix,  data):
    global logDir
    fname = f"{request_id}-{suffix}"
    with open(f"{logDir}/{fname}", "w") as f:
        f.write(data)
    return fname

def prepare_headers(headers):
    if 'Accept-Encoding' not in headers:
        headers['Accept-Encoding'] = None
    return headers

def normalize_headers(headers) -> dict:
    headers = {k.lower(): v for k, v in headers.items()}
    for ignored_key in IGNORED_HTTP_HEADERS:
        if ignored_key in headers:
            del headers[ignored_key]
    return headers

def normalize_response_body(body_str):
    try:
        body = json.loads(body_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing body: {e} for body: {body_str[:100]}")
        return {}

    for ignored_key in ["id", "start_time_in_millis", "expiration_time_in_millis", "completion_time_in_millis"]:
        if ignored_key in body:
            del body[ignored_key]

    if "response" in body:
        if "took" in body["response"]:
            del body["response"]["took"]

    return body



def replay_request(request_id: str, raw_str: str) -> bool:
    try:
        logger("START: replaying request: ", request_id)
        data = json.loads(raw_str)

        method = data["request"]["method"]
        url_path = data["request"]["path"]
        url = URL_BASE + url_path
        headers = data["request"]["headers"]
        body = data["request"]["body"]

        logger("Replaying request: ", method, url)

        headers = prepare_headers(headers)

        response = requests.request(method, url, headers=headers, data=body)

        original_response_headers = data["response"]["headers"]
        response_headers = response.headers
        original_response_body = data["response"]["body"]
        response_body = response.text

        # Get rid of CaseInsensitiveDict
        original_response_headers = normalize_headers(original_response_headers)
        response_headers = normalize_headers(response_headers)

        # Normalize the body
        original_response_body = normalize_response_body(original_response_body)
        response_body = normalize_response_body(response_body)

        # Compare the headers
        headers_diff = jsondiff.diff(original_response_headers, response_headers, syntax='explicit')
        body_diff = jsondiff.diff(original_response_body, response_body, syntax='explicit')
        if headers_diff or body_diff:
            logger("FAIL:", request_id)
            if headers_diff:
                logger("Headers diff: ", store_evidence(request_id, "diff-headers.json", json.dumps(headers_diff)))
                logger("Original headers: ", store_evidence(request_id, "original-headers.json", json.dumps(original_response_headers)))
                logger("Response headers: ", store_evidence(request_id, "response-headers.json", json.dumps(response_headers)))
            if body_diff:
                logger("Body diff: ", store_evidence(request_id, "diff-body.json", json.dumps(original_response_body)))
                logger("Original body chunk: ", store_evidence(request_id, "original-body.json", json.dumps(original_response_body)))
                logger("Response body: ", store_evidence(request_id, "response-body.json", json.dumps(response_body)))
            return False
        else:
            logger("PASS: Responses matched", request_id)
            return True

    except json.JSONDecodeError as e:
        logger(f"Error: {e}")
        logger("Failed to decode JSON: ", raw_str, "request_id: ", request_id)

def replay_traffic(file_path):
    with open(file_path, "r") as f:
        lines = f.readlines()

    i, request_count, failed_count = 0, 0, 0
    while i < len(lines):
        j = i + 1
        while j < len(lines) and lines[j].rstrip() != "}":
            j += 1
        data = "".join(lines[i:j+1])
        request_count += 1
        request_id = f"{file_path}:{request_count}"
        request_id = request_id.replace("/", "_")
        request_id = request_id.replace(".", "_")
        request_id = request_id.replace(":", "_")
        res = replay_request(request_id, data.strip())
        if not res:
            failed_count += 1
        i = j + 1

    if failed_count > 0:
        logger(f"Failed to match {failed_count} responses our of {request_count}.")
        return False
    else:
        logger(f"All {request_count} responses matched.")
        return True

def find_json_files():
    directory = "data"
    try:
        # List all files in the specified directory
        return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.json')]
        # Print the list of JSON files

    except FileNotFoundError:
        logger(f"The directory {directory} does not exist.")
    except PermissionError:
        logger(f"Permission denied for accessing the directory {directory}.")

    return []

def main():
    global logFile
    global logDir
    print("Started replaying traffic.")

    logDir = os.path.join(LOG_FILE_QUERY_PREFIX, datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))
    pathlib.Path(logDir).mkdir(parents=True, exist_ok=True)
    replay_log = logDir + "/replay.log"
    print("Logging to file", replay_log)

    with open(replay_log, "w") as logFile:

        # Specify the path to your recorded file
        files = find_json_files()
        logger(f"Replaying traffic from files {', '.join(files)}")

        for file in files:
            res = replay_traffic(file)

            if not res:
                print("Aborting replay. Errors found.")
                sys.exit(1)

    print("Finished replaying traffic.")

main()
