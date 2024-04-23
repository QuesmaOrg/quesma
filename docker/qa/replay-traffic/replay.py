import requests
import json
import jsondiff
import sys
import os

URL_BASE = "http://mitmproxy:8080"

IGNORED_HTTP_HEADERS = [
    "x-quesma-headers-source", "x-quesma-source", "content-length", "transfer-encoding",
    "date", "x-opaque-id" # We maybe want to support them one day
]

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

def replay_request(raw_str: str) -> bool:
    try:
        data = json.loads(raw_str)

        method = data["request"]["method"]
        url_path = data["request"]["path"]
        url = URL_BASE + url_path
        headers = data["request"]["headers"]
        body = data["request"]["body"]

        print("Replaying request: ", method, url)

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
            print("FAIL: Mismatch in responses")
            if headers_diff:
                print("  Headers diff: ", headers_diff)
                # print("  Original headers: ", json.dumps(original_response_headers)[:100])
                # print("  Response headers: ", json.dumps(response_headers)[:100])
            if body_diff:
                print("  Body diff: ", body_diff)
                print("  Original body chunk: ", json.dumps(original_response_body)[:100])
                print("  Response body: ", json.dumps(response_body)[:100])
            return False
        else:
            print("PASS: Responses matched")
            return True

    except json.JSONDecodeError as e:
        print(f"Error: {e}")
        print("Failed to decode JSON: ", raw_str)

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
        res = replay_request(data.strip())
        if not res:
            failed_count += 1
        i = j + 1

    if failed_count > 0:
        print(f"Failed to match {failed_count} responses our of {request_count}.")
        sys.exit(1)
    else:
        print(f"All {request_count} responses matched.")

def find_json_files():
    directory = "data"
    try:
        # List all files in the specified directory
        return [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.json')]
        # Print the list of JSON files

    except FileNotFoundError:
        print(f"The directory {directory} does not exist.")
    except PermissionError:
        print(f"Permission denied for accessing the directory {directory}.")

    return []


# Specify the path to your recorded file
files = find_json_files()
print(f"Replaying traffic from files {', '.join(files)}")

for file in files:
    replay_traffic(file)