#!/usr/bin/env python3
import os

LOG_FILE_PREFIX = "/var/mitmproxy/requests/"
QUERY_FILE_PREFIX = "/var/mitmproxy/query/"
PLACEHOLDER=".placeholder"

def _delete_files_in_dir(dir_path):
    if not os.path.isdir(dir_path):
        print(f"Error: {dir_path} is not a directory.")
        return
    
    for filename in os.listdir(dir_path): 
        file_path = os.path.join(dir_path, filename)
        if filename != PLACEHOLDER and os.path.isfile(file_path):
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

def delete_log_files():
    _delete_files_in_dir(LOG_FILE_PREFIX)
    _delete_files_in_dir(QUERY_FILE_PREFIX)

print("clean.py: Start.")
delete_log_files()
print("clean.py: Done.")