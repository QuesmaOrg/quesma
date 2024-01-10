#!/usr/bin/env python3
import os

LOG_FILE_PREFIX = "/var/mitmproxy/requests/"
PLACEHOLDER=".placeholder"

def delete_log_files():
    # Check if LOG_FILE_PREFIX is a directory
    if not os.path.isdir(LOG_FILE_PREFIX):
        print(f"Error: {LOG_FILE_PREFIX} is not a directory.")
        return

    # List all files in the directory
    for filename in os.listdir(LOG_FILE_PREFIX):
        file_path = os.path.join(LOG_FILE_PREFIX, filename)

        # Check if it's a file is not a placeholder and not a directory
        if filename != PLACEHOLDER and os.path.isfile(file_path):
            try:
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

print("clean.py: Start.")
delete_log_files()
print("clean.py: Done.")