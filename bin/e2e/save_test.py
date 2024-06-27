#!/usr/bin/env python3
import argparse
import os
import shutil

# Usage: usually just run this script without any arguments, it will save all requests
# If you want to save only some requests, use flags: -f, -l, -s, -e
# More info about flags: ./bin/save_test.py -h

TESTS_SRC_DIR = "../../docker/mitmproxy/requests/"
TESTS_DST_DIR = "../../quesma/tests/end2end/testcases/"


# Returns lowest "free" test suite nr in TESTS_DST_DIR directory
# So if there are suites (directories) 1, 2, 3, returns 4
# If there are suites 1, 3, 5, returns 2
def get_new_test_suite_nr() -> str:
    existing_testcase_nrs = [file.name for file in os.scandir(TESTS_DST_DIR) if file.is_dir() and file.name.isdigit()]
    possible_testcase_nrs = [str(i) for i in range(1, len(existing_testcase_nrs) + 2)]
    return str(min(set(possible_testcase_nrs) - set(existing_testcase_nrs)))


def save_test(test_nrs: list[str]):
    if not test_nrs:
        print("No requests to save, doing nothing.")
        return

    suite_nr = get_new_test_suite_nr()
    suite_dir = os.path.join(TESTS_DST_DIR, suite_nr)
    os.makedirs(suite_dir)

    copied_test_nrs = []
    cur_dst_nr = 1
    for cur_src_nr in test_nrs:
        src = os.path.join(TESTS_SRC_DIR, str(cur_src_nr) + ".http")
        dst = os.path.join(suite_dir, str(cur_dst_nr) + ".http")
        if not os.path.exists(src):
            continue
        shutil.copyfile(src, dst)
        cur_dst_nr += 1
        copied_test_nrs += [int(cur_src_nr)]

    print(f"Requests {copied_test_nrs} saved in {suite_dir}/")


if __name__ == "__main__":
    requests_available = sorted([file.name[:-5]
                                 for file in os.scandir(TESTS_SRC_DIR)
                                 if file.is_file() and file.name.endswith(".http") and file.name[:-5].isdigit()])
    print("Available test requests:", requests_available, "\n")
    save_test(requests_available)
