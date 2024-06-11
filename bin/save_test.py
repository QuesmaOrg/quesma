#!/usr/bin/env python3
import argparse
import os
import shutil

TESTS_SRC_DIR = "docker/mitmproxy/requests/"
TESTS_DST_DIR = "quesma/tests/end2end/testcases/"


# Returns lowest "free" test nr in TESTS_DST_DIR directory
# So if there are tests (directories) 1, 2, 3, returns 4
# If there are tests 1, 3, 5, returns 2
def get_test_nr() -> str:
    existing_testcase_nrs = [file.name for file in os.scandir(TESTS_DST_DIR) if file.is_dir() and str.isdigit(file.name)]
    possible_testcase_nrs = [str(i) for i in range(1, len(existing_testcase_nrs) + 2)]
    return str(min(set(possible_testcase_nrs) - set(existing_testcase_nrs)))


def save_test(args):
    test_nr = get_test_nr()
    test_dst_dir = os.path.join(TESTS_DST_DIR, test_nr)
    os.makedirs(test_dst_dir)

    start, i = 1, 1
    while True:
        src = os.path.join(TESTS_SRC_DIR, str(i) + ".http")
        dst = os.path.join(test_dst_dir, str(i) + ".http")
        if not os.path.exists(src):
            break
        shutil.copyfile(src, dst)
        i += 1

    print(f"Tests {start}-{i} saved in {test_dst_dir}")


def parse_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument("-l", "--last", help="Save only last N requests", required=False, type=int)
    ap.add_argument("-f", "--first", help="Save only first N requests", required=False, type=int)
    return vars(ap.parse_args())


def main():
    args = parse_arguments()

    save_test(args)


if __name__ == "__main__":
    main()
