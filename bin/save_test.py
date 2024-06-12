#!/usr/bin/env python3
import argparse
import os
import shutil

TESTS_SRC_DIR = "docker/mitmproxy/requests/"
TESTS_DST_DIR = "quesma/tests/end2end/testcases/"


# Returns lowest "free" test suite nr in TESTS_DST_DIR directory
# So if there are suites (directories) 1, 2, 3, returns 4
# If there are suites 1, 3, 5, returns 2
def get_new_test_suite_nr() -> str:
    existing_testcase_nrs = [file.name for file in os.scandir(TESTS_DST_DIR) if file.is_dir() and file.name.isdigit()]
    possible_testcase_nrs = [str(i) for i in range(1, len(existing_testcase_nrs) + 2)]
    return str(min(set(possible_testcase_nrs) - set(existing_testcase_nrs)))


def save_test(test_nrs: list[str]):
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

    print(f"Tests {copied_test_nrs} saved in {suite_dir}/")


def get_requests_to_save(requests_available: list[int], args: dict[str, any]) -> list[str]:
    if all(value is None for value in args.values()):
        return [str(nr) for nr in requests_available]

    requests_to_save = set()
    if args["last"] is not None:
        requests_to_save.update(requests_available[-args["last"]:])
    if args["first"] is not None:
        requests_to_save.update(requests_available[:args["first"]])
    if args["slice"] is not None:
        slice_ = args["slice"].split(":")
        if len(slice_) != 2 or not slice_[0].isdigit() or not slice_[1].isdigit():
            print("Invalid slice argument, should be two integers separated by ':', got: ", args["slice"])
        else:
            requests_to_save.update([nr for nr in requests_available if int(slice_[0]) <= nr <= int(slice_[1])])
    if args["enumerate"] is not None:
        requests_to_save.update(args["enumerate"])

    return [str(nr) for nr in sorted(requests_to_save)]


def parse_arguments():
    ap = argparse.ArgumentParser(epilog="Without flags: all requests.   With some flags - subset of requests ("
                                        "union-like, so '-f 10 -l 10' will save (first 10 UNION last 10) requests")
    ap.add_argument("-l", "--last", help="Save last N requests", required=False, type=int)
    ap.add_argument("-f", "--first", help="Save first N requests", required=False, type=int)
    ap.add_argument("-s", "--slice", help="Save slice of requests, e.g. 10:20", required=False, type=str)
    ap.add_argument("-e", "--enumerate", nargs="+", help="List nrs of requests to save, e.g. '-e 1 3 10'", required=False, type=int)
    return vars(ap.parse_args())


def main():
    args = parse_arguments()
    requests_available = sorted([int(file.name[:-5])
                                 for file in os.scandir(TESTS_SRC_DIR)
                                 if file.is_file() and file.name.endswith(".http") and file.name[:-5].isdigit()])
    print("Available tests:", requests_available, "\n")
    save_test(get_requests_to_save(requests_available, args))


if __name__ == "__main__":
    main()
