#!/usr/bin/env python3

"""
Execute the test suite on a loop in many processes to find race conditions.
"""

import argparse
import os

import mpcontroller as mpc


class Tester(mpc.Worker):
    def mainloop(self):
        exitcode = os.system("pytest -x --full-trace")
        if exitcode != 0:
            raise Exception("vroom")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        help="number of process to use simultaniously",
        default=mpc.cpu_count,
        type=int,
    )
    args = parser.parse_args()

    for _ in range(args.n):
        Tester.spawn()

    while True:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
