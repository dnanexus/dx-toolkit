#!/usr/bin/env python

import argparse
import os
import subprocess

parser = argparse.ArgumentParser(description="Runs Java integration tests.")

TOOLKIT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))

def run():
    try:
        subprocess.check_call("make test_java", cwd=TOOLKIT_ROOT_DIR, shell=True)
    except subprocess.CalledProcessError as e:
        print "Tests failed, printing out error reports:"
        for filename in os.listdir(os.path.join(TOOLKIT_ROOT_DIR, "src/java/target/surefire-reports")):
            if filename.startswith("com.dnanexus."):
                print open(os.path.join(TOOLKIT_ROOT_DIR, "src/java/target/surefire-reports", filename)).read().strip()
        raise e

if __name__ == '__main__':
    args = parser.parse_args()
    run()
