#!/usr/bin/env python

import argparse
import os
import subprocess

parser = argparse.ArgumentParser(description="Runs Java integration tests.")
parser.add_argument(
    '--mvn-test',
    help=('Arg to be supplied to "mvn -Dtest=... test" to specify test classes or methods to be run. ' +
          'See https://maven.apache.org/surefire/maven-surefire-plugin/examples/single-test.html for ' +
          'available options'),
    metavar='MyTestClass',
)

TOOLKIT_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..'))

def run():
    if args.mvn_test:
        make_cmd = ["mvn", "-Dtest=" + args.mvn_test, "test"]
    else:
        make_cmd = ["mvn", "test"]
    try:
        # mvn will compile the Java files, but it doesn't know how to generate
        # the API wrappers and toolkit release file. So run the make step here
        # first.
        subprocess.check_call(["make", "java"], cwd=os.path.join(TOOLKIT_ROOT_DIR, "src"))
        subprocess.check_call(make_cmd, cwd=os.path.join(TOOLKIT_ROOT_DIR, "src", "java"))
    except subprocess.CalledProcessError as e:
        print "Tests failed, printing out error reports:"
        for filename in os.listdir(os.path.join(TOOLKIT_ROOT_DIR, "src/java/target/surefire-reports")):
            if filename.startswith("com.dnanexus."):
                print open(os.path.join(TOOLKIT_ROOT_DIR, "src/java/target/surefire-reports", filename)).read().strip()
        raise e

if __name__ == '__main__':
    args = parser.parse_args()
    run()
