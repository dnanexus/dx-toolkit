import json
import logging
import re
import subprocess
import sys

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

EXIT_SUCCESS = 0


class Matcher:

    def match(self, pyenv: str) -> bool:
        pass


@dataclass
class ExactMatcher(Matcher):
    pattern: re.Pattern

    def match(self, pyenv: str) -> bool:
        return pyenv == self.pattern


@dataclass
class WildcardMatcher(Matcher):
    pattern: List[str]

    def __init__(self, pattern: str):
        self.pattern = pattern.split("-")
        for part in self.pattern:
            if "*" in part and part != "*":
                raise ValueError("Wild-cards can be used only for whole sections of pyenv names!")

    def match(self, pyenv: str) -> bool:
        pyenv_parts = pyenv.split("-")
        if len(self.pattern) != len(pyenv_parts):
            return False
        for i in range(len(self.pattern)):
            if self.pattern[i] != "*" and self.pattern[i] != pyenv_parts[i]:
                return False
        return True


@dataclass
class RegexpMatcher(Matcher):
    pattern: re.Pattern

    def __init__(self, pattern: str):
        self.pattern = re.compile(pattern)

    def match(self, pyenv: str) -> bool:
        return self.pattern.match(pyenv)


def extract_failed_tests(log: Path) -> List[str]:
    failed_tests = []
    in_block = False
    with open(log) as fh:
        for line in fh:
            line = line.strip()
            if line == "=========================== short test summary info ============================":
                in_block = True
            elif in_block and line.startswith("======= "):
                in_block = False
                break
            elif in_block and line.startswith("FAILED "):
                tmp = line[line.find("::") + 2:]
                failed_tests.append(tmp[:tmp.find(" - ")])
    return failed_tests if len(failed_tests) > 0 else None


def make_execution_summary(test_results: Dict[str, Dict], report_file: Path) -> int:
    logging.info("Test execution summary (%d/%d succeeded):", len([k for k, v in test_results.items() if v["code"] == EXIT_SUCCESS]), len(test_results))
    for pyenv in sorted(test_results.keys()):
        code = test_results[pyenv]["code"]
        msg = f"  {'[ SUCCESS ]' if code == EXIT_SUCCESS else '[  FAIL   ]'}        {pyenv} (exit code: {code}"
        if test_results[pyenv]["failed_tests"] is not None:
            msg += ", failed tests: " + ", ".join(test_results[pyenv]["failed_tests"])
        msg += ")"
        logging.info(msg)

    if report_file:
        with open(report_file, 'w') as fh:
            json.dump(test_results, fh)

    return 0 if all(map(lambda x: x["code"] == EXIT_SUCCESS, test_results.values())) else 1


def init_logging(verbose: bool) -> None:
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG if verbose else logging.INFO)


def init_base_argparser(parser) -> None:
    parser.add_argument("-d", "--dx-toolkit", required=True, help="Path to dx-toolkit source dir")
    parser.add_argument("-b", "--dx-toolkit-ref", help="dx-toolkit git reference (branch, commit, etc.) to test")
    parser.add_argument("--pull", action="store_true", help="Pull dx-toolkit repo before running the tests")
    parser.add_argument("-t", "--token", required=True, help="API token")
    parser.add_argument("-l", "--logs", default="./logs", help="Directory where to store logs")
    parser.add_argument("-e", "--env", choices=["stg", "prod"], default="stg", help="Platform")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Number of workers (i.e. parallelly running tests)")
    parser.add_argument("-a", "--retries", type=int, default=1, help="Number of retries for failed execution to eliminate network issues")
    parser.add_argument("-r", "--extra-requirement", dest="extra_requirements", action="append", help="Explicitly install this library to the virtual environment before installing dx-toolkit. Format is the same as requirements.txt file.")
    parser.add_argument("-o", "--report", help="Save status report to file in JSON format")
    pyenv_group = parser.add_mutually_exclusive_group()
    pyenv_group.add_argument("-f", "--pyenv-filter", dest="pyenv_filters", action="append", help="Run only in environments matching the filters. Supported are wild-card character '*' (e.g. ubuntu-*-py3-*) or regular expression (when using --regexp-filters flag). Exclusive filters can be using when prefixed with '!'.")
    pyenv_group.add_argument("--run-failed", metavar="REPORT", help="Load report file and run only failed environments")
    parser.add_argument("--print-logs", action="store_true", help="Print logs of all executions")
    parser.add_argument("--print-failed-logs", action="store_true", help="Print logs of failed executions")
    parser.add_argument("--regexp-filters", action="store_true", help="Apply filters as a fully-featured regular expressions")
    parser.add_argument("--pytest-matching", help="Run only tests matching given substring expression (the same as pytest -k EXPRESSION)")
    parser.add_argument("--pytest-exitfirst", action="store_true", help="Exit pytest instantly on first error or failed test (the same as pytest -x)")
    parser.add_argument("--pytest-tee", action="store_true", help="Also print stdout/stderr during execution (the same as pytest --capture=tee-sys)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")


def filter_pyenvs(all_pyenvs, filters_inclusive, filters_exclusive):
    pyenvs = all_pyenvs
    if filters_inclusive is not None and len(filters_inclusive) > 0:
        pyenvs = [p for p in pyenvs if any(map(lambda x: x.match(p), filters_inclusive))]
    if filters_exclusive is not None and len(filters_exclusive) > 0:
        pyenvs = [p for p in pyenvs if all(map(lambda x: not x.match(p), filters_exclusive))]

    pyenvs.sort()

    return pyenvs


def parse_common_args(args) -> dict:
    pytest_args = []
    if args.pytest_matching:
        pytest_args += ["-k", args.pytest_matching]
    if args.pytest_exitfirst:
        pytest_args.append("-x")
    if args.pytest_tee:
        pytest_args += ["--capture", "tee-sys"]

    pyenv_filters_inclusive = None
    pyenv_filters_exclusive = None
    MatcherClass = RegexpMatcher if args.regexp_filters else WildcardMatcher
    if args.run_failed:
        with open(args.run_failed) as fh:
            pyenv_filters_inclusive = [ExactMatcher(k) for k, v in json.load(fh).items() if v != 0]
    elif args.pyenv_filters:
        pyenv_filters_inclusive = [MatcherClass(f) for f in args.pyenv_filters if f[0] != "!"]
        pyenv_filters_exclusive = [MatcherClass(f[1:]) for f in args.pyenv_filters if f[0] == "!"]

    if args.pull:
        logging.debug("Pulling dx-toolkit git repository")
        try:
            subprocess.run(["git", "pull"], cwd=args.dx_toolkit, check=True, capture_output=True)
        except:
            logging.exception("Unable to pull dx-toolkit git repo")
            sys.exit(1)

    if args.dx_toolkit_ref:
        logging.debug(f"Checking out dx-toolkit git reference '{args.dx_toolkit_ref}'")
        try:
            subprocess.run(["git", "checkout", args.dx_toolkit_ref], cwd=args.dx_toolkit, check=True, capture_output=True)
        except:
            logging.exception("Unable to checkout dx-toolkit git reference")
            sys.exit(1)

    logs_dir = Path(args.logs)
    if not logs_dir.is_dir():
        logging.debug("Logs directory does not exist. Creating...")
        logs_dir.mkdir()

    return dict(
        dx_toolkit=Path(args.dx_toolkit),
        token=args.token,
        env=args.env,
        pyenv_filters_inclusive=pyenv_filters_inclusive,
        pyenv_filters_exclusive=pyenv_filters_exclusive,
        extra_requirements=args.extra_requirements,
        pytest_args=pytest_args,
        report=args.report,
        logs_dir=logs_dir,
        print_logs=args.print_logs,
        print_failed_logs=args.print_failed_logs,
        workers=args.workers,
        retries=args.retries,
    )
