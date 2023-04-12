import json
import logging
import re

from dataclasses import dataclass
from pathlib import Path
from typing import List


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


def init_logging(verbose: bool) -> None:
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG if verbose else logging.INFO)


def init_base_argparser(parser) -> None:
    parser.add_argument("-d", "--dx-toolkit", required=True, help="Path to dx-toolkit source dir")
    parser.add_argument("-t", "--token", required=True, help="API token")
    parser.add_argument("-l", "--logs", default="./logs", help="Directory where to store logs")
    parser.add_argument("-e", "--env", choices=["stg", "prod"], default="stg", help="Platform")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Number of workers (i.e. parallelly running tests)")
    parser.add_argument("-r", "--report", help="Save status report to file")
    pyenv_group = parser.add_mutually_exclusive_group()
    pyenv_group.add_argument("-f", "--pyenv-filter", dest="pyenv_filters", action="append", help="Run only in environments matching the filters. Supported are wild-card character '*' (e.g. ubuntu-*-py3-*) or regular expression (when using --regexp-filters flag)")
    pyenv_group.add_argument("--run-failed", metavar="REPORT", help="Load report file and run only failed environments")
    parser.add_argument("--print-failed-logs", action="store_true", help="Print logs of failed executions")
    parser.add_argument("--regexp-filters", action="store_true", help="Apply filters as a fully-featured regular expressions")
    parser.add_argument("--pytest-matching", help="Run only tests matching given substring expression (the same as pytest -k EXPRESSION)")
    parser.add_argument("--pytest-exitfirst", action="store_true", help="Exit pytest instantly on first error or failed test (the same as pytest -x)")
    parser.add_argument("--pytest-tee", action="store_true", help="Also print stdout/stderr during execution (the same as pytest --capture=tee-sys)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")


def parse_common_args(args) -> dict:
    pytest_args = []
    if args.pytest_matching:
        pytest_args += ["-k", args.pytest_matching]
    if args.pytest_exitfirst:
        pytest_args.append("-x")
    if args.pytest_tee:
        pytest_args += ["--capture", "tee-sys"]

    pyenv_filters = None
    MatcherClass = RegexpMatcher if args.regexp_filters else WildcardMatcher
    if args.run_failed:
        with open(args.run_failed) as fh:
            pyenv_filters = [ExactMatcher(k) for k, v in json.load(fh).items() if v != 0]
    elif args.pyenv_filters:
        pyenv_filters = [MatcherClass(f) for f in args.pyenv_filters]

    logs_dir = Path(args.logs)
    if not logs_dir.is_dir():
        logging.debug("Logs directory does not exist. Creating...")
        logs_dir.mkdir()

    return dict(
        dx_toolkit=Path(args.dx_toolkit),
        token=args.token,
        env=args.env,
        pytest_args=pytest_args,
        pyenv_filters=pyenv_filters,
        report=args.report,
        logs_dir=logs_dir,
        print_failed_logs=args.print_failed_logs,
        workers=args.workers,
    )
