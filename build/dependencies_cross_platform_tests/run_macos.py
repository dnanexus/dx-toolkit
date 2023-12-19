import argparse
import logging
import os
import platform
import random
import shutil
import subprocess
import sys
import tempfile
import time

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from utils import EXIT_SUCCESS, init_base_argparser, init_logging, parse_common_args, extract_failed_tests, make_execution_summary, filter_pyenvs, Matcher

ROOT_DIR = Path(__file__).parent.absolute()

PYENVS = \
    ["system"] + \
    [f"official-{p}" for p in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")] + \
    [f"pyenv-{p}" for p in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")] + \
    [f"brew-{p}" for p in ("3.8", "3.9", "3.10", "3.11", "3.12")]

EXIT_TEST_EXECUTION_FAILED = 1


@dataclass
class PyEnv:
    name: str
    _env: str = field(init=False, default=None)
    _ver: str = field(init=False, default=None)

    def __post_init__(self):
        p = self.name.split("-")
        if len(p) > 1:
            self._env = p[0]
            self._ver = p[1]
        else:
            self._env = p[0]
            if p[0] == "system":
                if not Path("/usr/bin/python3").is_file():
                    raise Exception("Python 2.7 is no longer supported")
                self._ver = "3"

    @property
    def is_system(self):
        return self.name == "system"

    @property
    def is_official(self):
        return self._env == "official"

    @property
    def is_brew(self):
        return self._env == "brew"

    @property
    def is_pyenv(self):
        return self._env == "pyenv"

    @property
    def python_version(self):
        return self._ver


@dataclass
class DXPYTestsRunner:
    dx_toolkit: Path
    token: str
    env: str = "stg"
    pyenv_filters_inclusive: Optional[List[Matcher]] = None
    pyenv_filters_exclusive: Optional[List[Matcher]] = None
    extra_requirements: Optional[List[str]] = None
    pytest_args: Optional[str] = None
    report: Optional[str] = None
    logs_dir: str = Path("logs")
    workers: int = 1
    retries: int = 1
    print_logs: bool = False
    print_failed_logs: bool = False
    _macos_version: float = float('.'.join(platform.mac_ver()[0].split('.')[:2]))
    _brew_in_opt: bool = Path("/opt/homebrew").is_dir()
    _test_results: Dict[str, Dict] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.dx_toolkit = self.dx_toolkit.absolute()
        self.logs_dir = self.logs_dir.absolute()
        logging.debug(f"Detected MacOS version {self._macos_version}")

    def run(self):
        pyenvs = filter_pyenvs(PYENVS, self.pyenv_filters_inclusive, self.pyenv_filters_exclusive)

        logging.info("Python environments: " + ", ".join(pyenvs))

        for pyenv in pyenvs:
            p = PyEnv(pyenv)
            if p.is_pyenv:
                logging.info(f"[{pyenv}] Installing Python {p.python_version} using pyenv")
                with open(self.logs_dir / f"{pyenv}_install.log", 'w') as fh:
                    subprocess.run(["pyenv", "install", "--skip-existing", p.python_version], check=True, stdout=fh, stderr=subprocess.STDOUT, text=True)
            elif p.is_brew:
                logging.info(f"[{pyenv}] Installing Python {p.python_version} using brew")
                with open(self.logs_dir / f"{pyenv}_install.log", 'w') as fh:
                    subprocess.run(["brew", "install", "--overwrite", "--force", f"python@{p.python_version}"], check=True, stdout=fh, stderr=subprocess.STDOUT, text=True)

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for pyenv in pyenvs:
                executor.submit(self._run_pyenv, pyenv)
            executor.shutdown(wait=True)

        exit_code = make_execution_summary(self._test_results, self.report)
        return exit_code

    def _store_test_results(self, pyenv, code, failed_tests=None):
        self._test_results[pyenv] = {
            "code": code,
            "failed_tests": failed_tests
        }
        with open(self.logs_dir / f"{pyenv}.status", 'w') as fh:
            fh.write(f"{code}\n")

    def _run_pyenv(self, pyenv: str):
        try:
            for i in range(1, self.retries + 1):
                try:
                    self._do_run_pyenv(pyenv)
                    break
                except:
                    if i == self.retries:
                        raise
                    logging.exception(f"[{pyenv}] Tests execution failed (try {i})")
                    time.sleep(random.randrange(70, 90))
        except:
            logging.exception(f"[{pyenv} Failed running tests")
            self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED)

    def _do_run_pyenv(self, pyenv: str):
        p = PyEnv(pyenv)
        with tempfile.TemporaryDirectory() as wd:
            logging.info(f"[{pyenv}] Preparing for test execution (temporary dir: '{wd}')")
            wd = Path(wd)

            dx_python_root = wd / "python"
            shutil.copytree(self.dx_toolkit / "src" / "python", dx_python_root)

            env = os.environ.copy()

            if p.is_system:
                env["DXPY_TEST_BASE_PYTHON_BIN"] = f"/usr/bin/python3"
            elif p.is_official:
                env["DXPY_TEST_BASE_PYTHON_BIN"] = str(Path("/Library") / "Frameworks" / "Python.framework" / "Versions" / p.python_version / "bin" / f"python{p.python_version}")
            elif p.is_pyenv:
                subprocess.run(f"pyenv local {p.python_version}", cwd=wd, check=True, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
                env["DXPY_TEST_USING_PYENV"] = "true"
            elif p.is_brew:
                if self._brew_in_opt:
                    env["DXPY_TEST_BASE_PYTHON_BIN"] = str(Path("/opt") / "homebrew" / "bin" / f"python{p.python_version}")
                else:
                    env["DXPY_TEST_BASE_PYTHON_BIN"] = str(Path("/usr") / "local" / "opt" / f"python@{p.python_version}" / "bin" / f"python{p.python_version}")

            if self.extra_requirements and len(self.extra_requirements) > 0:
                extra_requirements_file = wd / "extra_requirements.txt"
                with open(extra_requirements_file, 'w') as fh:
                    fh.writelines(self.extra_requirements)
                env["DXPY_TEST_EXTRA_REQUIREMENTS"] = str(extra_requirements_file)

            env_dir = wd / "testenv"

            logging.info(f"[{pyenv}] Running tests")
            env["DXPY_TEST_TOKEN"] = self.token
            env["DXPY_TEST_PYTHON_VERSION"] = p.python_version[0]
            env["DX_USER_CONF_DIR"] = str((wd / ".dnanexus_config").absolute())
            tests_log: Path = self.logs_dir / f"{pyenv}_test.log"
            with open(tests_log, 'w') as fh:
                res = subprocess.run([ROOT_DIR / "macos" / "run_tests.sh", dx_python_root, env_dir] + (self.pytest_args or []), env=env, cwd=wd, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
            if res.returncode != 0:
                logging.error(f"[{pyenv}] Tests exited with non-zero code. See log for console output: {tests_log.absolute()}")
                if self.print_logs or self.print_failed_logs:
                    self._print_log(pyenv, tests_log)
                self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED, extract_failed_tests(tests_log))
                return

            logging.info(f"[{pyenv}] Tests execution successful")
            if self.print_logs:
                self._print_log(pyenv, tests_log)
            self._store_test_results(pyenv, EXIT_SUCCESS)

    def _print_log(self, pyenv, log):
        with open(log) as fh:
            logging.info(f"[{pyenv}] Tests execution log:\n{fh.read()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    init_base_argparser(parser)

    args = parser.parse_args()

    init_logging(args.verbose)

    ret = DXPYTestsRunner(
        **parse_common_args(args)
    ).run()
    sys.exit(ret)
