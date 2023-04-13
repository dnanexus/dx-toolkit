import argparse
import json
import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from utils import init_base_argparser, init_logging, parse_common_args, Matcher

ROOT_DIR = Path(__file__).parent.absolute()

PYENVS = \
    ["system"] + \
    [f"official-{p}" for p in ("2.7", "3.6", "3.7", "3.8", "3.9", "3.10", "3.11")] + \
    [f"pyenv-{p}" for p in ("2.7", "3.6", "3.7", "3.8", "3.9", "3.10", "3.11")] + \
    [f"brew-{p}" for p in ("3.8", "3.9", "3.10", "3.11")]

EXIT_SUCCESS = 0
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
                self._ver = "3" if Path("/usr/bin/python3").is_file() else "2"

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
    pyenv_filters: List[Matcher] = None
    pytest_args: Optional[str] = None
    report: Optional[str] = None
    logs_dir: str = Path("logs")
    workers: int = 1
    print_logs: bool = False
    print_failed_logs: bool = False
    _macos_version: float = float('.'.join(platform.mac_ver()[0].split('.')[:2]))
    _brew_in_opt: bool = Path("/opt/homebrew").is_dir()
    _test_results: Dict[str, bool] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self.dx_toolkit = self.dx_toolkit.absolute()
        self.logs_dir = self.logs_dir.absolute()
        logging.debug(f"Detected MacOS version {self._macos_version}")

    def run(self):
        has_filters = self.pyenv_filters is not None and len(self.pyenv_filters) > 0
        pyenvs = [p for p in PYENVS if any(map(lambda x: x.match(p), self.pyenv_filters))] if has_filters else PYENVS
        pyenvs.sort()

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

        logging.info("Test execution summary (%d/%d succeeded):", len([k for k, v in self._test_results.items() if v == EXIT_SUCCESS]), len(self._test_results))
        for pyenv in pyenvs:
            if pyenv in self._test_results:
                code = self._test_results[pyenv]
                logging.info(f"  {'[ SUCCESS ]' if code == EXIT_SUCCESS else '[  FAIL   ]'}        {pyenv} (exit code: {code})")

        if self.report:
            with open(self.report, 'w') as fh:
                json.dump(self._test_results, fh)

        return 0 if all(map(lambda x: x == EXIT_SUCCESS, self._test_results.values())) else 1

    def _store_test_results(self, pyenv, code):
        self._test_results[pyenv] = code
        with open(self.logs_dir / f"{pyenv}.status", 'w') as fh:
            fh.write(f"{code}\n")

    def _run_pyenv(self, pyenv: str):
        try:
            self._do_run_pyenv(pyenv)
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
                env["DXPY_TEST_BASE_PYTHON_BIN"] = f"/usr/bin/python{'3' if p.python_version == '3' else ''}"
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

            env_dir = wd / "testenv"

            logging.info(f"[{pyenv}] Running tests")
            env["DXPY_TEST_TOKEN"] = self.token
            env["DXPY_TEST_PYTHON_VERSION"] = p.python_version[0]
            tests_log: Path = self.logs_dir / f"{pyenv}_test.log"
            with open(tests_log, 'w') as fh:
                res = subprocess.run([ROOT_DIR / "macos" / "run_tests.sh", dx_python_root, env_dir] + (self.pytest_args or []), env=env, cwd=wd, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
            if res.returncode != 0:
                logging.error(f"[{pyenv}] Tests exited with non-zero code. See log for console output: {tests_log.absolute()}")
                if self.print_failed_logs:
                    self._print_log(pyenv, tests_log)
                self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED)
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
