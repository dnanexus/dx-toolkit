import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from utils import init_base_argparser, init_logging, parse_common_args, filter_pyenvs, Matcher

ROOT_DIR = Path(__file__).parent.absolute()

_PYTHON_VERSIONS = ["2.7", "3.7", "3.8", "3.9", "3.10", "3.11"]
PYENVS = [f"official-{p}" for p in _PYTHON_VERSIONS]

EXIT_SUCCESS = 0
EXIT_TEST_EXECUTION_FAILED = 1


@dataclass
class DXPYTestsRunner:
    dx_toolkit: Path
    token: str
    env: str = "stg"
    pyenv_filters_inclusive: Optional[List[Matcher]] = None
    pyenv_filters_exclusive: Optional[List[Matcher]] = None
    pytest_args: Optional[str] = None
    report: Optional[str] = None
    logs_dir: str = Path("logs")
    workers: int = 1
    print_logs: bool = False
    print_failed_logs: bool = False
    pytest_python: str = "python3.11"
    skip_interactive_tests: bool = False
    gha_force_python: Optional[str] = None
    _test_results: Dict[str, int] = field(default_factory=dict, init=False)

    def __post_init__(self):
        if self.workers > 1:
            raise ValueError("Windows currently does not support multiple workers")

    def run(self):
        if (self.pyenv_filters_inclusive is not None or self.pyenv_filters_exclusive is not None) and self.gha_force_python:
            raise AssertionError("Cannot use filters with enforced Python!")

        if self.gha_force_python:
            pyenvs = ["gha"]
        else:
            pyenvs = filter_pyenvs(PYENVS, self.pyenv_filters_inclusive, self.pyenv_filters_exclusive)

        logging.info("Python environments: " + ", ".join(pyenvs))

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
            logging.exception(f"[{pyenv}] Failed running tests")
            self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED)

    def _do_run_pyenv(self, pyenv: str):
        with tempfile.TemporaryDirectory() as wd:
            logging.info(f"[{pyenv}] Running tests (temporary dir: '{wd}')")
            wd = Path(wd)
            if self.gha_force_python:
                python_bin = self.gha_force_python
            else:
                python_bin = Path("C:\\") / f"Python{pyenv.split('-')[-1].replace('.', '')}" / "python.exe"
            python_version = subprocess.run([python_bin, "--version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True).stdout.split(" ")[1][0]
            logging.debug(f"[{pyenv}] Running on Python {python_version}")
            dx_python_root = wd / "python"
            shutil.copytree(self.dx_toolkit / "src" / "python", dx_python_root)
            env_dir = wd / "testenv"
            try:
                if python_version == "2":
                    subprocess.run([python_bin, "-m", "pip", "install", "virtualenv"], check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                    subprocess.run([python_bin, "-m", "virtualenv", env_dir], check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                else:
                    subprocess.run([python_bin, "-m", "venv", env_dir], check=True)
            except subprocess.CalledProcessError as e:
                logging.error(f"[{pyenv}] Unable to create virtual environment for test execution\n{e.output}")
                self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED)
                return

            pytest_python = self.pytest_python or sys.executable
            pytest_args =' '.join(self.pytest_args) if self.pytest_args else ""
            script = wd / "run_tests.ps1"
            with open(script, 'w') as fh:
                fh.write(f"""
$ErrorActionPreference = 'stop'
$Env:PSModulePath = $Env:PSModulePath + ";$env:UserProfile\\Documents\\PowerShell\\Modules"
{env_dir}\\Scripts\\activate.ps1

echo "Base Python version:"
python --version

echo "Pytest Python path: {pytest_python}"
echo "Pytest Python version:"
{pytest_python} --version

python -m pip install {dx_python_root}

If($LastExitCode -ne 0)
{{
    Exit 1
}}

{pytest_python} -m pytest -v {pytest_args} {(ROOT_DIR / 'dependencies_sanity_tests.py').absolute()}

If($LastExitCode -ne 0)
{{
    Exit 1
}}

Exit 0
""")

            tests_log: Path = self.logs_dir / f"{pyenv}_test.log"
            env = os.environ.copy()
            env["DXPY_TEST_TOKEN"] = self.token
            env["DXPY_TEST_PYTHON_BIN"] = str(env_dir / "Scripts" / "python")
            env["DXPY_TEST_PYTHON_VERSION"] = python_version
            env["DXPY_TEST_SKIP_INTERACTIVE"] = str(self.skip_interactive_tests)
            env["DX_USER_CONF_DIR"] = wd / ".dnanexus_config"
            if python_version == "2":
                env["PYTHONIOENCODING"] = "UTF-8"
            with open(tests_log, 'w') as fh:
                res = subprocess.run(["powershell", "-ExecutionPolicy", "Unrestricted", script], env=env, stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
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

    parser.add_argument("--pytest-python", help="Binary used for executing Pytest. By default it uses the same Python as for executing this script.")
    parser.add_argument("--skip-interactive-tests", action="store_true", help="Skip interactive tests")
    parser.add_argument("--gha-force-python", help="GitHub Actions: Run only artificial pyenv with specified Python binary")

    args = parser.parse_args()

    init_logging(args.verbose)

    ret = DXPYTestsRunner(
        **parse_common_args(args),
        pytest_python=args.pytest_python,
        skip_interactive_tests=args.skip_interactive_tests,
        gha_force_python=args.gha_force_python,
    ).run()
    sys.exit(ret)
