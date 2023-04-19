#!/usr/bin/env python3

import argparse
import docker
import json
import logging
import re
import shutil
import sys
import tempfile

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from utils import EXIT_SUCCESS, init_base_argparser, init_logging, parse_common_args, extract_failed_tests, print_execution_summary, filter_pyenvs, Matcher

ROOT_DIR = Path(__file__).parent.absolute()
DOCKERFILES_DIR = ROOT_DIR / "linux" / "dockerfiles"
PYENVS = [re.sub("\\.Dockerfile$", "", f.name) for f in DOCKERFILES_DIR.iterdir() if f.name.endswith(".Dockerfile")]

EXIT_IMAGE_BUILD_FAILED = 1
EXIT_TEST_EXECUTION_FAILED = 2

client = docker.from_env()


class TestExecutionFailed(Exception):

    def __init__(self, msg, failed_tests):
        super().__init__(msg)
        self.failed_tests = failed_tests


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
    print_logs: bool = False
    print_failed_logs: bool = False
    keep_images: bool = False
    pull: bool = True
    _test_results: Dict[str, Dict] = field(default_factory=dict, init=False)

    def run(self):
        pyenvs = filter_pyenvs(PYENVS, self.pyenv_filters_inclusive, self.pyenv_filters_exclusive)

        logging.info("Python environments: " + ", ".join(pyenvs))

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            for pyenv in pyenvs:
                executor.submit(self._run_pyenv, pyenv)
            executor.shutdown(wait=True)

        print_execution_summary(self._test_results, self.report)

        return 0 if all(map(lambda x: x == EXIT_SUCCESS, self._test_results.values())) else 1

    def _store_test_results(self, pyenv, code, failed_tests=None):
        self._test_results[pyenv] = {
            "code": code,
            "failed_tests": failed_tests
        }
        with open(self.logs_dir / f"{pyenv}.status", 'w') as fh:
            fh.write(f"{code}\n")

    def _run_pyenv(self, pyenv):
        try:
            image = self._build_image(pyenv)
        except:
            logging.exception(f"[{pyenv}] Unable to build docker image")
            self._store_test_results(pyenv, EXIT_IMAGE_BUILD_FAILED)
            return

        try:
            self._run_tests(pyenv, image)
        except Exception as e:
            logging.exception(f"[{pyenv}] Tests execution failed.")
            self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED, e.failed_tests if isinstance(e, TestExecutionFailed) else None)
            return
        finally:
            if not self.keep_images:
                try:
                    client.images.remove(image.id)
                except:
                    logging.exception(f"[{pyenv}] Unable to remove docker image")

        self._store_test_results(pyenv, EXIT_SUCCESS)

    def _build_image(self, pyenv):
        logging.info(f"[{pyenv}] Building Docker image")
        dockerfile = f"{pyenv}.Dockerfile"
        try:
            image, log_stream = client.images.build(path=str(DOCKERFILES_DIR), tag=f"dxpy-testenv:{pyenv}", dockerfile=dockerfile, pull=self.pull, rm=True)
            with open(self.logs_dir / f"{pyenv}_build.log", 'w') as fh:
                for msg in log_stream:
                    if "stream" in msg:
                        fh.write(msg["stream"])
        except:
            logging.info(f"[{pyenv}] Docker build command failed and no logs were produced. For manual debugging, run 'docker build -f {DOCKERFILES_DIR}/{dockerfile} {DOCKERFILES_DIR}'")
            raise
        logging.info(f"[{pyenv}] Docker image successfully built")
        return image

    def _run_tests(self, pyenv, image):
        with tempfile.TemporaryDirectory() as wd:
            logging.info(f"[{pyenv}] Running tests (temporary dir: '{wd}')")
            wd = Path(wd)
            tests_log: Path = self.logs_dir / f"{pyenv}_test.log"
            volumes = {
                ROOT_DIR: {'bind': '/tests', 'mode': 'ro'},
                str(self.dx_toolkit): {'bind': '/dx-toolkit/', 'mode': 'ro'}
            }

            if self.extra_requirements and len(self.extra_requirements) > 0:
                extra_requirements_file = wd / "extra_requirements.txt"
                with open(extra_requirements_file, 'w') as fh:
                    fh.writelines(self.extra_requirements)
                volumes[extra_requirements_file] = {'bind': '/extra-requirements.txt', 'mode': 'ro'}

            command = " ".join(self.pytest_args) if self.pytest_args is not None and len(self.pytest_args) > 0 else None
            container = client.containers.run(
                image.id,
                command=command,
                volumes=volumes,
                environment={
                    "DXPY_TEST_TOKEN": self.token,
                    "DXPY_TEST_ENV": self.env,
                },
                detach=True
            )

            with open(tests_log, 'w') as fh:
                for msg in container.logs(stream=True, follow=True):
                    fh.write(msg.decode())
                    fh.flush()

            status = container.wait()["StatusCode"]

            try:
                container.remove()
            except:
                logging.exception(f"[{pyenv}] Cannot remove container")

            if status != 0:
                logging.error(f"[{pyenv}] Container exitted with non-zero return code. See log for console output: {tests_log.absolute()}")
                if self.print_logs or self.print_failed_logs:
                    self._print_log(pyenv, tests_log)
                raise TestExecutionFailed("Docker container exited with non-zero code", extract_failed_tests(tests_log))

            logging.info(f"[{pyenv}] Tests execution successful")
            if self.print_logs:
                self._print_log(pyenv, tests_log)

    def _print_log(self, pyenv, log):
        with open(log) as fh:
            logging.info(f"[{pyenv}] Tests execution log:\n{fh.read()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    init_base_argparser(parser)

    parser.add_argument("-k", "--keep-images", action="store_true", help="Do not delete docker images")
    parser.add_argument("--no-docker-pull", action="store_true", help="Do NOT pull base images from Docker hub on build")

    args = parser.parse_args()

    init_logging(args.verbose)

    ret = DXPYTestsRunner(
        **parse_common_args(args),
        keep_images=args.keep_images,
        pull=not args.no_docker_pull,
    ).run()
    sys.exit(ret)
