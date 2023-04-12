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
from typing import Dict, List

from utils import init_base_argparser, init_logging, parse_common_args, Matcher

ROOT_DIR = Path(__file__).parent.absolute()
DOCKERFILES_DIR = ROOT_DIR / "linux" / "dockerfiles"
PYENVS = [re.sub("\\.Dockerfile$", "", f.name) for f in DOCKERFILES_DIR.iterdir() if f.name.endswith(".Dockerfile")]

EXIT_SUCCESS = 0
EXIT_IMAGE_BUILD_FAILED = 1
EXIT_TEST_EXECUTION_FAILED = 2

client = docker.from_env()


@dataclass
class DXPYTestsRunner:
    dx_toolkit: Path
    token: str
    env: str = "stg"
    pyenv_filters: List[Matcher] = None
    pytest_args: str = None
    report: str = None
    logs_dir: str = Path("logs")
    workers: int = 1
    print_failed_logs: bool = False
    keep_images: bool = False
    pull: bool = True
    _test_results: Dict[str, int] = field(default_factory=dict, init=False)

    def run(self):
        has_filters = self.pyenv_filters is not None and len(self.pyenv_filters) > 0
        pyenvs = [p for p in PYENVS if any(map(lambda x: x.match(p), self.pyenv_filters))] if has_filters else PYENVS
        pyenvs.sort()

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

    def _run_pyenv(self, pyenv):
        try:
            image = self._build_image(pyenv)
        except:
            logging.exception(f"[{pyenv}] Unable to build docker image")

            self._store_test_results(pyenv, EXIT_IMAGE_BUILD_FAILED)
            return

        try:
            self._run_tests(pyenv, image)
        except:
            logging.exception(f"[{pyenv}] Tests execution failed.")
            self._store_test_results(pyenv, EXIT_TEST_EXECUTION_FAILED)
            return
        finally:
            if not self.keep_images:
                client.images.remove(image.id)

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
            dx_python_root = wd / "python"
            shutil.copytree(self.dx_toolkit / "src" / "python", dx_python_root)
            tests_log: Path = self.logs_dir / f"{pyenv}_test.log"
            command = " ".join(self.pytest_args) if self.pytest_args is not None and len(self.pytest_args) > 0 else None
            container = client.containers.run(
                image.id,
                command=command,
                volumes={
                    ROOT_DIR: {'bind': '/tests', 'mode': 'ro'},
                    str(dx_python_root): {'bind': '/dx-toolkit/src/python', 'mode': 'rw'}
                },
                environment={
                    "DXPY_TEST_TOKEN": self.token,
                    "DXPY_TEST_ENV": self.env,
                },
                remove=True,
                detach=True
            )

            with open(tests_log, 'w') as fh:
                for msg in container.logs(stream=True, follow=True):
                    fh.write(msg.decode())
                    fh.flush()

            status = container.wait()["StatusCode"]
            if status != 0:
                logging.error(f"[{pyenv}] Container exitted with non-zero return code. See log for console output: {tests_log.absolute()}")
                if self.print_failed_logs:
                    with open(tests_log) as fh:
                        logging.error(f"[{pyenv}] Text execution log:\n{fh.read()}")
                raise Exception("Docker container exited with non-zero code")

            logging.info(f"[{pyenv}] Tests execution successful")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    init_base_argparser(parser)

    parser.add_argument("-k", "--keep-images", action="store_true", help="Do not delete docker images")
    parser.add_argument("--no-pull", action="store_true", help="Do NOT pull base images from Docker hub on build")

    args = parser.parse_args()

    init_logging(args.verbose)

    ret = DXPYTestsRunner(
        **parse_common_args(args),
        keep_images=args.keep_images,
        pull=not args.no_pull,
    ).run()
    sys.exit(ret)
