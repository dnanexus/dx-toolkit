import contextlib
import os

import filelock
import pytest


@pytest.fixture(scope='session')
def serial_lock(tmp_path_factory: pytest.TempPathFactory):
    lock_file = tmp_path_factory.getbasetemp().parent / 'serial_tests.lock'
    yield filelock.FileLock(lock_file=str(lock_file))
    with contextlib.suppress(OSError):
        os.remove(path=lock_file)


@pytest.fixture()
def serial(serial_lock):
    with serial_lock.acquire(poll_interval=0.1):
        yield
