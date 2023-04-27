import json
import os
import platform
import pytest
import random
import re
import shutil
import string
import subprocess
import sys
import time

from contextlib import contextmanager

FILESIZE = 100
TEST_DIR = os.path.abspath(os.path.join(__file__, os.pardir))
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"
GHA_WATCH_RETRIES = 5
GHA_KNOWN_WATCH_ERRORS = (
    "[Errno 110] Connection timed out", "[Errno 104] Connection reset by peer", "1006: Connection is already closed.", "[Errno 32] Broken pipe",
    "1006: EOF occurred in violation of protocol"
)


skip_on_windows = pytest.mark.skipif(IS_WINDOWS, reason="This test cannot run on Windows")
run_only_on_windows = pytest.mark.skipif(not IS_WINDOWS, reason="This test can run only on Windows")
skip_interactive_on_request = pytest.mark.skipif(os.environ.get("DXPY_TEST_SKIP_INTERACTIVE", "False").lower() == "true", reason="Requested skipping of interactive tests")


def _randstr(length=10):
    return "".join(random.choice(string.ascii_letters) for x in range(length))


def _upload_file(dir, name, content=None, platform_path=None, wait_until_closed=True):
    filep = os.path.join(dir, name)

    if content is None:
        if IS_WINDOWS:
            res = subprocess.run(["fsutil", "file", "createnew", filep, str(FILESIZE * 1024 * 1024)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            res = subprocess.run(["dd", "if=/dev/random", "of=%s" % filep, "bs=%d" % (1024 * 1024), "count=%s" % FILESIZE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert res.returncode == 0
    else:
        with open(filep, 'w') as fh:
            fh.write(content)

    cmd = ["dx", "upload", "--brief"]
    if platform_path is not None:
        cmd += ["--path", platform_path]
    cmd += [filep]

    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    file_id = res.stdout.strip()
    assert file_id.startswith("file-")

    if wait_until_closed:
        for _ in range(20):
            res = subprocess.run(["dx", "describe", "--json", file_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if json.loads(res.stdout)["state"] == "closed":
                return file_id, filep
            time.sleep(10)
        raise AssertionError("Files did not reach closed state within a time limit")

    return file_id, filep


def _diff_files(file1, file2):
    if IS_WINDOWS:
        return subprocess.run(["fc", "/B", file1, file2], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    else:
        return subprocess.run(["diff", file1, file2], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)


@contextmanager
def working_directory(path):
    orig_wd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(orig_wd)


@pytest.fixture
def tmp_path_str(tmp_path):
    """
    Fixture which converts tmp_path built-in from Path to str which is necessary for Python 3.5 compability.
    """
    return str(tmp_path)


@pytest.fixture(scope="session")
def dx_python_bin():
    return os.getenv("DXPY_TEST_PYTHON_BIN").strip()


@pytest.fixture(scope="module", autouse=True)
def project():
    token = os.getenv("DXPY_TEST_TOKEN")
    assert token is not None
    env = os.getenv("DXPY_TEST_ENV") or "stg"
    assert env in ["stg", "prod"]
    res = subprocess.run(["dx", "login", "--noprojects", "--token", token] + (["--staging"] if env == "stg" else []))
    assert res.returncode == 0
    res = subprocess.run(["dx", "new", "project", "--select", "--brief", "dxpy-deptest-%s" % _randstr()], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    project_id = res.stdout.strip()
    assert project_id.startswith("project-")

    yield project_id

    res = subprocess.run(["dx", "rmproject", "--yes", project_id])
    assert res.returncode == 0


@pytest.fixture
def applet(request):
    res = subprocess.run(["dx", "build", "--force", "--brief", os.path.join(TEST_DIR, "applets", "test-%s" % request.param)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    applet_id = json.loads(res.stdout.strip())["id"]
    assert applet_id.startswith("applet-")

    yield applet_id

    res = subprocess.run(["dx", "rm", applet_id])
    assert res.returncode == 0


@pytest.fixture
def two_files(tmp_path_str):
    file1 = _randstr()
    content1 = "My file 1 content..."
    fileid1, _ = _upload_file(tmp_path_str, file1, content1)

    file2 = _randstr()
    content2 = "My file 2 content..."
    fileid2, _ = _upload_file(tmp_path_str, file2, content2)

    yield ((file1, fileid1, content1), (file2, fileid2, content2))

    res = subprocess.run(["dx", "rm", fileid1])
    assert res.returncode == 0
    res = subprocess.run(["dx", "rm", fileid2])
    assert res.returncode == 0


def test_python_version(dx_python_bin):
    """
    Tested libraries: none
    """
    python_version = os.getenv("DXPY_TEST_PYTHON_VERSION")
    assert python_version in ["2", "3"]
    res = subprocess.run([dx_python_bin, "--version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    assert res.returncode == 0
    assert res.stdout.split(" ")[1][0] == python_version


def test_file_simple(tmp_path_str, project):
    """
    Tested libraries: requests, urllib3
    """
    file = "file"
    platform_path = "/test_file"
    assert platform_path[0] == "/"
    assert platform_path[1:] != file
    file_id, filep = _upload_file(tmp_path_str, "file", platform_path=platform_path)
    res = subprocess.run(["dx", "describe", "--json", file_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    file_describe = json.loads(res.stdout)
    assert file_describe["class"] == "file"
    assert file_describe["id"] == file_id
    assert file_describe["project"] == project
    res = subprocess.run(["dx", "download", platform_path], cwd=tmp_path_str)
    assert res.returncode == 0

    res = _diff_files(filep, os.path.join(tmp_path_str, platform_path[1:]))
    assert res.returncode == 0


def test_file_nonexistent():
    """
    Tested libraries: requests, urllib3
    """
    res = subprocess.run(["dx", "download", "file-%s" % _randstr(24)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode != 0
    assert "code 404" in res.stderr


def test_print_env():
    """
    Tested libraries: psutil
    """
    res = subprocess.run(["dx", "env"])
    assert res.returncode == 0


@skip_on_windows
def test_download_all_inputs(tmp_path_str, two_files):
    """
    Tested libraries: psutil
    """
    file1, fileid1, _ = two_files[0]
    file2, fileid2, _ = two_files[1]
    destdir = os.path.join(tmp_path_str, "dest")
    os.mkdir(destdir)
    with working_directory(destdir):
        job_input = {
            "inp1": {"$dnanexus_link": fileid1},
            "inp2": {"$dnanexus_link": fileid2},
        }

        with open("job_input.json", 'w') as fh:
            json.dump(job_input, fh)

        env = os.environ.copy()
        env["HOME"] = destdir
        shutil.copytree(os.getenv("DX_USER_CONF_DIR", os.path.join(os.path.expanduser('~'), ".dnanexus_config")), ".dnanexus_config")
        res = subprocess.run(["dx-download-all-inputs", "--parallel"], env=env)
        assert res.returncode == 0

    res = _diff_files(os.path.join(tmp_path_str, file1), os.path.join(destdir, "in", "inp1", file1))
    assert res.returncode == 0
    res = _diff_files(os.path.join(tmp_path_str, file2), os.path.join(destdir, "in", "inp2", file2))
    assert res.returncode == 0


@pytest.mark.parametrize("applet", ["simple"], indirect=["applet"])
def test_job_simple(project, applet):
    """
    Tested libraries: requests, urllib3
    """
    res = subprocess.run(["dx", "run", "--yes", "--brief", "--ignore-reuse", applet], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    job_id = res.stdout.strip()
    assert job_id.startswith("job-")
    res = subprocess.run(["dx", "describe", "--json", job_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    job_describe = json.loads(res.stdout)
    assert job_describe["id"] == job_id
    assert job_describe["project"] == project
    res = subprocess.run(["dx", "wait", job_id])
    assert res.returncode == 0
    res = subprocess.run(["dx", "watch", job_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    assert "Started" in res.stdout
    assert "Finished" in res.stdout


@pytest.mark.parametrize("applet", ["watch"], indirect=["applet"])
def test_job_watch(project, applet):
    """
    Tested libraries: requests, urllib3, websocket-client
    """
    res = subprocess.run(["dx", "run", "--yes", "--brief", "--ignore-reuse", applet], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    job_id = res.stdout.strip()
    assert job_id.startswith("job-")
    res = subprocess.run(["dx", "describe", "--json", job_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    job_describe = json.loads(res.stdout)
    assert job_describe["id"] == job_id
    assert job_describe["project"] == project

    for i in range(GHA_WATCH_RETRIES):
        res = subprocess.run(["dx", "watch", job_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        assert res.returncode == 0

        if any(map(lambda x: x in res.stderr, GHA_KNOWN_WATCH_ERRORS)):
            time.sleep(15)
            continue

        assert "Started" in res.stdout
        assert "Test to stderr" in res.stdout
        assert "Finished" in res.stdout
        return

    assert False, "Watch did not successfully finished even after %d retries" % GHA_WATCH_RETRIES


def test_import(dx_python_bin):
    """
    Tested libraries: none
    """
    res = subprocess.run([dx_python_bin, "-c", "import dxpy"])
    assert res.returncode == 0


def test_normalize_time_input(dx_python_bin, two_files):
    """
    Tested libraries: python-dateutil
    """
    _, fileid1, _ = two_files[0]
    _, fileid2, _ = two_files[1]

    res = subprocess.run(["dx", "find", "data", "--created-after=-1w"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    assert fileid1 in res.stdout
    assert fileid2 in res.stdout

    res = subprocess.run([dx_python_bin, "-c", "import sys; import dxpy.utils; sys.exit(0 if dxpy.utils.normalize_time_input('1d', default_unit='s') == 24 * 60 * 60 * 1000 else 1)"])
    assert res.returncode == 0


@skip_on_windows
@pytest.mark.parametrize("applet", ["inputs"], indirect=["applet"])
def test_run_interactive(applet, two_files):
    """
    Tested libraries: readline
    """
    import pexpect
    file, fileid, content = two_files[0]

    inp1_val = "string value"
    proc = pexpect.spawn("dx run %s" % applet)
    proc.expect("inp1:")
    proc.sendline(inp1_val)
    proc.expect("inp2:")
    proc.send("\t\t")
    proc.expect(file)
    proc.send(file[0:5] + "\t")
    proc.expect(file)
    proc.send("\n")
    proc.expect("Confirm running the executable with this input \\[Y/n\\]:")
    proc.sendline("Y")
    proc.expect("Watch launched job now\\? \\[Y/n\\]")
    job_id = re.search("(job-[a-zA-Z0-9]{24})", proc.before.decode()).group(1)
    proc.sendline("n")
    proc.expect(pexpect.EOF)

    assert job_id is not None

    res = subprocess.run(["dx", "wait", job_id])
    assert res.returncode == 0

    res = subprocess.run(["dx", "watch", job_id], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    assert res.returncode == 0
    assert inp1_val in res.stdout
    assert fileid in res.stdout
    assert content in res.stdout


@pytest.mark.skipif(IS_LINUX and sys.version_info < (3, 7), reason="Won't fix for old Python versions (see DEVEX-2258)")
@skip_on_windows
def test_dx_app_wizard_interactive(tmp_path_str):
    """
    Tested libraries: readline
    """
    import pexpect

    with working_directory(tmp_path_str):
        app_name = "test_applet"
        proc = pexpect.spawn("dx-app-wizard")
        proc.expect("App Name:")
        proc.sendline(app_name)
        proc.expect("Title .+:")
        proc.sendline()
        proc.expect("Summary .+:")
        proc.sendline()
        proc.expect("Version .+:")
        proc.sendline()
        proc.expect("1st input name .+:")
        proc.sendline("inp1")
        proc.expect("Label .+: ")
        proc.sendline()
        proc.expect("Choose a class .+:")
        proc.send("\t\t")
        proc.expect("boolean")
        proc.send("fl\t")
        proc.expect("float")
        proc.sendline()
        proc.expect("This is an optional parameter .+:")
        proc.sendline("n")
        proc.expect("2nd input name .+:")
        proc.sendline()
        proc.expect("1st output name .+:")
        proc.sendline("out1")
        proc.expect("Label .+:")
        proc.sendline()
        proc.expect("Choose a class .+:")
        proc.send("\t\t")
        proc.expect("record")
        proc.send("h\t")
        proc.expect("hash")
        proc.sendline()
        proc.expect("2nd output name .+:")
        proc.sendline()
        proc.expect("Timeout policy .+:")
        proc.sendline()
        proc.expect("Programming language:")
        proc.sendline("bash")
        proc.expect("Will this app need access to the Internet\\? .+:")
        proc.sendline()
        proc.expect("Will this app need access to the parent project\\? .+:")
        proc.sendline()
        proc.expect("Choose an instance type for your app .+:")
        proc.sendline()
        proc.expect(pexpect.EOF)

    assert os.path.isdir(os.path.join(tmp_path_str, app_name))
    assert os.path.isfile(os.path.join(tmp_path_str, app_name, "dxapp.json"))


@skip_on_windows
def test_argcomplete(two_files):
    """
    Tested libraries: argcomplete
    """
    import pexpect
    file, _, _ = two_files[0]

    proc = pexpect.spawn("/bin/bash")
    proc.sendline('eval "$(register-python-argcomplete dx|sed \'s/-o default//\')"')
    proc.send("dx \t\t")
    proc.expect("generate_batch_inputs")
    proc.send("new \t\t")
    proc.expect("record")
    proc.send("wor\t")
    proc.expect("workflow")
    proc.sendline('\003')
    proc.send("dx describe \t\t")
    proc.expect(file)
    proc.sendline('\003')
    proc.sendline("exit")
    proc.expect(pexpect.EOF)


@run_only_on_windows
@skip_interactive_on_request
@pytest.mark.parametrize("applet", ["inputs"], indirect=["applet"])
def test_dx_run_interactive_windows(tmp_path_str, applet, two_files):
    """
    Tested libraries: pyreadline, pyreadline3
    """
    file, fileid, content = two_files[0]
    with working_directory(tmp_path_str):
        env = os.environ.copy()
        env["APPLET"] = applet
        env["INP1_VAL"] = "test value"
        env["INP2_VAL"] = file
        res = subprocess.run(["powershell", "-ExecutionPolicy", "Unrestricted", os.path.join(TEST_DIR, "windows", "test_dx_run_interactive.ps1")], timeout=30, env=env)
        assert res.returncode == 0


@run_only_on_windows
@skip_interactive_on_request
def test_dx_app_wizard_interactive_windows(tmp_path_str):
    """
    Tested libraries: pyreadline, pyreadline3
    """
    with working_directory(tmp_path_str):
        app_name = "test_applet"
        res = subprocess.run(["powershell", "-ExecutionPolicy", "Unrestricted", os.path.join(TEST_DIR, "windows", "test_dx_app_wizard_interactive.ps1")], timeout=30)
        assert res.returncode == 0

    time.sleep(2)
    assert os.path.isdir(os.path.join(tmp_path_str, app_name))
    assert os.path.isfile(os.path.join(tmp_path_str, app_name, "dxapp.json"))


@run_only_on_windows
def test_colorama(dx_python_bin):
    """
    Tested libraries: colorama
    """
    res = subprocess.run([dx_python_bin, "-c", "import colorama; colorama.init()"])
    assert res.returncode == 0
