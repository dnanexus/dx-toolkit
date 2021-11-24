import copy
import enum
import errno
import logging
import os
from pathlib import Path
import subprocess
from subprocess import CalledProcessError
import sys
import tempfile
from typing import Generic, IO, Optional, Sequence, Tuple, Type, TypeVar, Union, cast

from dxpy.sugar.processing.utils import command_lists_to_strings

LOG = logging.getLogger()
DEFAULT_EXECUTABLE = "/bin/bash"


class StdType(enum.IntEnum):
    """
    stdout/stderr types
    """

    PIPE = 0
    FILE = 1
    BUFFER = 2
    SYS = 3
    OTHER = 4


Mode = TypeVar("Mode", str, bytes)


class Processes(Generic[Mode]):
    """
    Encapsulates one or more commands, runs those commands using the
    `subprocess` module, and provides access to the results.

    Args:
        cmds: Command strings or sequences of command arguments to be chained together.
        stdin: Standard input to pass to the first process in the chain; can be
            a Path (filename), in which case the input is read from that file;
            `None`, in which case no stdin is used; `StdType.SYS`, in which case
            sys.stdin is used, or bytes, which will be passed to stdin.
        stdout: How to capture stdout of the final process in the chain; can be
            a Path (filename), in which case the output is written to that
            file; None, in which case stdout is discarded; or a `StdType`:
            `StdType.PIPE`, in which case `subprocess.PIPE` is used; `StdType.BUFFER`,
            in which case output is captured to a buffer; or `StdType.SYS`, in which
            case stdout is sent to `sys.stdout`. If a file, any existing file with the
            same name is overwritten.
        stderr: How to capture stderr of the final process in the chain (see
            `stdout` for details).
        capture_stderr: Whether to capture the contents of stderr of processes
            other than the final process.
        mode: Mode to use for stdin/stdout/stderr; can be bytes (raw mode) or str
            (text mode).
        encoding: Encoding to use when `mode = str`.
        echo: Whether to echo commands to the logger before running the commands.
            Can be overridden by the `run()` method's `echo` parameter.
        allowed_return_codes: Sequence of return codes that signal successful
            completion; [0] by default.
        raise_on_error: Whether to raise an exception if the process terminates
            with a non-allowed return code.
        timeout: When blocking, number of seconds to wait for the process to complete
            before raising a TimeoutError (can be overridden by `timeout` parameter
            to `block()`).
        popen_kwargs: Keyword arguments to pass to Popen constructors.
    """

    def __init__(
        self,
        cmds: Sequence[Union[str, Sequence[str]]],
        stdin: Optional[Union[Mode, Path, StdType]] = None,
        stdout: Optional[Union[Path, StdType]] = StdType.PIPE,
        stderr: Optional[Union[Path, StdType]] = StdType.PIPE,
        capture_stderr: bool = True,
        mode: Type[Mode] = str,
        encoding: str = "UTF-8",
        echo: bool = None,
        allowed_return_codes: Sequence[int] = (0,),
        raise_on_error: Optional[bool] = None,
        timeout: Optional[int] = None,
        **popen_kwargs,
    ):
        if "universal_newlines" in popen_kwargs:
            universal_newlines = popen_kwargs.pop("universal_newlines")
            if universal_newlines != (mode is str):
                raise ValueError(
                    f"'universal_newlines' is redundant; if specified, its value must "
                    f"match the 'mode' value of {mode}"
                )
        if "text" in popen_kwargs:
            text = popen_kwargs.pop("text")
            if text != (mode is str):
                raise ValueError(
                    f"'text' is redundant; if specified, its value must "
                    f"match the 'mode' value of {mode}"
                )

        self.cmds = cmds
        self._stdin_arg = stdin
        self._stdin = None
        self._stdin_type = None
        self._stdout_arg = stdout
        self._stdout = None
        self._stdout_type = None
        self._stderr_arg = stderr
        self._stderr = None
        self._stderr_type = None
        self.capture_stderr = capture_stderr
        self._stderr_buffers = [] if capture_stderr else None
        self._stderr_content = None
        self._mode = mode
        self.encoding = encoding
        self._closed = False
        self.echo = echo
        self.allowed_return_codes = set(allowed_return_codes)
        self.popen_kwargs = popen_kwargs
        self.raise_on_error = raise_on_error
        self.timeout = timeout
        self._processes = None
        self._output_handle = None
        self._returncode = None
        self._out = None
        self._err = None

    @property
    def returncode(self) -> int:
        """
        The return code of the last process to finish with an error, or 0. Is None
        if the last process hasn't finished running.
        """
        if not self.was_run:
            raise RuntimeError("Must call run() before returncode can be requested")
        if self._returncode is None:
            # Set `self._returncode` to the returncode of the last process
            # to finish with an error, to mimic behavior of `set -o pipefail`.
            # Note that if any process other than the last has a return code of
            # None, we ignore it.
            self._returncode = self._processes[-1].poll()
            if (
                self._returncode in self.allowed_return_codes
                and len(self._processes) > 1
            ):
                # The last process finished running without error, but check all
                # the other processes for an error.
                for proc in reversed(self._processes[-2::-1]):
                    rc = proc.poll()
                    if rc:
                        self._returncode = rc
                        break
        return self._returncode

    @property
    def text_mode(self) -> bool:
        return self._mode is str

    def _init_stdin(self) -> Union[int, IO]:
        self._stdin, self._stdin_type, retval = self._init_std(
            self._stdin_arg, sys.stdin, False
        )
        return retval

    def _init_stdout(self) -> Union[int, IO]:
        self._stdout, self._stdout_type, retval = self._init_std(
            self._stdout_arg, sys.stdout, True
        )
        return retval

    def _init_stderr(self) -> Union[int, IO]:
        self._stderr, self._stderr_type, retval = self._init_std(
            self._stderr_arg, sys.stderr, True
        )
        return retval

    def _init_std(
        self,
        value: Optional[Union[Path, StdType, bytes, str]],
        sys_stream: IO,
        is_output: bool = True,
    ) -> Tuple[Optional[IO], StdType, Union[int, IO]]:
        """

        """
        std_type = StdType.OTHER
        stream = None
        retval = None
        if isinstance(value, bytes):
            std_type = StdType.BUFFER
            # Put input in a tempfile
            stream = self._create_and_open_tempfile("w+")
            if self.text_mode:
                stream.write(cast(bytes, value).decode(self.encoding))
            else:
                stream.write(cast(bytes, value))
            stream.seek(0)
        elif isinstance(value, str):
            std_type = StdType.BUFFER
            # Put input in a tempfile
            stream = self._create_and_open_tempfile("w+")
            if self.text_mode:
                stream.write(cast(str, value))
            else:
                stream.write(cast(str, value).encode(self.encoding))
            stream.seek(0)
        elif isinstance(value, StdType):
            std_type = cast(StdType, value)
            if std_type is StdType.PIPE:
                retval = subprocess.PIPE
            elif std_type is StdType.BUFFER and is_output:
                stream = self._create_and_open_tempfile()
            elif std_type is StdType.SYS:
                stream = sys_stream
            else:
                raise ValueError(f"Invalid argument: {value}")
        elif value is not None:
            stream = self._open_file(cast(Path, value), "w" if is_output else "r")
            std_type = StdType.FILE

        return stream, std_type, retval or stream

    def _get_stderr_buffer(self) -> IO:
        """
        Creates a temporary file to use for storing a stderr stream.

        Returns:
            The opened file object.
        """
        if self.capture_stderr:
            handle = self._create_and_open_tempfile()
            self._stderr_buffers.append(handle)
            return handle

    def _open_file(self, path: Union[str, Path], mode: str) -> IO:
        mode += "t" if self.text_mode else "b"
        return open(path, mode)

    def _create_and_open_tempfile(self, mode: str = "w") -> IO:
        """
        Creates and returns a temporary output file object.
        """
        mode += "t" if self.text_mode else "b"
        return open(tempfile.mkstemp()[1], mode)

    @property
    def output(self) -> Mode:
        """
        The contents of the `stdout` stream of the last process in the chain.
        Only available if `self.closed is True` and `self._stdout_type in
        {StdType.PIPE, StdType.BUFFER}`.
        """
        if not (self.closed and self.stdout_type in {StdType.PIPE, StdType.BUFFER}):
            raise RuntimeError(
                "'output' is only available for closed processes using a pipe or "
                "buffer for stdout."
            )
        return self._out

    @property
    def error(self) -> Mode:
        """
        The contents of the `stderr` stream of the last process in the chain.
        Only available if `self.closed is True` and `self._stderr_type in
        {StdType.PIPE, StdType.BUFFER}`.
        """
        if not (self.closed and self.stderr_type in {StdType.PIPE, StdType.BUFFER}):
            raise RuntimeError(
                "'error' is only available for closed processes using a pipe or "
                "buffer for stderr."
            )
        return self._err

    @property
    def stdin_type(self) -> StdType:
        if self._stdin_type is None:
            raise RuntimeError("Cannot access 'stdin_type' until after calling 'run'.")
        return self._stdin_type

    @property
    def stdin_stream(self) -> IO:
        """
        The `stdin` stream.

        Note:
            Writing directly to the process stdin could cause a deadlock. Unless you
            know what you are doing, do not access this stream - instead, pass
            `stdin=<string or bytes>` to the initializer.
        """
        if not self.was_run:
            raise RuntimeError("Cannot access 'stdout' until after calling 'run'.")
        return self._stdin or self._processes[0].stdin

    @property
    def stdout_type(self) -> StdType:
        if self._stdout_type is None:
            raise RuntimeError("Cannot access 'stdout_type' until after calling 'run'.")
        return self._stdout_type

    @property
    def stdout_stream(self) -> IO:
        """
        The `stdout` stream of the last process in the chain.
        """
        if not self.was_run:
            raise RuntimeError("Cannot access 'stdout' until after calling 'run'.")
        return self._stdout or self._processes[-1].stdout

    @property
    def stderr_type(self) -> StdType:
        if self._stdout_type is None:
            raise RuntimeError("Cannot access 'stderr_type' until after calling 'run'.")
        return self._stderr_type

    @property
    def stderr_stream(self) -> IO:
        """
        The `stderr` stream of the last process in the chain.
        """
        if not self.was_run:
            raise RuntimeError("Cannot access 'stderr' until after calling 'run'.")
        return self._stderr or self._processes[-1].stderr

    def get_all_stderr(self) -> Sequence[Mode]:
        """
        Get the contents of all stderr streams. Stderr streams of all but the final
        process are only available if `self.capture_stderr is True`. Stderr stream
        of the final process is only available if `self._stderr_type in
        (StdType.BUFFER, StdType.PIPE)`.

        Returns:
            A list of strings, where each string is the captured stderr stream of a
            process.
        """
        if not self.closed:
            raise RuntimeError(
                "Cannot access 'stderr' contents until all processes have completed "
                "and file handles have been closed."
            )
        stderr = []
        if self.capture_stderr:
            stderr.extend(self._stderr_content)
        if self._stderr_type in (StdType.BUFFER, StdType.PIPE):
            stderr.append(self.error)
        return stderr

    @property
    def was_run(self) -> bool:
        """
        Whether the `run()` method was called.
        """
        return bool(self._processes)

    @property
    def done(self) -> bool:
        """
        Whether the commands have finished running.
        """
        return self.returncode is not None

    @property
    def ok(self) -> bool:
        """
        Whether the commands completed successfully, i.e. all had an allowed
        returncode.
        """
        return self.returncode in self.allowed_return_codes

    @property
    def closed(self) -> bool:
        """
        Whether `self.close()` has been called.
        """
        return self._closed

    def run(self, echo: bool = None, **kwargs):
        """
        Run the commands.

        Args:
            echo: Whether to echo the commands to the log. Overrides the value of
                `self.echo`.
            kwargs: Keyword arguments to pass to Popen. Overrides any values specified
                in `self.popen_kwargs`.
        """
        if self.was_run:
            raise RuntimeError("Cannot call run() more than once.")

        if echo is None:
            echo = self.echo
        if echo is not False:
            LOG.info(str(self))

        num_commands = len(self.cmds)
        procs = []

        for i, cmd in enumerate(self.cmds, 1):
            popen_kwargs = copy.copy(self.popen_kwargs)
            popen_kwargs.update(kwargs)
            if self.text_mode:
                popen_kwargs["universal_newlines"] = True
                popen_kwargs["encoding"] = self.encoding
            if i == 1:
                popen_kwargs["stdin"] = self._init_stdin()
            elif procs:
                popen_kwargs["stdin"] = procs[-1].stdout
            if i == num_commands:
                popen_kwargs["stdout"] = self._init_stdout()
                popen_kwargs["stderr"] = self._init_stderr()
            else:
                popen_kwargs["stdout"] = subprocess.PIPE
                popen_kwargs["stderr"] = self._get_stderr_buffer()
            proc = subprocess.Popen(cmd, **popen_kwargs)
            if procs:
                procs[-1].stdout.close()
            procs.append(proc)

        self._processes = procs

    def block(
        self,
        close: bool = True,
        raise_on_error: Optional[bool] = None,
        timeout: Optional[int] = None,
    ):
        """
        Wait for all commands to finish.

        Args:
            close: Whether to call `self.close()` after the processes complete.
            raise_on_error: Whether to raise a :class:`subprocess.CalledProcessError`
                if the returncode was not 0.
            timeout: Seconds to wait before killing the process; if None, process
                will run until completed.
        """
        if not self.was_run:
            raise RuntimeError("Cannot call block() until after calling run()")
        if self.closed:
            raise RuntimeError("Cannot call block() after calling close()")

        default_value = "" if self.text_mode else b""
        last_proc = self._processes[-1]
        if timeout is None:
            timeout = self.timeout

        try:  # TODO: figure out how to test this
            out, err = (
                default_value if std is None else std.strip()
                for std in last_proc.communicate(timeout=timeout)
            )
            if self._stdout_type == StdType.PIPE:
                self._out = out
            if self._stderr_type == StdType.PIPE:
                self._err = err
        except ValueError:  # TODO: figure out how to test this
            LOG.exception("Error reading from stdout/stderr")

        if close and not self.closed:
            self._close_and_set_std()

        if raise_on_error is None:
            raise_on_error = self.raise_on_error
        if raise_on_error is not False:
            self.raise_if_error()

    def kill(self) -> bool:
        """
        Kill the running commands. It is recommended to call `close()` if this
        returns `True`.

        Returns:
            True if processes were killed, else False.
        """
        if self.was_run and not self.done:
            try:
                for i, proc in enumerate(self._processes):
                    try:
                        proc.kill()
                    except OSError as oserr:  # TODO: figure out how to test this
                        if oserr.errno == errno.ESRCH:
                            # Ignore - process has already died
                            pass
                        LOG.exception(
                            "Error killing running process command %s", self.cmds[i]
                        )
            finally:
                if not self.closed:
                    try:
                        self.block()
                    except CalledProcessError:
                        # We expect this to happen when a process is killed
                        pass
                    except RuntimeError:  # TODO: figure out how to test this
                        LOG.exception(
                            "Error waiting for killed process(es) to end; any opened "
                            "files were not closed"
                        )

            return True

        return False

    def close(self):
        """
        For each of stdout, stderr of the final process, if it is a file close it,
        or if it is a buffer, set its value to `self.out/self.err`.
        """
        if not (self.was_run and self.done):
            raise RuntimeError("Can only call close() after 'done' is True.")

        if not self._closed:
            self._close_and_set_std()

    def _close_and_set_std(self):
        """
        Close any open files and set the values of `self._out` and `self._err`
        if they are of type StdType.BUFFER.
        """

        def close_file(handle):
            try:  # TODO: figure out how to test
                handle.close()
            except IOError:
                LOG.exception("Error closing output file %s", handle.name)

        def remove_file(handle):
            try:
                os.unlink(handle.name)
            except IOError:  # TODO: figure out how to test
                LOG.exception("Error removing file %s", handle.name)

        if self._stdin_type in {StdType.FILE, StdType.BUFFER}:
            try:
                close_file(self._stdin)
            finally:
                if self._stdin_type is StdType.BUFFER:
                    remove_file(self._stdin)

        def close_output_buffer(handle) -> Mode:
            close_file(handle)
            try:
                with self._open_file(handle.name, "r") as inp:
                    return inp.read()
            finally:
                remove_file(handle)

        if self._stdout_type is StdType.FILE:
            close_file(self._stdout)
        elif self._stdout_type is StdType.BUFFER:
            self._out = close_output_buffer(self._stdout)

        if self._stderr_type is StdType.FILE:
            close_file(self._stderr)
        elif self._stderr_type is StdType.BUFFER:
            self._err = close_output_buffer(self._stderr)

        if self.capture_stderr:
            self._stderr_content = [
                close_output_buffer(buf) for buf in self._stderr_buffers
            ]
            self._stderr_buffers = None

        self._closed = True

    def raise_if_error(self):
        """
        Raise an exception if the processes are finished running and the
        return code is not 0.

        Raises:
            CalledProcessError
        """
        if self.done and not self.ok:
            sep = "\n" if self.text_mode else b"\n"
            msg = "stderr from executed commands:\n{}".format(
                sep.join(self.get_all_stderr())
            )
            raise CalledProcessError(self.returncode, str(self), output=msg)

    def __str__(self) -> str:
        cmd_str = " | ".join(command_lists_to_strings(self.cmds))
        if self._stdout_type == StdType.FILE:
            cmd_str += " > {}".format(self._stdout.name)
        return cmd_str

    def __enter__(self) -> "Processes":
        if not self.was_run:
            self.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type or (self.was_run and not self.done):
            # Kill the processes if exiting the context manager with an error
            # or before the processes finish running.
            self.kill()
        self.close()
