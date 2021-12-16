from typing import Optional, Sequence, Type, Union

from dxpy.sugar.processing.core import CalledProcessError, Mode, StdType, Processes
from dxpy.sugar.processing import utils


DEFAULT_EXECUTABLE = "/bin/bash"


def sub(
    cmds: Union[str, Sequence[Union[str, Sequence[str]]]], **kwargs
) -> Optional[str]:
    """
    Convenience method, equivalent to run(cmd, mode=str, block=True, **kwargs).

    Args:
        cmds: The command(s) to run
        kwargs: Additional kwargs passed to `run()`

    Returns:
        The process stdout (if its type is PIPE or BUFFER) else None.
    """
    if not kwargs.get("block", True):
        raise ValueError("Must call sub() with block=True")
    if not kwargs.get("mode", str) is str:
        raise ValueError("Must call sub() with mode=str")
    p = run(cmds, **kwargs)
    if p.stdout_type in {StdType.BUFFER, StdType.PIPE}:
        return p.output


def run(
    cmds: Union[str, Sequence[Union[str, Sequence[str]]]],
    shell: Optional[Union[str, bool]] = None,
    mode: Type[Mode] = str,
    block: bool = True,
    **kwargs
) -> Processes:
    """
    Runs several commands that pipe to each other in a python-aware way.

    Args:
        cmds: Any number of commands (lists or strings) to pipe together. This may be
            a string, in which case it will be split on the pipe ('|') character to
            get the component commands.
        shell: Can be a boolean specifying whether to execute the command
            using the shell, or a string value specifying the shell executable to use
            (which also implies shell=True). If None, the value is auto-detected: `False` if `cmds`
            is a string or an array of strings, otherwise `True`. If False, the command is executed
            via the default shell (which, according to the subprocess docs, is /bin/sh).
        mode: I/O mode; can be str (text) or bytes (raw).
        block: Whether to block until all processes have completed.
        kwargs: Additional keyword arguments to pass to :class:`Processes` constructor.

    Returns:
        A :class:`dxpy.processing.core.Processes` object.

    Raises:
        subprocess.CalledProcessError: if any subprocess in pipe returns exit
            code not 0.

    Examples:
        Usage 1: Pipe multiple commands together and print output to file
            example_cmd1 = ['dx', 'download', 'file-xxxx']
            example_cmd2 = ['gunzip']
            out_f = "somefilename.fasta"
            run([example_cmd1, example_cmd2], stdout=out_f)

            This function will print and execute the following command:
            'dx download file-xxxx | gunzip > somefilename.fasta'

        Usage 2: Pipe multiple commands together and return output
            example_cmd1 = ['gzip', 'file.txt']
            example_cmd2 = ['dx', 'upload', '-', '--brief']
            file_id = sub([example_cmd1, example_cmd2], block=True)

            This function will print and execute the following command:
            'gzip file.txt | dx upload - --brief '
            and return the output.

        Usage 3: Run a single command with output to file
            run('echo "hello world"', stdout='test2.txt')

        Usage 4: A command failing mid-pipe should return CalledProcessedError
            run(
                [['echo', 'hi:bye'], ['grep', 'blah'], ['cut', '-d', ':', '-f', '1']]
            )
            Traceback (most recent call last):
                  ...
            CalledProcessError: Command '['grep', 'blah']' returned non-zero
                exit status 1
    """
    if isinstance(cmds, str):
        cmds = [c.strip() for c in cmds.split("|")]
        if shell is None:
            shell = False
    else:
        cmds = list(cmds)
        if len(cmds) == 0:
            raise ValueError("'cmds' cannot be an empty list")
        if shell is None:
            shell = not isinstance(cmds[0], str)

    if shell:
        cmds = utils.command_lists_to_strings(cmds)
    else:
        cmds = utils.command_strings_to_lists(cmds)

    if shell is True:
        executable = DEFAULT_EXECUTABLE
    elif isinstance(shell, str):
        executable = shell
    else:
        executable = None

    processes = Processes(
        cmds, mode=mode, shell=(shell is not False), executable=executable, **kwargs
    )

    if block:
        with processes:
            processes.block()
    else:
        processes.run()

    return processes
