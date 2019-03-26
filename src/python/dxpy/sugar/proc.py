import shlex
import subprocess

from dxpy.sugar import STR_TYPES, info


def run(
    cmd, return_output=False, output_file=None, shell=True, block=True, echo=True,
    **kwargs
):
    """
    Execute a command using a subprocess.

    Args:
        cmd (str or list): Command to be executed using subprocess. Input of
            type 'list' is recommended. When input is of type 'string',
            command is executed using /bin/bash and shell=True is specified.
        return_output (bool): If command output should be returned.
        output_file (str): Name of file to which command stdout should be redirected.
        shell: Whether to execute the command using the shell.
        block: Whether to block until the process is complete. If True, `return_output`
            is ignored.
        echo: Whether to echo the command to the log before executing it.
        kwargs: Additional arguments passed to check_output or check_call.

    Note:
        `return_output` and `output_file` paramters are mutually exclusive.
        If output_file exists, it will be overwritten.

    Returns:
        * If `block=False`, returns the Popen for the running process.
        * If return_output is provided, the output string is returned.
        * If output_file is provided, stdout is redirected to the specified file
        and the function returns None.

    Raises:
        CalledProcessError may be raised if the command failed to execute.

    Examples:
        run_cmd(['echo', 'hello world'], output_file='test1.txt')
    """
    executable = None
    if shell:
        executable = "/bin/bash"
        if not isinstance(cmd, STR_TYPES):
            cmd = _list2cmdline(cmd)
        if echo:
            print_cmd = cmd
            if output_file:
                print_cmd += " > {}".format(output_file)
            info(print_cmd)
    else:
        if isinstance(cmd, STR_TYPES):
            cmd = shlex.split(cmd)
        if echo:
            print_cmd = cmd
            if output_file:
                print_cmd += [">", output_file]
            info(_list2cmdline(print_cmd))

    if not block:
        return subprocess.Popen(cmd, shell=shell, executable=executable, **kwargs)
    elif return_output:
        output = subprocess.check_output(
            cmd, shell=shell, executable=executable, **kwargs
        ).strip()
        return output
    elif output_file:
        with open(output_file, "wb") as fopen:
            subprocess.check_call(
                cmd, shell=shell, executable=executable, stdout=fopen, **kwargs
            )
    else:
        subprocess.check_call(
            cmd, shell=shell, executable=executable, **kwargs
        )


def chain(cmds, return_output=False, output_file=None, block=True, echo=True):
    """
    Function to run several commands that pipe to each other in a python
    aware way.

    Args:
        cmds (iterable): Any number of commands (lists or strings) to pipe together.
        output_file (str): Filename to redirected stdout to at end of pipe. If True,
            `block` is set to True as well.
        return_output (bool): Whether to return stdout at end of pipe. If True,
            `block` is set to True as well.
        block: Whether to block until all processes have completed.
        echo: Whether to echo the command to the log before executing it.

    Note:
        `output_file` and `return_output` are mutually exclusive.
        If output_file exists, it will be overwritten.

    Returns:
        stdout (str) is returned if `return_output is True`. If
        `return_output is False`, `output_file is None`, and `block is False`,
        a list of processes is returned.

    Raises:
        subprocess.CalledProcessError: if any subprocess in pipe returns exit
            code not 0.

    Examples:
        Usage 1: Pipe multiple commands together and print output to file
            example_cmd1 = ['dx', 'download', 'file-xxxx']
            example_cmd2 = ['gunzip']
            out_f = "somefilename.fasta"
            chain([example_cmd1, example_cmd2], output_file=out_f)

            This function will print and execute the following command:
            'dx download file-xxxx | gunzip > somefilename.fasta'

        Usage 2: Pipe multiple commands together and return output
            example_cmd1 = ['gzip', 'file.txt']
            example_cmd2 = ['dx', 'upload', '-', '--brief']
            file_id = chain([example_cmd1, example_cmd2], return_output=True)

            This function will print and execute the following command:
            'gzip file.txt | dx upload - --brief '
            and return the output.

        Usage 3: Pipe a single command with output to file
            chain(['echo "hello world"'], output_file='test2.txt')
            Note: This calls the run function instead of chain.

        Usage 4: A command failing mid-pipe should return CalledProcessedError
            chain([['echo', 'hi:bye'], ['grep', 'blah'], ['cut', '-d', ':', '-f', '1']])
            Traceback (most recent call last):
                  ...
            CalledProcessError: Command '['grep', 'blah']' returned non-zero
                exit status 1
    """
    cmds = [
        shlex.split(cmd) if isinstance(cmd, STR_TYPES) else cmd
        for cmd in cmds
    ]

    if len(cmds) == 1:
        # if only one command is provided, use run_cmd instead
        return run(
            cmds[0], return_output=return_output, output_file=output_file, block=block,
            echo=echo
        )

    if echo:
        cmd_str = " | ".join(" ".join(cmd) for cmd in cmds)
        if output_file:
            cmd_str += " > {}".format(output_file)
        info(cmd_str)

    procs = []

    # Chain all but the last command
    for cmd in cmds[:-1]:
        popen_kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE
        }
        if procs:
            popen_kwargs["stdin"] = procs[-1].stdout
        proc = subprocess.Popen(cmd, **popen_kwargs)
        if procs:
            procs[-1].stdout.close()
        procs.append(proc)

    # Run the last command and redirect the output
    last_cmd = cmds[-1]
    penultimate_proc = procs[-1]
    output = None

    if return_output or output_file:
        block = True

    if not block:
        if output_file:
            with open(output_file, "w") as fopen:
                last_proc = subprocess.Popen(
                    last_cmd, stdin=penultimate_proc.stdout, stdout=fopen,
                    stderr=subprocess.PIPE
                )
        else:
            last_proc = subprocess.Popen(
                last_cmd, stdin=penultimate_proc.stdout, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        penultimate_proc.stdout.close()
        procs.append(last_proc)
        return procs

    if output_file:
        with open(output_file, "w") as fopen:
            subprocess.check_call(
                last_cmd, stdin=penultimate_proc.stdout, stdout=fopen,
                stderr=subprocess.PIPE
            )
    elif return_output is True:
        output = subprocess.check_output(
            last_cmd, stdin=penultimate_proc.stdout, stderr=subprocess.PIPE
        ).strip()
    else:
        subprocess.check_call(
            last_cmd, stdin=penultimate_proc.stdout, stderr=subprocess.PIPE
        )

    penultimate_proc.stdout.close()

    # check that all intermediate commands finished successfully
    for cmd, proc in zip(cmds, procs):
        # Polling is needed first in order to set the returncode attr
        proc.poll()
        returncode = proc.returncode
        if returncode != 0 and returncode is not None:
            raise subprocess.CalledProcessError(
                returncode, cmd, output=None
            )

    return output


def _list2cmdline(seq):
    """
    Translate a sequence of arguments into a command line
    string, using the same rules as the MS C runtime:
    1) Arguments are delimited by white space, which is either a
       space or a tab.
    2) A string surrounded by double quotation marks is
       interpreted as a single argument, regardless of white space
       contained within.  A quoted string can be embedded in an
       argument.
    3) A double quotation mark preceded by a backslash is
       interpreted as a literal double quotation mark.
    4) Backslashes are interpreted literally, unless they
       immediately precede a double quotation mark.
    5) If backslashes immediately precede a double quotation mark,
       every pair of backslashes is interpreted as a literal
       backslash.  If the number of backslashes is odd, the last
       backslash escapes the next double quotation mark as
       described in rule 3.

    Note:
        This function is copied from the subprocess module, as it is
        not part of the public API.
    """

    # See
    # http://msdn.microsoft.com/en-us/library/17w5ykft.aspx
    # or search http://msdn.microsoft.com for
    # "Parsing C++ Command-Line Arguments"
    result = []

    for arg in seq:
        bs_buf = []

        # Add a space to separate this argument from the others
        if result:
            result.append(" ")

        needquote = (" " in arg) or ("\t" in arg) or not arg
        if needquote:
            result.append('"')

        for c in arg:
            if c == "\\":
                # Don't know if we need to double yet.
                bs_buf.append(c)
            elif c == '"':
                # Double backslashes.
                result.append("\\" * len(bs_buf)*2)
                bs_buf = []
                result.append('\\"')
            else:
                # Normal char
                if bs_buf:
                    result.extend(bs_buf)
                    bs_buf = []
                result.append(c)

        # Add remaining backslashes, if any.
        if bs_buf:
            result.extend(bs_buf)

        if needquote:
            result.extend(bs_buf)
            result.append('"')

    return "".join(result)
