#!/usr/bin/python3

import dxpy
import subprocess, locale

class DXCalledProcessError(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, output=None, stderr=None):
        self.returncode = returncode
        self.cmd = cmd
        self.output = output
        self.stderr = stderr
    def __str__(self):
        return "Command '%s' returned non-zero exit status %d, stderr:\n%s" % (self.cmd, self.returncode, self.stderr)


def check_output(*popenargs, **kwargs):
    """
    Adapted version of the builtin subprocess.check_output which sets a
    "stderr" field on the resulting exception (in addition to "output")
    if the subprocess fails. (If the command succeeds, the contents of
    stderr are discarded.)

    :param also_return_stderr: if True, return stderr along with the output of the command as such (output, stderr)
    :type also_return_stderr: bool

    Unlike subprocess.check_output, unconditionally decodes the contents of the subprocess stdout and stderr using
    sys.stdin.encoding.
    """
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    if 'stderr' in kwargs:
        raise ValueError('stderr argument not allowed, it will be overridden.')

    return_stderr = False
    if 'also_return_stderr' in kwargs:
        if kwargs['also_return_stderr']:
            return_stderr = True
        del kwargs['also_return_stderr']

    # Unplug stdin (if not already overridden) so that dx doesn't prompt
    # user for input at the tty
    process = subprocess.Popen(stdin=kwargs.get('stdin', subprocess.PIPE),
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, *popenargs, **kwargs)
    output, err = process.communicate()
    retcode = process.poll()
    output = output.decode(locale.getpreferredencoding())
    err = err.decode(locale.getpreferredencoding())
    if retcode:
        print(err)
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        exc = DXCalledProcessError(retcode, cmd, output=output, stderr=err)
        raise exc

    if return_stderr:
        return (output, err)
    else:
        return output



def run(command, **kwargs):
    print("$ %s" % (command,))
    output = check_output(command, shell=True, **kwargs)
    print(output)
    return output

hello_repo_url = "https://github.com/nextflow-io/hello"
#applet_id = run("dx build --nextflow --repository '{}' --brief".format(hello_repo_url)).strip()
applet_id = "applet-GGVjFf809PPx6JF13Jj92gjj"
applet = dxpy.DXApplet(applet_id)
print(applet)
desc = applet.describe()

print(applet.get_details())


print(desc)

