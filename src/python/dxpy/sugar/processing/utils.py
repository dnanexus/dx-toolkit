import shlex
from typing import Sequence, Union


def quote_args(seq: Sequence[str]) -> str:
    """
    Quote command line arguments.

    Args:
        seq: Command line arguments.

    Returns:
        Sequence of quoted command line arguments.
    """
    return " ".join(shlex.quote(str(arg)) for arg in seq)


def command_strings_to_lists(
    cmds: Sequence[Union[str, Sequence[str]]]
) -> Sequence[Sequence[str]]:
    """
    Convert any command strings in `cmds` to lists.

    Args:
        cmds: Commands - either strings or lists of arguments.

    Returns:
        A sequence of command argument sequences.
    """
    return [shlex.split(cmd) if isinstance(cmd, str) else cmd for cmd in cmds]


def command_lists_to_strings(
    cmds: Sequence[Union[str, Sequence[str]]]
) -> Sequence[str]:
    """
    Convert any command lists in `cmds` to strings.

    Args:
        cmds: Commands - either strings or lists of arguments.

    Returns:
        A sequence of command strings.
    """
    return [quote_args(cmd) if not isinstance(cmd, str) else cmd for cmd in cmds]
