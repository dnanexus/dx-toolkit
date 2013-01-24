#******************************************************************************\
#* Copyright (c) 2003-2004, Martin Blais
#* All rights reserved.
#*
#* Redistribution and use in source and binary forms, with or without
#* modification, are permitted provided that the following conditions are
#* met:
#*
#* * Redistributions of source code must retain the above copyright
#*   notice, this list of conditions and the following disclaimer.
#*
#* * Redistributions in binary form must reproduce the above copyright
#*   notice, this list of conditions and the following disclaimer in the
#*   documentation and/or other materials provided with the distribution.
#*
#* * Neither the name of the Martin Blais, Furius, nor the names of its
#*   contributors may be used to endorse or promote products derived from
#*   this software without specific prior written permission.
#*
#* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
#* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
#* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
#* OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
#* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
#* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#******************************************************************************\

# Copyright 2012-2013 DNAnexus, Inc. All rights reserved.

import sys, os, types, argparse
import dxpy.utils.completer

def autocomplete(parser, arg_completer=None, subcommands=None):
    """Automatically detect if we are requested completing and if so generate
    completion automatically from given parser.

    'parser' is the options parser to use.

    'arg_completer' is a callable object that gets invoked to produce a list of
    completions for arguments completion (oftentimes files).

    If 'subcommands' is specified, the script expects it to be a map of
    command-name to an object of any kind.  We are assuming that this object is
    a map from command name to a pair of (options parser, completer) for the
    command. If the value is not such a tuple, the method
    'autocomplete(completer)' is invoked on the resulting object.

    This will attempt to match the first non-option argument into a subcommand
    name and if so will use the local parser in the corresponding map entry's
    value.  This is used to implement completion for subcommand syntax."""

    # If we are not requested for complete, simply return silently, let the code
    # caller complete. This is the normal path of execution.
    if not os.environ.has_key('ARGPARSE_AUTO_COMPLETE'):
        return

    ifs = os.environ.get('IFS')
    cwords = os.environ['COMP_WORDS'].split(ifs)
    cline = os.environ['COMP_LINE']
    cpoint = int(os.environ['COMP_POINT'])
    cword = int(os.environ['COMP_CWORD'])

    # If requested, try subcommand syntax to find an options parser for that
    # subcommand.
    if subcommands:
        assert isinstance(subcommands, types.DictType)
        if len(cwords) > 2 and cwords[1]+" "+cwords[2] in subcommands:
            parser = subcommands[cwords[1]+" "+cwords[2]][0]
            return autocomplete(parser, arg_completer=subcommands[cwords[1]+" "+cwords[2]][1])
        elif len(cwords) > 1 and cwords[1] in subcommands:
            parser = subcommands[cwords[1]][0]
            return autocomplete(parser, arg_completer=subcommands[cwords[1]][1])
        else:
            return autocomplete(parser)

    cword_pos = 0
    for i in range(cpoint, 0, -1):
        if cline[i:i+len(cwords[cword])] == cwords[cword]:
            cword_pos = i
            break

    prefix = cline[cword_pos:cpoint]
    suffix = cline[cpoint:cword_pos+len(cwords[cword])]

    completions = []

    # Subcommand and options completion
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            completions += [subcmd for subcmd in action.choices.keys() if subcmd.startswith(prefix)]
        elif prefix and prefix.startswith('-'):
            completions += [option for option in action.option_strings if option.startswith(prefix)]

    # Argument completion
    if arg_completer and (not prefix or not prefix.startswith('-')):
        if isinstance(arg_completer, dxpy.utils.completer.LocalCompleter):
            # Avoid using the built-in local completer, fall back to default bash completer
            print "__DX_STOP_COMPLETION__"
            sys.exit(1)
        completions += arg_completer.get_matches(cline, cpoint, prefix, suffix)

    # If there's only one completion, and it doesn't end with / or :, add a space
    if len(completions) == 1 and completions[0][-1] not in '/:':
        completions[0] += ' '

    # Print result
    print ifs.join(completions)

    # Exit with error code (we do not let the caller continue on purpose, this
    # is a run for completions only.)
    sys.exit(1)
