# Copyright (C) 2026 DNAnexus, Inc.
#
# This file is part of dx-toolkit (DNAnexus platform client libraries).
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may not
#   use this file except in compliance with the License. You may obtain a copy
#   of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

'''
Best-effort detection of the AI coding agent, if any, driving the current
process, reported as product tokens appended to the dxpy User-Agent::

    ai-agent/<label> ai-host/<label> ai-provider/<label> os/<label>

Every label comes from the fixed lists below. Signals are only checked for
presence or compared with a fixed value; no environment text is ever copied
into a token. The information is client-reported and non-authoritative.
'''

from __future__ import print_function, unicode_literals, division, absolute_import

import os
import platform

OPT_OUT_VAR = "DX_USER_AGENT_NO_AGENT_INFO"
MAX_SUFFIX_LENGTH = 128

FIELD_AGENT = "ai-agent"
FIELD_HOST = "ai-host"
FIELD_PROVIDER = "ai-provider"
FIELD_OS = "os"

OTHER = "other"


def _present(name):
    return lambda env: bool(env.get(name))


def _equals(name, value):
    return lambda env: env.get(name) == value


def _equals_ignore_case(name, value):
    return lambda env: (env.get(name) or "").lower() == value


def _contains_ignore_case(name, value):
    return lambda env: value in (env.get(name) or "").lower()


def _all(*checks):
    return lambda env: all(check(env) for check in checks)


def _any(*checks):
    return lambda env: any(check(env) for check in checks)


def _detected(agent):
    return lambda env: _detect_agent(env) == agent


# (label, signals); checked in order after AI_AGENT, and the first label with a
# matching signal wins. Amp also sets CLAUDECODE, so it must come before claude-code.
_AGENTS = (
    ("amp", (_equals("AGENT", "amp"), _present("AMP_CURRENT_THREAD_ID"))),
    ("claude-code",  (_present("CLAUDECODE"), _present("CLAUDE_CODE"), _present("CLAUDE_CODE_IS_COWORK"))),
    ("codex", (_present("CODEX_THREAD_ID"), _present("CODEX_CI"), _present("CODEX_SANDBOX"))),
    ("gemini", (_present("GEMINI_CLI"),)),
    ("antigravity", (_present("ANTIGRAVITY_AGENT"),)),
    ("cursor", (_present("CURSOR_AGENT"), _equals("CURSOR_EXTENSION_HOST_ROLE", "agent-exec"))),
    ("copilot", (_present("COPILOT_AGENT"), _present("COPILOT_CLI"), _present("COPILOT_MODEL"),
                 _present("COPILOT_ALLOW_ALL"))),
    ("opencode", (_present("OPENCODE"), _present("OPENCODE_CLIENT"))),
    ("augment", (_present("AUGMENT_AGENT"),)),
    ("qoder", (_present("QODER_AGENT"),)),
    ("windsurf", (_equals("WINDSURF_CASCADE_TERMINAL", "1"),)),
    ("grok", (_present("GROK_SESSION_ID"), _present("GROK_AGENT"))),
    ("replit", (_present("REPL_ID"),)),
    ("goose", (_present("GOOSE_PROVIDER"),)),
)

# Values accepted in AI_AGENT, after trimming and lowercasing; anything else is "other".
_AI_AGENT_VALUES = {
    "claude": "claude-code",
    "claude-code": "claude-code",
    "cowork": "claude-code",
    "github-copilot": "copilot",
    "github-copilot-cli": "copilot",
    "copilot": "copilot",
    "cursor": "cursor",
    "cursor-cli": "cursor",
    "augment": "augment",
    "augment-cli": "augment",
    "gemini": "gemini",
    "gemini-cli": "gemini",
}

_AI_AGENT_VALUES.update((label, label) for label in (
    "codex", "antigravity", "opencode", "amp", "grok", "replit", "goose", "v0",
    "kiro", "qoder", "muse-code", "windsurf", "aider",
))

# Checked in order; the first match wins. Cursor and Windsurf are VS Code forks whose
# terminals may also set TERM_PROGRAM=vscode, so they must come before vscode.
_HOSTS = (
    ("cursor", _any(_equals("TERM_PROGRAM", "cursor"),
                    _all(_equals("TERM_PROGRAM", "vscode"), _present("CURSOR_TRACE_ID")))),
    ("windsurf", _any(_equals_ignore_case("TERM_PROGRAM", "windsurf"),
                      _contains_ignore_case("VSCODE_GIT_ASKPASS_MAIN", "windsurf"))),
    ("vscode", _equals("TERM_PROGRAM", "vscode")),
    ("jetbrains", _equals("TERMINAL_EMULATOR", "JetBrains-JediTerm")),
    ("kiro", _equals("TERM_PROGRAM", "kiro")),
    ("eclipse", _any(_present("ECLIPSE_HOME"), _present("ECLIPSE_WORKSPACE"))),
)

# Checked in order; the first match wins.
_PROVIDERS = (
    ("bedrock", _equals("CLAUDE_CODE_USE_BEDROCK", "1")),
    ("vertex", _equals("CLAUDE_CODE_USE_VERTEX", "1")),
    ("copilot", _detected("copilot")),
)

_OPERATING_SYSTEMS = {
    "Darwin": "mac",
    "Windows": "windows",
    "Linux": "linux",
}


def _token(field, label):
    return field + "/" + label


def _detect_agent(env):
    ai_agent = (env.get("AI_AGENT") or "").strip().lower()
    if ai_agent:
        return _AI_AGENT_VALUES.get(ai_agent, OTHER)
    for label, signals in _AGENTS:
        if any(signal(env) for signal in signals):
            return label
    return None


def detect_agent_tokens(environ=None, system=None):
    '''
    :param environ: environment to inspect; defaults to ``os.environ``
    :param system: operating system name as returned by ``platform.system()``; defaults to the current one
    :returns: ordered list of ``field/label`` tokens, empty when no agent is detected

    Each field reports at most one label, the first match. Host, provider and OS
    tokens are only reported alongside an agent token.
    '''
    env = os.environ if environ is None else environ

    agent = _detect_agent(env)
    if agent is None:
        return []

    # Add agent token
    tokens = [_token(FIELD_AGENT, agent)]

    # Add host token
    for label, signal in _HOSTS:
        if signal(env):
            tokens.append(_token(FIELD_HOST, label))
            break

    # Add provider token
    for label, signal in _PROVIDERS:
        if signal(env):
            tokens.append(_token(FIELD_PROVIDER, label))
            break

    # Add OS token
    os_label = _OPERATING_SYSTEMS.get(platform.system() if system is None else system)
    if os_label:
        tokens.append(_token(FIELD_OS, os_label))

    return tokens


def agent_user_agent_suffix(environ=None, system=None):
    '''
    :returns: space-separated agent tokens to append to the User-Agent, or ``""``

    Returns ``""`` when ``DX_USER_AGENT_NO_AGENT_INFO=1`` or no agent is detected. The
    result is at most ``MAX_SUFFIX_LENGTH`` characters; tokens that do not fit are
    dropped from the end, never cut.
    '''
    env = os.environ if environ is None else environ
    if env.get(OPT_OUT_VAR) == "1":
        return ""

    suffix = ""
    for token in detect_agent_tokens(env, system=system):
        candidate = token if not suffix else suffix + " " + token
        if len(candidate) > MAX_SUFFIX_LENGTH:
            break
        suffix = candidate
    return suffix
