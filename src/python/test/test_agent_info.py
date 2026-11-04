#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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

from __future__ import print_function, unicode_literals, division, absolute_import

import re
import unittest
from unittest import mock

from dxpy.utils import agent_info
from dxpy.utils.agent_info import agent_user_agent_suffix, detect_agent_tokens

LABEL_TOKEN = re.compile(r"^(ai-agent|ai-host|ai-provider|os)/[a-z0-9.-]+$")


def tokens(env, system="Linux"):
    return detect_agent_tokens(env, system=system)


def suffix(env, system="Linux"):
    return agent_user_agent_suffix(env, system=system)


class TestAgentDetection(unittest.TestCase):
    def test_no_agent(self):
        self.assertEqual(tokens({}), [])
        self.assertEqual(suffix({}), "")

    def test_each_env_signal(self):
        cases = [
            ({"CLAUDECODE": "1"}, "claude-code"),
            ({"CLAUDE_CODE": "1"}, "claude-code"),
            ({"CLAUDE_CODE_IS_COWORK": "1"}, "claude-code"),
            ({"CODEX_THREAD_ID": "abc"}, "codex"),
            ({"CODEX_CI": "1"}, "codex"),
            ({"CODEX_SANDBOX": "seatbelt"}, "codex"),
            ({"GEMINI_CLI": "1"}, "gemini"),
            ({"ANTIGRAVITY_AGENT": "1"}, "antigravity"),
            ({"CURSOR_AGENT": "1"}, "cursor"),
            ({"CURSOR_EXTENSION_HOST_ROLE": "agent-exec"}, "cursor"),
            ({"COPILOT_AGENT": "1"}, "copilot"),
            ({"COPILOT_CLI": "1"}, "copilot"),
            ({"COPILOT_MODEL": "gpt"}, "copilot"),
            ({"COPILOT_ALLOW_ALL": "1"}, "copilot"),
            ({"OPENCODE": "1"}, "opencode"),
            ({"OPENCODE_CLIENT": "cli"}, "opencode"),
            ({"AUGMENT_AGENT": "1"}, "augment"),
            ({"QODER_AGENT": "1"}, "qoder"),
            ({"WINDSURF_CASCADE_TERMINAL": "1"}, "windsurf"),
            ({"AGENT": "amp"}, "amp"),
            ({"AMP_CURRENT_THREAD_ID": "T-1"}, "amp"),
            ({"GROK_SESSION_ID": "s"}, "grok"),
            ({"GROK_AGENT": "1"}, "grok"),
            ({"REPL_ID": "r"}, "replit"),
            ({"GOOSE_PROVIDER": "openai"}, "goose"),
        ]
        for env, label in cases:
            with self.subTest(env=env):
                self.assertEqual(tokens(env)[0], "ai-agent/" + label)

    def test_empty_and_mismatched_values_are_ignored(self):
        self.assertEqual(tokens({"CLAUDECODE": ""}), [])
        self.assertEqual(tokens({"AGENT": "someone"}), [])
        self.assertEqual(tokens({"CURSOR_EXTENSION_HOST_ROLE": "user"}), [])
        self.assertEqual(tokens({"WINDSURF_CASCADE_TERMINAL": "0"}), [])

    def test_signals_that_are_not_agents(self):
        self.assertEqual(tokens({"CURSOR_TRACE_ID": "t", "TERM_PROGRAM": "vscode"}), [])
        self.assertEqual(tokens({"COPILOT_GITHUB_TOKEN": "secret"}), [])

    def test_only_first_agent_is_reported(self):
        self.assertEqual(tokens({"CLAUDECODE": "1", "CODEX_CI": "1", "GEMINI_CLI": "1"}),
                         ["ai-agent/claude-code", "os/linux"])

    def test_amp_wins_over_claude_code(self):
        self.assertEqual(tokens({"AGENT": "amp", "CLAUDECODE": "1"}), ["ai-agent/amp", "os/linux"])

    def test_ai_agent_known_values(self):
        cases = [
            ("v0", "v0"),
            ("devin", "other"),
            ("claude", "claude-code"),
            ("cowork", "claude-code"),
            ("github-copilot-cli", "copilot"),
            ("cursor-cli", "cursor"),
            ("  Kiro  ", "kiro"),
        ]
        for value, label in cases:
            with self.subTest(value=value):
                self.assertEqual(tokens({"AI_AGENT": value})[0], "ai-agent/" + label)

    def test_ai_agent_unknown_value_is_never_copied(self):
        result = suffix({"AI_AGENT": "Evil/1.0 (x) injected"})
        self.assertEqual(result, "ai-agent/other os/linux")
        self.assertNotIn("evil", result.lower())
        self.assertNotIn("injected", result)

    def test_ai_agent_wins_over_inferred_agent(self):
        self.assertEqual(tokens({"AI_AGENT": "codex", "CLAUDECODE": "1"}), ["ai-agent/codex", "os/linux"])
        self.assertEqual(tokens({"AI_AGENT": "unknown", "CLAUDECODE": "1"}), ["ai-agent/other", "os/linux"])

    def test_empty_ai_agent_is_ignored(self):
        self.assertEqual(tokens({"AI_AGENT": "  "}), [])

    def test_hosts(self):
        cases = [
            ({"TERM_PROGRAM": "vscode", "CURSOR_TRACE_ID": "t"}, "cursor"),
            ({"TERM_PROGRAM": "cursor"}, "cursor"),
            ({"TERM_PROGRAM": "Windsurf"}, "windsurf"),
            ({"TERM_PROGRAM": "vscode"}, "vscode"),
            ({"TERMINAL_EMULATOR": "JetBrains-JediTerm"}, "jetbrains"),
            ({"TERM_PROGRAM": "kiro"}, "kiro"),
        ]
        for env, label in cases:
            with self.subTest(env=env):
                env = dict(env, CLAUDECODE="1")
                self.assertEqual(tokens(env), ["ai-agent/claude-code", "ai-host/" + label, "os/linux"])

    def test_only_first_host_is_reported(self):
        env = {"CLAUDECODE": "1", "TERM_PROGRAM": "vscode", "TERMINAL_EMULATOR": "JetBrains-JediTerm"}
        self.assertEqual([t for t in tokens(env) if t.startswith("ai-host/")], ["ai-host/vscode"])

    def test_providers(self):
        self.assertIn("ai-provider/bedrock", tokens({"CLAUDECODE": "1", "CLAUDE_CODE_USE_BEDROCK": "1"}))
        self.assertIn("ai-provider/vertex", tokens({"CLAUDECODE": "1", "CLAUDE_CODE_USE_VERTEX": "1"}))
        self.assertEqual(tokens({"COPILOT_AGENT": "1"}), ["ai-agent/copilot", "ai-provider/copilot", "os/linux"])
        self.assertNotIn("ai-provider/bedrock", tokens({"CLAUDECODE": "1", "CLAUDE_CODE_USE_BEDROCK": "0"}))

    def test_only_first_provider_is_reported(self):
        env = {"CLAUDECODE": "1", "COPILOT_AGENT": "1", "CLAUDE_CODE_USE_BEDROCK": "1", "CLAUDE_CODE_USE_VERTEX": "1"}
        self.assertEqual([t for t in tokens(env) if t.startswith("ai-provider/")], ["ai-provider/bedrock"])

    def test_operating_systems(self):
        for system, label in (("Darwin", "mac"), ("Windows", "windows"), ("Linux", "linux")):
            with self.subTest(system=system):
                self.assertEqual(tokens({"CLAUDECODE": "1"}, system=system)[-1], "os/" + label)
        self.assertEqual(tokens({"CLAUDECODE": "1"}, system="FreeBSD"), ["ai-agent/claude-code"])

    def test_nothing_without_an_agent(self):
        env = {"TERM_PROGRAM": "vscode", "CLAUDE_CODE_USE_BEDROCK": "1", "TERMINAL_EMULATOR": "JetBrains-JediTerm"}
        self.assertEqual(tokens(env), [])
        self.assertEqual(suffix(env), "")

    def test_full_order(self):
        env = {"CLAUDECODE": "1", "TERM_PROGRAM": "vscode", "CLAUDE_CODE_USE_BEDROCK": "1"}
        self.assertEqual(suffix(env, system="Darwin"),
                         "ai-agent/claude-code ai-host/vscode ai-provider/bedrock os/mac")

    def test_opt_out(self):
        env = {"CLAUDECODE": "1", "DX_USER_AGENT_NO_AGENT_INFO": "1"}
        self.assertEqual(suffix(env), "")
        self.assertNotEqual(suffix(dict(env, DX_USER_AGENT_NO_AGENT_INFO="0")), "")

    def test_cap_drops_whole_tokens(self):
        env = {"CLAUDECODE": "1", "TERM_PROGRAM": "vscode", "CLAUDE_CODE_USE_BEDROCK": "1"}
        self.assertLessEqual(len(suffix(env)), agent_info.MAX_SUFFIX_LENGTH)
        with mock.patch.object(agent_info, "MAX_SUFFIX_LENGTH", 40):
            self.assertEqual(suffix(env), "ai-agent/claude-code ai-host/vscode")

    def test_every_label_matches_pattern(self):
        env = {name: "1" for name in ("CLAUDECODE", "CODEX_CI", "GEMINI_CLI", "ANTIGRAVITY_AGENT",
                                      "CURSOR_AGENT", "COPILOT_AGENT", "OPENCODE", "AUGMENT_AGENT",
                                      "AMP_CURRENT_THREAD_ID", "GROK_AGENT", "REPL_ID", "GOOSE_PROVIDER",
                                      "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX")}
        env["TERM_PROGRAM"] = "vscode"
        for token in tokens(env):
            self.assertRegex(token, LABEL_TOKEN)
        labels = set(agent_info._AI_AGENT_VALUES.values())
        labels.update(label for label, _ in agent_info._AGENTS)
        labels.update(label for label, _ in agent_info._HOSTS)
        labels.update(label for label, _ in agent_info._PROVIDERS)
        labels.update(agent_info._OPERATING_SYSTEMS.values())
        for label in labels:
            self.assertRegex(label, r"^[a-z0-9.-]+$")


if __name__ == '__main__':
    unittest.main()
