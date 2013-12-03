// Copyright (C) 2013 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
//   Licensed under the Apache License, Version 2.0 (the "License"); you may
//   not use this file except in compliance with the License. You may obtain a
//   copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//   License for the specific language governing permissions and limitations
//   under the License.

package com.dnanexus;

import java.util.Map;

/**
 * Utility class containing helpers for test configuration.
 */
public class TestEnvironment {

    public enum ConfigOption {
        RUN_JOBS("DXTEST_RUN_JOBS");

        private String envVarName;

        private ConfigOption(String envVarName) {
            this.envVarName = envVarName;
        }

        private String getEnvironmentVariable() {
            return this.envVarName;
        }
    }

    /**
     * Returns true if tests with the specified configuration flag can be run.
     *
     * @param configParam configuration flag
     *
     * @return whether the test can be run
     */
    public static boolean canRunTest(ConfigOption configParam) {
        Map<String, String> environment = System.getenv();
        String environmentVariable = configParam.getEnvironmentVariable();
        return environment.containsKey(environmentVariable)
                && !environment.get(environmentVariable).isEmpty()
                && !environment.get(environmentVariable).equals("0");
    }
}
