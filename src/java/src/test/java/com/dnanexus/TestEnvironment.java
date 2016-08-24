// Copyright (C) 2013-2016 DNAnexus, Inc.
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

    /**
     * A special feature, not present in all nucleus test or production environments, that is
     * required by a test.
     */
    public enum ConfigOption {

        // TODO: support the DXTEST_FULL environment variable, which behaves as if all the
        // individual environment variables below were set.

        /**
         * Run tests that require unreleased server features.
         */
        RUN_NEXT_TESTS("DX_RUN_NEXT_TESTS"),

        /**
         * Run tests that can create apps. The tests are liable to expect the apps to be
         * initializable in an environment where no app of that name currently exists, so it must be
         * possible to clean the environment before the tests start.
         */
        ISOLATED_ENV("DXTEST_ISOLATED_ENV"),

        /**
         * Run tests that use a FUSE filesystem.
         */
        FUSE("DXTEST_FUSE"),

        /**
         * Run tests that can spawn jobs. The tests are liable to wait for the jobs to complete, so
         * at least one worker must be present to chew on incoming jobs.
         */
        RUN_JOBS("DXTEST_RUN_JOBS"),

        /**
         * Run tests that use squid3 to launch an HTTP proxy.
         */
        HTTP_PROXY("DXTEST_HTTP_PROXY"),

        /**
         * Run tests that are liable to clobber your local environment.
         */
        ENV("DXTEST_ENV");

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
