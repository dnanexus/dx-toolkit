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

/**
 * Utility class that produces an appropriate user-agent string for the Java
 * client.
 */
class DXUserAgent {
    /**
     * Returns a user-agent string.
     */
    public static String getUserAgent() {
        // Contains the following pieces of info:
        // dx-toolkit version (e.g. 0.100.0)
        // host operating system
        // Java specification version (e.g. 1.6.0_27)
        // VM name and version
        return "dxjava/" + DXToolkitVersion.TOOLKIT_VERSION + " " + System.getProperty("os.name").replace(" ", "")
                + " java/" + System.getProperty("java.version") + " [" + System.getProperty("java.vm.name") + "]/"
                + System.getProperty("java.vm.version");
    }
}
