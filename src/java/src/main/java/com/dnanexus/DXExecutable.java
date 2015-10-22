// Copyright (C) 2013-2015 DNAnexus, Inc.
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
 * An executable (applet, app, or workflow).
 *
 * @param <T> type of the execution object that is produced when the executable is run (job or
 *        analysis).
 */
public interface DXExecutable<T extends DXExecution> {
    /**
     * Returns the ID of the object.
     *
     * @return the DNAnexus object ID
     */
    public String getId();

    /**
     * Returns an object for creating a new run of this executable.
     *
     * @return executable runner
     */
    public ExecutableRunner<T> newRun();
}
