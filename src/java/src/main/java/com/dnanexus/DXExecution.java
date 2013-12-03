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

/**
 * An execution (job or analysis).
 */
public abstract class DXExecution extends DXObject {

    /**
     * Initializes a new execution with the specified execution ID and environment.
     */
    protected DXExecution(String dxId, DXEnvironment env) {
        super(dxId, env);
    }

    /**
     * Returns the output of the execution, deserialized to the specified class.
     *
     * @param outputClass class to deserialize to
     *
     * @return execution output
     * @throws IllegalStateException if the execution is not in the DONE state.
     */
    public abstract <T> T getOutput(Class<T> outputClass) throws IllegalStateException;

    /**
     * Terminates the execution.
     */
    public void terminate() {
        DXAPI.jobTerminate(this.dxId);
    }

    /**
     * Waits until the execution has successfully completed and is in the DONE state.
     *
     * @return the same DXExecution object
     *
     * @throws IllegalStateException if the job reaches the FAILED or TERMINATED state.
     */
    public abstract DXExecution waitUntilDone() throws IllegalStateException;

}
