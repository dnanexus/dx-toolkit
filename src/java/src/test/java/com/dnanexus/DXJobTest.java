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

import java.io.IOException;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class DXJobTest {

    @Test
    public void testJobDescribeDeserialization() throws IOException {
        DXJob.Describe describe =
                new DXJob.Describe(
                        DXJSON.safeTreeToValue(
                                DXJSON.parseJson("{\"id\": \"job-000000000000000000000000\", "
                                        + "\"parentJob\": \"job-000000000000000000000001\", \"created\": 1234567890000, "
                                        + "\"modified\": 1234567890123, \"name\": \"my job\", \"state\": \"done\"}"),
                                DXJob.DescribeResponseHash.class), DXEnvironment.create());
        DXJob parentJob = DXJob.getInstance("job-000000000000000000000001");
        Assert.assertEquals("job-000000000000000000000000", describe.getId());
        Assert.assertEquals("my job", describe.getName());
        Assert.assertEquals(new Date(1234567890000L), describe.getCreationDate());
        Assert.assertEquals(new Date(1234567890123L), describe.getModifiedDate());
        Assert.assertEquals(parentJob, describe.getParentJob());
        Assert.assertEquals(JobState.DONE, describe.getState());

        // Extra fields in the response should not cause us to choke (for API
        // forward compatibility)
        DXJSON.safeTreeToValue(DXJSON.parseJson("{\"notAField\": true}"),
                DXJob.DescribeResponseHash.class);
    }

}
