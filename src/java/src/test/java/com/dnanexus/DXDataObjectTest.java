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
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

public class DXDataObjectTest {

    // Note: many more tests for generic data object functionality live in DXRecordTest.java.

    @Test
    public void testDeserializeProjectsMap() throws IOException {
        JsonNode input =
                DXJSON.parseJson("{\"project-0000\": \"CONTRIBUTE\", \"project-1111\": \"ADMINISTER\"}");
        Map<String, AccessLevel> listProjectsResponse =
                DXDataObject.deserializeListProjectsMap(input);
        Map<String, AccessLevel> expected =
                ImmutableMap.of("project-0000", AccessLevel.CONTRIBUTE, "project-1111",
                        AccessLevel.ADMINISTER);
        Assert.assertEquals(expected, listProjectsResponse);
    }
}
