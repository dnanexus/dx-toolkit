// Copyright (C) 2014-2015 DNAnexus, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class InputAndOutputParameterTest {
    @Test
    public void testFactoryMethods() {
        try {
            InputParameter.newInputParameter("foo", null);
            Assert.fail("Expected InputParameter with null class to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            InputParameter.newInputParameter(null, IOClass.FILE);
            Assert.fail("Expected InputParameter with null name to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            OutputParameter.newOutputParameter("foo", null);
            Assert.fail("Expected OutputParameter with null class to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            OutputParameter.newOutputParameter(null, IOClass.FILE);
            Assert.fail("Expected OutputParameter with null name to fail");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testInputParamDeserialization() {
        InputParameter ip = DXJSON.safeTreeToValue(
                DXJSON.getObjectBuilder().put("name", "foo").put("class", "string")
                        .put("optional", true).put("label", "mylabel")
                        .put("unrecognizedField", "asdf").build(), InputParameter.class);
        Assert.assertEquals("foo", ip.getName());
        Assert.assertEquals(IOClass.STRING, ip.getIOClass());
        Assert.assertTrue(ip.isOptional());
    }

    @Test
    public void testOutputParamDeserialization() {
        OutputParameter op = DXJSON.safeTreeToValue(
                DXJSON.getObjectBuilder().put("name", "foo").put("class", "string")
                        .put("optional", true).put("label", "mylabel")
                        .put("unrecognizedField", "asdf").build(), OutputParameter.class);
        Assert.assertEquals("foo", op.getName());
        Assert.assertEquals(IOClass.STRING, op.getIOClass());
        Assert.assertTrue(op.isOptional());
    }
}
