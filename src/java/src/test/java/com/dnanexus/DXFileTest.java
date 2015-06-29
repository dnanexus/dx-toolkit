// Copyright (C) 2013-2014 DNAnexus, Inc.
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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.DXDataObject.DescribeOptions;
import com.dnanexus.DXFile.Describe;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class DXFileTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXFileTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy();
        }
    }

    @Test
    public void testCreateFileSerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000011112222333344445555\", \"name\": \"foo\", \"media\": \"application/json\"}"),
                mapper.valueToTree(DXFile.newFile()
                        .setProject(DXProject.getInstance("project-000011112222333344445555"))
                        .setName("foo").setMediaType("application/json").buildRequestHash()));
    }

    @Test
    public void testCreateFileSimple() {
        DXFile f = DXFile.newFile().setProject(testProject).setName("foo").build();
        Describe describe = f.describe();
        Assert.assertEquals("foo", describe.getName());
    }

    @Test
    public void testCustomFields() {
        DXFile f = DXFile.newFile().setProject(testProject).setName("test").setMediaType("foo/bar")
                .build();

        // Retrieve some fields and verify that the ones we want are there and the ones we don't
        // want are not there
        DXFile.Describe describe = f.describe(DescribeOptions.get().withCustomFields(
                ImmutableList.of("media")));

        Assert.assertEquals("foo/bar", describe.getMediaType());
        try {
            describe.getName();
            Assert.fail("Expected getName to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }

        // Now describe with some complementary fields and perform the same check
        describe = f.describe(DescribeOptions.get().withCustomFields(ImmutableList.of("name")));

        Assert.assertEquals("test", describe.getName());
        try {
            describe.getMediaType();
            Assert.fail("Expected getMediaType to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    @Test
    public void testDescribeWithOptions() {
        DXFile f =
                DXFile.newFile().setProject(testProject).setName("test").setMediaType("foo/bar")
                        .build();
        Describe describe = f.describe(DescribeOptions.get());
        Assert.assertEquals("test", describe.getName());
        Assert.assertEquals("foo/bar", describe.getMediaType());
    }

    @Test
    public void testGetInstance() {
        DXFile file = DXFile.getInstance("file-000000000000000000000000");
        Assert.assertEquals("file-000000000000000000000000", file.getId());
        Assert.assertEquals(null, file.getProject());

        DXFile file2 =
                DXFile.getInstance("file-000000000000000000000001",
                        DXProject.getInstance("project-123412341234123412341234"));
        Assert.assertEquals("file-000000000000000000000001", file2.getId());
        Assert.assertEquals("project-123412341234123412341234", file2.getProject().getId());

        try {
            DXFile.getInstance(null);
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXFile.getInstance("file-123412341234123412341234", (DXContainer) null);
            Assert.fail("Expected creation without setting project to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXFile.getInstance(null, DXProject.getInstance("project-123412341234123412341234"));
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testDataObjectMethods() {
        DXDataObjectTest.BuilderFactory<DXFile.Builder, DXFile> builderFactory =
                new DXDataObjectTest.BuilderFactory<DXFile.Builder, DXFile>() {
                    @Override
                    public DXFile.Builder getBuilder() {
                        return DXFile.newFile();
                    }
                };
        DXDataObjectTest.testOpenDataObjectMethods(testProject, builderFactory);
        // TODO: test out closed data object methods too
    }

    @Test
    public void testBuilder() {
        DXDataObjectTest.testBuilder(testProject,
                new DXDataObjectTest.BuilderFactory<DXFile.Builder, DXFile>() {
                    @Override
                    public DXFile.Builder getBuilder() {
                        return DXFile.newFile();
                    }
                });

        DXFile file =
                DXFile.newFile().setProject(testProject).setMediaType("application/json").build();
        Assert.assertEquals("application/json", file.describe().getMediaType());
    }

}
