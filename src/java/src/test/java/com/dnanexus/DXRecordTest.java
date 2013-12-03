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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.DXDataObjectTest.SampleMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DXRecordTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXRecordTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy();
        }
    }

    // External tests

    @Test
    public void testCreateRecordSimple() {
        // Project is required
        //
        // TODO: this test may need to be fixed if environment has a workspace set
        try {
            DXRecord.newRecord().setName("foo").buildRequestHash();
            Assert.fail("Expected creation without setting project to fail");
        } catch (IllegalStateException e) {
            // Expected
        }

        DXProject p = DXProject.newProject().setName("Java test project").build();

        // Setting the same field more than once is disallowed
        try {
            DXRecord.newRecord().setProject(p)
                    .setProject(DXProject.getInstance("project-000000000000000000000000"));
            Assert.fail("Expected double setting of setProject to fail");
        } catch (IllegalStateException e) {
            // Expected
        }

        // A successful record
        DXRecord r = DXRecord.newRecord().setProject(p).setName("foo").build();
        Assert.assertEquals("foo", r.describe().getName());

        p.destroy();
    }

    @Test
    public void testGetInstance() {
        DXRecord record = DXRecord.getInstance("record-0000");
        Assert.assertEquals("record-0000", record.getId());
        Assert.assertEquals(null, record.getProject());

        DXRecord record2 =
                DXRecord.getInstance("record-0001",
                        DXProject.getInstance("project-123412341234123412341234"));
        Assert.assertEquals("record-0001", record2.getId());
        Assert.assertEquals("project-123412341234123412341234", record2.getProject().getId());

        try {
            DXRecord.getInstance(null);
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXRecord.getInstance("record-1234", (DXContainer) null);
            Assert.fail("Expected creation without setting project to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXRecord.getInstance(null, DXProject.getInstance("project-123412341234123412341234"));
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testDataObjectMethods() {
        DXDataObjectTest.BuilderFactory<DXRecord.Builder, DXRecord> builderFactory =
                new DXDataObjectTest.BuilderFactory<DXRecord.Builder, DXRecord>() {
                    @Override
                    public DXRecord.Builder getBuilder() {
                        return DXRecord.newRecord();
                    }
                };
        DXDataObjectTest.testOpenDataObjectMethods(testProject, builderFactory);
        DXDataObjectTest.testClosedDataObjectMethods(testProject, builderFactory);
    }

    @Test
    public void testBuilder() {
        DXDataObjectTest.testBuilder(testProject,
                new DXDataObjectTest.BuilderFactory<DXRecord.Builder, DXRecord>() {
                    @Override
                    public DXRecord.Builder getBuilder() {
                        return DXRecord.newRecord();
                    }
                });
    }

    // Internal tests

    @Test
    public void testCreateRecordSerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000011112222333344445555\", \"name\": \"foo\"}"),
                mapper.valueToTree(DXRecord.newRecord()
                        .setProject(DXProject.getInstance("project-000011112222333344445555"))
                        .setName("foo").buildRequestHash()));

        // Properties and details have particularly tricky serialization

        SampleMetadata sampleMetadata = new SampleMetadata("bar");
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000011112222333344445555\", \"properties\": {\"species\": \"human\"}, \"details\": {\"sampleId\": \"bar\"}}"),
                mapper.valueToTree(DXRecord.newRecord()
                        .setProject(DXProject.getInstance("project-000011112222333344445555"))
                        .putProperty("species", "human").setDetails(sampleMetadata)
                        .buildRequestHash()));
    }

}
