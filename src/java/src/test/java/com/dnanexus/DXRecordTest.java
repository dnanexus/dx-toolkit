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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DXRecordTest {

    /**
     * Sample object to be serialized into "details" for test data objects.
     */
    @JsonInclude(Include.NON_NULL)
    public static class SampleMetadata {
        @JsonProperty
        private String sampleId;

        @SuppressWarnings("unused")
        private SampleMetadata() {}

        /**
         * Initializes a metadata object with the specified sample ID.
         *
         * @param sampleId sample ID
         */
        public SampleMetadata(String sampleId) {
            this.sampleId = sampleId;
        }

        /**
         * Returns the sample ID.
         *
         * @return sample ID
         */
        public String getSampleId() {
            return this.sampleId;
        }
    }

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
    public void testCustomFields() {
        DXRecord record = DXRecord.newRecord().setProject(testProject).setName("foo")
                .setFolder("/").build();

        // Retrieve some fields and verify that the ones we want are there and the ones we don't
        // want are not there
        DXRecord.Describe describe = record.describe(DescribeOptions.get().withCustomFields("name",
                "created", "details", "folder", "modified"));

        Assert.assertEquals("foo", describe.getName());
        Assert.assertTrue(describe.getCreationDate().getTime() > 0);
        Assert.assertNotNull(describe.getDetails(SampleMetadata.class));
        Assert.assertEquals("/", describe.getFolder());
        Assert.assertTrue(describe.getModificationDate().getTime() > 0);
        try {
            describe.getProject();
            Assert.fail("Expected getProject to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getProperties();
            Assert.fail("Expected getProperties to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getState();
            Assert.fail("Expected getState to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getTags();
            Assert.fail("Expected getTags to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getTypes();
            Assert.fail("Expected getTypes to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.isVisible();
            Assert.fail("Expected isVisible to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }

        // Now retrieve the complementary set of fields and do the same
        describe = record.describe(DescribeOptions.get().withCustomFields("project", "properties",
                "state", "tags", "types", "hidden"));
        Assert.assertEquals(testProject, describe.getProject());
        Assert.assertTrue(describe.getProperties().isEmpty());
        Assert.assertEquals(DataObjectState.OPEN, describe.getState());
        Assert.assertTrue(describe.getTags().isEmpty());
        Assert.assertTrue(describe.getTypes().isEmpty());
        Assert.assertTrue(describe.isVisible());
        try {
            describe.getName();
            Assert.fail("Expected getName to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getFolder();
            Assert.fail("Expected getFolder to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getCreationDate();
            Assert.fail("Expected getCreationDate to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getDetails(SampleMetadata.class);
            Assert.fail("Expected getDetails to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getModificationDate();
            Assert.fail("Expected getModificationDate to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    @Test
    public void testDescribeWithOptions() {
        DXRecord r = DXRecord.newRecord().setProject(testProject).setName("foo").build();
        DXRecord.Describe describe = r.describe(DescribeOptions.get());
        Assert.assertEquals("foo", describe.getName());
    }

    @Test
    public void testGetInstance() {
        DXRecord record = DXRecord.getInstance("record-000011112222333344445555");
        Assert.assertEquals("record-000011112222333344445555", record.getId());
        Assert.assertEquals(null, record.getProject());

        DXRecord record2 =
                DXRecord.getInstance("record-000100020003000400050006",
                        DXProject.getInstance("project-123412341234123412341234"));
        Assert.assertEquals("record-000100020003000400050006", record2.getId());
        Assert.assertEquals("project-123412341234123412341234", record2.getProject().getId());

        try {
            DXRecord.getInstance(null);
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXRecord.getInstance("record-123412341234123412341234", (DXContainer) null);
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
