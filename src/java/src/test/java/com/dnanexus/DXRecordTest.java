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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.exceptions.InvalidStateException;
import com.dnanexus.exceptions.ResourceNotFoundException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class DXRecordTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private DXProject testProject;

    @JsonInclude(Include.NON_NULL)
    private static class SampleMetadata {
        @JsonProperty
        private String sampleId;

        @SuppressWarnings("unused")
        private SampleMetadata() {}

        public SampleMetadata(String sampleId) {
            this.sampleId = sampleId;
        }

        public String getSampleId() {
            return this.sampleId;
        }
    }

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
        } catch (IllegalArgumentException e) {
            // Expected
        }

        DXProject p = DXProject.newProject().setName("Java test project").build();

        // Setting the same field more than once is disallowed
        try {
            DXRecord.newRecord().setProject(p)
                    .setProject(DXProject.getInstance("project-000000000000000000000000"))
                    .buildRequestHash();
            Assert.fail("Expected double setting of setProject to fail");
        } catch (IllegalArgumentException e) {
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
        DXRecord record = DXRecord.newRecord().setProject(testProject).setName("foo").build();

        // Adding and removing tags
        record.addTags(ImmutableList.of("a", "b"));
        Assert.assertEquals(ImmutableSet.of("a", "b"),
                ImmutableSet.copyOf(record.describe().getTags()));
        record.removeTags(ImmutableList.of("b", "c"));
        Assert.assertEquals(ImmutableList.of("a"), record.describe().getTags());
        // TODO: test fallback to default workspace for retrieval of project-specific metadata

        // Adding and removing types
        record.addTypes(ImmutableList.of("Loud", "Noisy"));
        Assert.assertEquals(ImmutableSet.of("Loud", "Noisy"),
                ImmutableSet.copyOf(record.describe().getTypes()));
        record.removeTypes(ImmutableList.of("Noisy", "Fast"));
        Assert.assertEquals(ImmutableList.of("Loud"), record.describe().getTypes());

        // Setting visibility
        Assert.assertTrue(record.describe().isVisible());
        record.setVisibility(false);
        Assert.assertFalse(record.describe().isVisible());
        record.setVisibility(true);
        Assert.assertTrue(record.describe().isVisible());

        // Setting properties
        record.putAllProperties(ImmutableMap.of("city", "Mountain View", "species", "human"));
        Assert.assertEquals(ImmutableMap.of("city", "Mountain View", "species", "human"), record
                .describe(DXDataObject.DescribeOptions.get().withProperties()).getProperties());
        record.removeProperty("city");
        Assert.assertEquals(ImmutableMap.of("species", "human"),
                record.describe(DXDataObject.DescribeOptions.get().withProperties())
                        .getProperties());

        // Setting details
        SampleMetadata sampleMetadata = new SampleMetadata("foo");
        record.setDetails(sampleMetadata);
        Assert.assertEquals(
                "foo",
                record.describe(DXDataObject.DescribeOptions.get().withDetails())
                        .getDetails(SampleMetadata.class).getSampleId());

        // Listing projects with this object
        Map<DXContainer, AccessLevel> projectList = record.listProjects();
        Assert.assertTrue(projectList.containsKey(testProject));
        Assert.assertEquals(1, projectList.size());
        Assert.assertEquals(AccessLevel.ADMINISTER, projectList.get(testProject));

        DXRecord closedRecord = DXRecord.newRecord().setProject(testProject).build().close();

        try {
            closedRecord.addTypes(ImmutableList.<String>of());
            Assert.fail("Expected setting types on a closed object to fail");
        } catch (InvalidStateException e) {
            // Expected
        }
        try {
            closedRecord.setVisibility(false);
            Assert.fail("Expected setting visibility on a closed object to fail");
        } catch (InvalidStateException e) {
            // Expected
        }
        try {
            closedRecord.setDetails(sampleMetadata);
            Assert.fail("Expected setting details on a closed object to fail");
        } catch (InvalidStateException e) {
            // Expected
        }

        // The following operations should still work when the object has been closed
        closedRecord.addTags(ImmutableList.of("a", "b"));
        closedRecord.putProperty("species", "human");

    }

    @Test
    public void testBuilder() {
        // TODO: builder should fall back to creating the record in the default workspace

        // Setting name
        DXRecord namedRecord =
                DXRecord.newRecord().setProject(testProject).setName("myrecord").build();
        Assert.assertEquals("myrecord", namedRecord.describe().getName());

        // Setting tags
        DXRecord recordWithTags =
                DXRecord.newRecord().setProject(testProject).addTags(ImmutableList.of("a", "b"))
                        .build();
        Assert.assertEquals(ImmutableSet.of("a", "b"),
                ImmutableSet.copyOf(recordWithTags.describe().getTags()));

        DXRecord recordWithProperties =
                DXRecord.newRecord().setProject(testProject).putProperty("sampleId", "123-456")
                        .putProperty("species", "human").build();
        Map<String, String> properties =
                recordWithProperties.describe(DXDataObject.DescribeOptions.get().withProperties())
                        .getProperties();
        Assert.assertEquals(ImmutableMap.of("sampleId", "123-456", "species", "human"), properties);

        // Setting types
        DXRecord recordWithTypes =
                DXRecord.newRecord().setProject(testProject)
                        .addTypes(ImmutableList.of("Loud", "Noisy")).build();
        Assert.assertEquals(ImmutableSet.of("Loud", "Noisy"),
                ImmutableSet.copyOf(recordWithTypes.describe().getTypes()));

        // Setting visibility
        DXRecord hiddenRecord =
                DXRecord.newRecord().setProject(testProject).setVisibility(false).build();
        DXRecord visibleRecord =
                DXRecord.newRecord().setProject(testProject).setVisibility(true).build();
        DXRecord recordWithoutSetVisibilityCall =
                DXRecord.newRecord().setProject(testProject).build();
        Assert.assertFalse(hiddenRecord.describe().isVisible());
        Assert.assertTrue(visibleRecord.describe().isVisible());
        Assert.assertTrue(recordWithoutSetVisibilityCall.describe().isVisible());

        // Setting details
        SampleMetadata sampleMetadata = new SampleMetadata("bar");
        DXRecord recordWithDetails =
                DXRecord.newRecord().setProject(testProject).setDetails(sampleMetadata).build();
        Assert.assertEquals("bar",
                recordWithDetails.describe(DXDataObject.DescribeOptions.get().withDetails())
                        .getDetails(SampleMetadata.class).getSampleId());

        // "parents" flag on creation
        try {
            DXRecord.newRecord().setProject(testProject).setFolder("/does/not/exist").build();
            Assert.fail("Expected creating record in a nonexistent folder to fail");
        } catch (ResourceNotFoundException e) {
            // Expected
        }
        // Same call with parents=true should succeed
        DXRecord.newRecord().setProject(testProject).setFolder("/does/not/exist", true).build();
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
