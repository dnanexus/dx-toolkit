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

import java.io.IOException;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.dnanexus.exceptions.InvalidStateException;
import com.dnanexus.exceptions.ResourceNotFoundException;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Common data object tests.
 */
public class DXDataObjectTest {

    /**
     * Object that returns a Builder for test purposes. This allows the test methods to obtain as
     * many data objects of a particular class as they like.
     *
     * @param <T> Builder class
     * @param <U> class of data object to be built
     */
    public interface BuilderFactory<T extends DXDataObject.Builder<T, U>, U extends DXDataObject> {
        /**
         * Returns a builder that generates data objects of the desired type.
         *
         * @return Builder object
         */
        public T getBuilder();
    }

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

    /**
     * Tests that a builder class sets data object fields correctly.
     *
     * @param testProject project in which to create temporary objects
     * @param builderFactory object that returns Builders to be tested
     */
    public static <T extends DXDataObject.Builder<T, U>, U extends DXDataObject> void testBuilder(
            DXContainer testProject, BuilderFactory<T, U> builderFactory) {
        // TODO: builder should fall back to creating the object in the
        // default workspace

        // Setting name
        U namedObject =
                builderFactory.getBuilder().setProject(testProject).setName("myobject").build();
        Assert.assertEquals("myobject", namedObject.describe().getName());

        // Setting tags
        U objectWithTags =
                builderFactory.getBuilder().setProject(testProject).addTags(ImmutableList.of("a"))
                        .addTags(ImmutableList.of("b")).build();
        Assert.assertEquals(ImmutableSet.of("a", "b"),
                ImmutableSet.copyOf(objectWithTags.describe().getTags()));

        U objectWithProperties =
                builderFactory.getBuilder().setProject(testProject)
                        .putProperty("sampleId", "123-456").putProperty("species", "human").build();
        Map<String, String> properties =
                objectWithProperties.describe(DXDataObject.DescribeOptions.get().withProperties())
                        .getProperties();
        Assert.assertEquals(ImmutableMap.of("sampleId", "123-456", "species", "human"), properties);
        try {
            objectWithProperties.describe().getProperties();
            Assert.fail("Expected getProperties to fail with IllegalStateException when properties were not retrieved");
        } catch (IllegalStateException e) {
            // Expected
        }

        // Setting types
        U objectWithTypes =
                builderFactory.getBuilder().setProject(testProject)
                        .addTypes(ImmutableList.of("Loud")).addTypes(ImmutableList.of("Noisy"))
                        .build();
        Assert.assertEquals(ImmutableSet.of("Loud", "Noisy"),
                ImmutableSet.copyOf(objectWithTypes.describe().getTypes()));

        // Setting visibility
        U hiddenObject =
                builderFactory.getBuilder().setProject(testProject).setVisibility(false).build();
        Assert.assertFalse(hiddenObject.describe().isVisible());
        U visibleObject =
                builderFactory.getBuilder().setProject(testProject).setVisibility(true).build();
        Assert.assertTrue(visibleObject.describe().isVisible());
        U objectWithoutSetVisibilityCall =
                builderFactory.getBuilder().setProject(testProject).build();
        Assert.assertTrue(objectWithoutSetVisibilityCall.describe().isVisible());

        // Setting details
        SampleMetadata sampleMetadata = new SampleMetadata("bar");
        U objectWithDetails =
                builderFactory.getBuilder().setProject(testProject).setDetails(sampleMetadata)
                        .build();
        Assert.assertEquals("bar",
                objectWithDetails.describe(DXDataObject.DescribeOptions.get().withDetails())
                        .getDetails(SampleMetadata.class).getSampleId());
        try {
            objectWithDetails.describe().getDetails(SampleMetadata.class);
            Assert.fail("Expected getDetails to fail with IllegalStateException when details were not retrieved");
        } catch (IllegalStateException e) {
            // Expected
        }

        // "parents" flag on creation
        try {
            builderFactory.getBuilder().setProject(testProject).setFolder("/does/not/exist")
                    .build();
            Assert.fail("Expected creating object in a nonexistent folder to fail");
        } catch (ResourceNotFoundException e) {
            // Expected
        }
        // Same call with parents=true should succeed
        builderFactory.getBuilder().setProject(testProject).setFolder("/does/not/exist", true)
                .build();
    }

    /**
     * Tests methods on closed data objects, optionally skipping the close operation.
     *
     * @param testProject project in which to create temporary objects
     * @param builderFactory object that returns Builders to be tested
     */
    public static <T extends DXDataObject.Builder<T, U>, U extends DXDataObject> void testClosedDataObjectMethods(
            DXContainer testProject, BuilderFactory<T, U> builderFactory) {
        testClosedDataObjectMethods(testProject, builderFactory, false);
    }

    /**
     * Tests methods on closed data objects, optionally skipping the close operation.
     *
     * @param testProject project in which to create temporary objects
     * @param builderFactory object that returns Builders to be tested
     * @param skipClose if true, doesn't actually attempt to close the data object (useful for
     *        applets, which are created in the closed state)
     */
    public static <T extends DXDataObject.Builder<T, U>, U extends DXDataObject> void testClosedDataObjectMethods(
            DXContainer testProject, BuilderFactory<T, U> builderFactory, boolean skipClose) {

        // Note that .close returns DXDataObject, not the specific subclass
        U closedDataObject = builderFactory.getBuilder().setProject(testProject).build();
        // Object may not necessarily be in the "closed" state after this
        // operation, but the metadata fields we'll try to modify below should
        // be fixed now. So, save some time by not waiting for the close
        // operation to complete.
        if (!skipClose) {
            closedDataObject.close();
        }

        Assert.assertNotSame(DataObjectState.OPEN, closedDataObject.describe().getState());

        try {
            closedDataObject.addTypes(ImmutableList.<String>of());
            Assert.fail("Expected setting types on a closed object to fail");
        } catch (InvalidStateException e) {
            // Expected
        }
        try {
            closedDataObject.setVisibility(false);
            Assert.fail("Expected setting visibility on a closed object to fail");
        } catch (InvalidStateException e) {
            // Expected
        }
        SampleMetadata sampleMetadata = new SampleMetadata("foo");
        try {
            closedDataObject.setDetails(sampleMetadata);
            Assert.fail("Expected setting details on a closed object to fail");
        } catch (InvalidStateException e) {
            // Expected
        }

        // The following operations should still work when the object has been closed
        closedDataObject.addTags(ImmutableList.of("a", "b"));
        closedDataObject.putProperty("species", "human");
    }

    /**
     * Tests methods on open data objects.
     *
     * @param testProject project in which to create temporary objects
     * @param builderFactory object that returns Builders to be tested
     */
    public static <T extends DXDataObject.Builder<T, U>, U extends DXDataObject> void testOpenDataObjectMethods(
            DXContainer testProject, BuilderFactory<T, U> builderFactory) {
        U dataObject = builderFactory.getBuilder().setProject(testProject).setName("foo").build();

        // Adding and removing tags
        dataObject.addTags(ImmutableList.of("a", "b"));
        Assert.assertEquals(ImmutableSet.of("a", "b"),
                ImmutableSet.copyOf(dataObject.describe().getTags()));
        dataObject.removeTags(ImmutableList.of("b", "c"));
        Assert.assertEquals(ImmutableList.of("a"), dataObject.describe().getTags());
        // TODO: test fallback to default workspace for retrieval of project-specific metadata

        // Adding and removing types
        dataObject.addTypes(ImmutableList.of("Loud", "Noisy"));
        Assert.assertEquals(ImmutableSet.of("Loud", "Noisy"),
                ImmutableSet.copyOf(dataObject.describe().getTypes()));
        dataObject.removeTypes(ImmutableList.of("Noisy", "Fast"));
        Assert.assertEquals(ImmutableList.of("Loud"), dataObject.describe().getTypes());

        // Setting visibility
        Assert.assertTrue(dataObject.describe().isVisible());
        dataObject.setVisibility(false);
        Assert.assertFalse(dataObject.describe().isVisible());
        dataObject.setVisibility(true);
        Assert.assertTrue(dataObject.describe().isVisible());

        // Setting properties
        dataObject.putAllProperties(ImmutableMap.of("city", "Mountain View", "species", "human"));
        Assert.assertEquals(ImmutableMap.of("city", "Mountain View", "species", "human"),
                dataObject.describe(DXDataObject.DescribeOptions.get().withProperties())
                        .getProperties());
        dataObject.removeProperty("city");
        Assert.assertEquals(ImmutableMap.of("species", "human"),
                dataObject.describe(DXDataObject.DescribeOptions.get().withProperties())
                        .getProperties());

        // Setting details
        SampleMetadata sampleMetadata = new SampleMetadata("foo");
        dataObject.setDetails(sampleMetadata);
        Assert.assertEquals(
                "foo",
                dataObject.describe(DXDataObject.DescribeOptions.get().withDetails())
                        .getDetails(SampleMetadata.class).getSampleId());

        // Listing projects with this object
        Map<DXContainer, AccessLevel> projectList = dataObject.listProjects();
        Assert.assertTrue(projectList.containsKey(testProject));
        Assert.assertEquals(1, projectList.size());
        Assert.assertEquals(AccessLevel.ADMINISTER, projectList.get(testProject));
    }

    /**
     * Tests deserialization of the result of the listProjects call.
     *
     * @throws IOException if there is a problem deserializing JSON
     */
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

    /**
     * Tests that invalid IDs are rejected.
     */
    @Test
    public void testIdChecking() {
        try {
            DXRecord.getInstance("record-00001111222233334444");
            Assert.fail("Expected IllegalArgumentException to be thrown because ID was invalid");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            DXRecord.getInstance("file-000011112222333344445555");
            Assert.fail("Expected IllegalArgumentException to be thrown because ID was invalid");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }
}
