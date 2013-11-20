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
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests DXSearch methods.
 */
public class DXSearchTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXSearchTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy();
        }
    }

    /**
     * Asserts that the contents of the Iterable are the given values (in some order).
     *
     * @param actualIterable Iterable of actual results
     * @param expected Expected results
     */
    public static <T> void assertEqualsAnyOrder(Iterable<T> actualIterable, T... expected) {
        Set<T> expectedSet = Sets.newHashSet(Arrays.asList(expected));
        Set<T> actualSet = Sets.newHashSet(actualIterable);
        Assert.assertEquals(expectedSet, actualSet);
    }

    // External tests

    /**
     * findDataObjects smoke test.
     */
    @Test
    public void testFindDataObjectsSimple() {
        Assert.assertEquals(0, DXSearch.findDataObjects().inProject(testProject)
                .nameMatchesExactly("foobarbaz").execute().asList().size());
    }

    /**
     * Tests paging through results.
     */
    @Test
    public void testFindDataObjectsWithPaging() {
        List<DXRecord> records = Lists.newArrayList();
        Set<String> recordIds = Sets.newHashSet();
        for (int i = 0; i < 8; ++i) {
            DXRecord record =
                    DXRecord.newRecord().setProject(testProject)
                            .setName("foo" + Integer.toString(i)).build();
            records.add(record);
            recordIds.add(record.getId());
        }
        List<DXRecord> outputRecords =
                DXSearch.findDataObjects().inProject(testProject).nameMatchesGlob("foo*")
                        .withClassRecord().execute().asList();
        Assert.assertEquals(8, outputRecords.size());

        List<DXRecord> outputRecordsWithPaging =
                DXSearch.findDataObjects().inProject(testProject).nameMatchesGlob("foo*")
                        .withClassRecord().execute(3).asList();
        Assert.assertEquals(outputRecords, outputRecordsWithPaging);
        Set<String> outputRecordIds = Sets.newHashSet();
        for (DXRecord record : outputRecordsWithPaging) {
            outputRecordIds.add(record.getId());
        }

        Assert.assertEquals(recordIds, outputRecordIds);
    }

    /**
     * Tests a variety of findDataObjects features.
     */
    @Test
    public void testFindDataObjects() {
        DXRecord moo =
                DXRecord.newRecord().setProject(testProject).setName("Moo")
                        .putProperty("sampleId", "1").addTypes(ImmutableList.of("genome")).build()
                        .close();
        DXRecord foo =
                DXRecord.newRecord().setProject(testProject).setName("foo")
                        .putProperty("sampleId", "2").addTags(ImmutableList.of("mytag")).build()
                        .close();
        DXRecord food =
                DXRecord.newRecord().setProject(testProject).setName("food")
                        .setFolder("/subfolder", true).build().close();
        DXRecord open = DXRecord.newRecord().setProject(testProject).setName("open").build();
        DXRecord invisible =
                DXRecord.newRecord().setProject(testProject).setName("invisible")
                        .setVisibility(false).build().close();

        // nameMatches*

        assertEqualsAnyOrder(
                DXSearch.findDataObjects().nameMatchesExactly("foo").inProject(testProject)
                        .execute().asList(), foo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().nameMatchesGlob("foo*").inProject(testProject).execute()
                        .asList(), foo, food);
        assertEqualsAnyOrder(DXSearch.findDataObjects().nameMatchesRegexp("[a-m]oo[^x]?")
                .inProject(testProject).execute().asList(), foo, food);
        assertEqualsAnyOrder(DXSearch.findDataObjects().nameMatchesRegexp("[a-m]oo[^x]?", true)
                .inProject(testProject).execute().asList(), moo, foo, food);

        // {created,modified}{Before,After}

        // We rely on the fact that moo.created < foo.created < food.created

        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .createdBefore(moo.describe().getCreationDate()).execute().asList(), moo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .createdAfter(foo.describe().getCreationDate()).execute().asList(), foo,
                food, open);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .modifiedBefore(foo.describe().getModificationDate()).execute().asList(),
                moo, foo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .modifiedAfter(foo.describe().getModificationDate()).execute().asList(),
                foo, food, open);

        // inFolder and friends

        assertEqualsAnyOrder(DXSearch.findDataObjects().inFolder(testProject, "/").execute()
                .asList(), moo, foo, open);
        assertEqualsAnyOrder(DXSearch.findDataObjects().inFolderOrSubfolders(testProject, "/")
                .execute().asList(), moo, foo, food, open);

        // withProperty

        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject).withProperty("sampleId")
                        .execute().asList(), moo, foo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject).withProperty("sampleId", "2")
                        .execute().asList(), foo);

        // withState

        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject).withState(DataObjectState.CLOSED)
                        .execute().asList(), moo, foo, food);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject).withState(DataObjectState.OPEN)
                        .execute().asList(), open);

        // withTags

        assertEqualsAnyOrder(DXSearch.findDataObjects().inProject(testProject).withTag("mytag")
                .execute().asList(), foo);

        // withTypes

        assertEqualsAnyOrder(DXSearch.findDataObjects().inProject(testProject).withType("genome")
                .execute().asList(), moo);

        // withVisibility

        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .withVisibility(DXSearch.VisibilityQuery.HIDDEN).execute().asList(),
                invisible);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .withVisibility(DXSearch.VisibilityQuery.VISIBLE).execute().asList(), moo,
                foo, food, open);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .withVisibility(DXSearch.VisibilityQuery.EITHER).execute().asList(), moo,
                foo, food, open, invisible);

        // TODO: withLinkTo, withMinimumAccessLevel

    }

    @Test
    public void testFindJobs() {
        @SuppressWarnings("unused")
        List<DXJob> results = DXSearch.findJobs().launchedBy("user-dnanexus").execute().asList();
    }

    // Internal tests

    /**
     * Tests formulating findDataObjects queries without actually issuing them.
     */
    @Test
    public void testFindDataObjectsQuerySerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"scope\": {\"project\":\"project-000000000000000000000000\"}}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"scope\": {\"project\":\"project-000000000000000000000000\", \"folder\": \"/my/subfolder\", \"recurse\": false}}"),
                mapper.valueToTree(DXSearch
                        .findDataObjects()
                        .inFolder(DXProject.getInstance("project-000000000000000000000000"),
                                "/my/subfolder").buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"scope\": {\"project\": \"project-000000000000000000000000\"}, \"name\": \"dnanexus\"}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .nameMatchesExactly("dnanexus").buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": \"dnanexus\"}"),
                mapper.valueToTree(DXSearch.findDataObjects().nameMatchesExactly("dnanexus")
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"regexp\": \"(DNA|dna)nexus\"}}"),
                mapper.valueToTree(DXSearch.findDataObjects().nameMatchesRegexp("(DNA|dna)nexus")
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"regexp\": \"[dr]nanexus\"}}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .nameMatchesRegexp("[dr]nanexus", false).buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"regexp\": \"[dr]nanexus\", \"flags\": \"i\"}}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .nameMatchesRegexp("[dr]nanexus", true).buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"glob\": \"*nexus\"}}"),
                mapper.valueToTree(DXSearch.findDataObjects().nameMatchesGlob("*nexus")
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"visibility\": \"hidden\"}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .withVisibility(DXSearch.VisibilityQuery.HIDDEN).buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"level\": \"ADMINISTER\"}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .withMinimumAccessLevel(AccessLevel.ADMINISTER).buildRequestHash()));

        try {
            DXSearch.findDataObjects().inProject(DXProject.getInstance("project-0000"))
                    .inProject(DXProject.getInstance("project-1111"));
            Assert.fail("Expected double setting of inProject to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findDataObjects().inFolder(DXProject.getInstance("project-0000"), "/1")
                    .inFolder(DXProject.getInstance("project-0000"), "/2");
            Assert.fail("Expected double setting of inFolder to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findDataObjects().nameMatchesExactly("ab").nameMatchesGlob("*b");
            Assert.fail("Expected double setting of name parameters to fail");
        } catch (IllegalStateException e) {
            // Expected
        }

        try {
            DXSearch.findDataObjects().withMinimumAccessLevel(AccessLevel.NONE);
            Assert.fail("Expected minimumAccessLevel=NONE to fail");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    public void testFindDataObjectsResponseSerialization() throws IOException {
        // Test deserialization of the result without making a real API call
        DXJSON.safeTreeToValue(
                DXJSON.parseJson("{\"results\":[{\"id\": \"record-000000000000000000000000\", \"project\": \"project-123412341234123412341234\"}]}"),
                DXSearch.FindDataObjectsResponse.class);

        // Extra fields in the response should not cause us to choke (for API
        // forward compatibility)
        DXJSON.safeTreeToValue(DXJSON.parseJson("{\"notAField\": true, \"results\":[]}"),
                DXSearch.FindDataObjectsResponse.class);
    }

    @Test
    public void testFindJobsRequestSerialization() throws IOException {
        Assert.assertEquals(DXJSON.parseJson("{\"launchedBy\":\"user-user1\"}"),
                mapper.valueToTree(DXSearch.findJobs().launchedBy("user-user1").buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000000000000000000000000\"}"),
                mapper.valueToTree(DXSearch.findJobs()
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .buildRequestHash()));

        // Conversion of dates to milliseconds since epoch
        GregorianCalendar january15 = new GregorianCalendar(2013, 0, 15);
        january15.setTimeZone(TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(
                DXJSON.parseJson("{\"createdBefore\":1358208000000}"),
                mapper.valueToTree(DXSearch.findJobs().createdBefore(january15.getTime())
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"createdAfter\":1358208000000}"),
                mapper.valueToTree(DXSearch.findJobs().createdAfter(january15.getTime())
                        .buildRequestHash()));

        // Setting multiple fields
        Assert.assertEquals(
                DXJSON.parseJson("{\"launchedBy\":\"user-user1\", \"project\":\"project-000000000000000000000000\"}"),
                mapper.valueToTree(DXSearch.findJobs().launchedBy("user-user1")
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .buildRequestHash()));

        // Setting the same field more than once is disallowed
        try {
            DXSearch.findJobs().launchedBy("user-user1").launchedBy("user-user2");
            Assert.fail("Expected double setting of launchedBy to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findJobs().inProject(DXProject.getInstance("project-0"))
                    .inProject(DXProject.getInstance("project-1"));
            Assert.fail("Expected double setting of inProject to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    @Test
    public void testFindJobsResponseSerialization() throws IOException {
        // Test deserialization of the result without making a real API call
        DXJSON.safeTreeToValue(
                DXJSON.parseJson("{\"results\":[{\"id\": \"job-000000000000000000000000\"}]}"),
                DXSearch.FindJobsResponse.class);

        // Extra fields in the response should not cause us to choke (for API
        // forward compatibility)
        DXJSON.safeTreeToValue(DXJSON.parseJson("{\"notAField\": true, \"results\":[]}"),
                DXSearch.FindJobsResponse.class);
    }

}
