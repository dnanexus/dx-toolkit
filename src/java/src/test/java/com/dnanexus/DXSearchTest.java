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
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class DXSearchTest {

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
    public void testFindDataObjects() {
        Assert.assertEquals(0, DXSearch.findDataObjects().nameMatchesExactly("foobarbaz").execute()
                .asList().size());

        // Test paging through results
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
                        .ofClassRecord().execute().asList();
        Assert.assertEquals(8, outputRecords.size());

        List<DXRecord> outputRecordsWithPaging =
                DXSearch.findDataObjects().inProject(testProject).nameMatchesGlob("foo*")
                        .ofClassRecord().execute(3).asList();
        Assert.assertEquals(outputRecords, outputRecordsWithPaging);
        Set<String> outputRecordIds = Sets.newHashSet();
        for (DXRecord record : outputRecordsWithPaging) {
            outputRecordIds.add(record.getId());
        }

        Assert.assertEquals(recordIds, outputRecordIds);
    }

    @Test
    public void testFindJobs() {
        @SuppressWarnings("unused")
        List<DXJob> results = DXSearch.findJobs().launchedBy("user-dnanexus").execute().asList();
    }

    // Internal tests

    @Test
    public void testFindDataObjectsQuerySerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"scope\": {\"project\":\"project-000000000000000000000000\"}}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"scope\": {\"project\":\"project-000000000000000000000000\", \"folder\": \"/my/subfolder\"}}"),
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

        try {
            DXSearch.findDataObjects().inProject(DXProject.getInstance("project-0000"))
                    .inProject(DXProject.getInstance("project-1111")).buildRequestHash();
            Assert.fail("Expected double setting of inProject to fail");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            DXSearch.findDataObjects().inFolder(DXProject.getInstance("project-0000"), "/1")
                    .inFolder(DXProject.getInstance("project-0000"), "/2").buildRequestHash();
            Assert.fail("Expected double setting of inFolder to fail");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            DXSearch.findDataObjects().nameMatchesExactly("ab").nameMatchesGlob("*b")
                    .buildRequestHash();
            Assert.fail("Expected double setting of name parameters to fail");
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
            DXSearch.findJobs().launchedBy("user-user1").launchedBy("user-user2")
                    .buildRequestHash();
            Assert.fail("Expected double setting of launchedBy to fail");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        try {
            DXSearch.findJobs().inProject(DXProject.getInstance("project-0"))
                    .inProject(DXProject.getInstance("project-1")).buildRequestHash();
            Assert.fail("Expected double setting of inProject to fail");
        } catch (IllegalArgumentException e) {
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
