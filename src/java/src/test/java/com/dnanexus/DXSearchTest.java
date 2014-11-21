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
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.DXSearch.PropertiesQuery;
import com.dnanexus.DXSearch.TypeQuery;
import com.dnanexus.DXSearch.VisibilityQuery;
import com.dnanexus.TestEnvironment.ConfigOption;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests DXSearch methods.
 */
public class DXSearchTest {

    @JsonInclude(Include.NON_NULL)
    private static class SampleAppInput {
        @JsonProperty("input_string")
        public final String inputString;

        public SampleAppInput(String inputString) {
            this.inputString = inputString;
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper();

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

    private DXProject testProject;

    /**
     * Creates and returns a minimal applet that takes a single string:input_string.
     *
     * @return created applet
     */
    private DXApplet createMinimalApplet() {
        final InputParameter input1 =
                InputParameter.newInputParameter("input_string", IOClass.STRING).build();

        // Minimal applet that outputs nothing
        String code = "\n";
        DXApplet applet =
                DXApplet.newApplet().setProject(testProject).setName("simple_test_java_app")
                        .setRunSpecification(RunSpecification.newRunSpec("bash", code).build())
                        .setInputSpecification(ImmutableList.of(input1))
                        .setOutputSpecification(ImmutableList.<OutputParameter>of()).build();
        return applet;
    }

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXSearchTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy(true);
        }
    }

    /**
     * Tests a variety of findDataObjects features.
     */
    @Test
    public void testFindDataObjects() {
        DXRecord moo = DXRecord.newRecord().setProject(testProject).setName("Moo")
                .putProperty("sampleId", "1").putProperty("process", "a")
                .addTypes(ImmutableList.of("genome", "report")).build().close();
        DXRecord foo = DXRecord.newRecord().setProject(testProject).setName("foo")
                .putProperty("sampleId", "2").addTags(ImmutableList.of("mytag"))
                .addTypes(ImmutableList.of("genome")).build().close();
        DXRecord food = DXRecord.newRecord().setProject(testProject).setName("food")
                .putProperty("process", "a").setFolder("/subfolder", true)
                .addTypes(ImmutableList.of("report")).build().close();
        DXRecord open = DXRecord.newRecord().setProject(testProject).setName("open")
                .addTypes(ImmutableList.of("type")).build();
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

        // We rely on the fact that
        // moo.created <= foo.created <= food.created <= open.created
        // with equality possible since the creation timestamp is encoded at
        // 1-sec resolution

        List<DXDataObject> createdBeforeResults =
                DXSearch.findDataObjects().inProject(testProject)
                        .createdBefore(moo.describe().getCreationDate()).execute().asList();
        Assert.assertTrue(1 <= createdBeforeResults.size() && createdBeforeResults.size() <= 4);
        Assert.assertTrue(createdBeforeResults.contains(moo));

        List<DXDataObject> createdAfterResults =
                DXSearch.findDataObjects().inProject(testProject)
                        .createdAfter(food.describe().getCreationDate()).execute().asList();
        Assert.assertTrue(2 <= createdAfterResults.size() && createdAfterResults.size() <= 4);
        Assert.assertTrue(createdAfterResults.contains(food));
        Assert.assertTrue(createdAfterResults.contains(open));

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
        assertEqualsAnyOrder(
                DXSearch.findDataObjects()
                        .inProject(testProject)
                        .withProperties(
                                PropertiesQuery.allOf(PropertiesQuery.withKey("sampleId"),
                                        PropertiesQuery.withKeyAndValue("process", "a"))).execute()
                        .asList(), moo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects()
                        .inProject(testProject)
                        .withProperties(
                                PropertiesQuery.anyOf(PropertiesQuery.withKey("sampleId"),
                                        PropertiesQuery.withKeyAndValue("process", "a"))).execute()
                        .asList(), moo, foo, food);

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
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .withTags(DXSearch.TagsQuery.anyOf("mytag", "zyzzx")).execute().asList(),
                foo);

        // withTypes

        assertEqualsAnyOrder(DXSearch.findDataObjects().inProject(testProject).withType("genome")
                .execute().asList(), moo, foo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .withTypes(TypeQuery.allOf("genome", "report")).execute().asList(), moo);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects().inProject(testProject)
                        .withTypes(TypeQuery.anyOf("genome", "report")).execute().asList(), moo,
                foo, food);
        assertEqualsAnyOrder(
                DXSearch.findDataObjects()
                        .inProject(testProject)
                        .withTypes(
                                TypeQuery.anyOf(TypeQuery.allOf("genome", "report"),
                                        TypeQuery.of("type"))).execute().asList(), moo, open);

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

        // withIdsIn

        assertEqualsAnyOrder(DXSearch.findDataObjects().withIdsIn(ImmutableList.of(moo, foo))
                .execute().asList(), moo, foo);
        // Hidden objects don't get returned unless specifically requested
        assertEqualsAnyOrder(DXSearch.findDataObjects().withIdsIn(ImmutableList.of(moo, invisible))
                .execute().asList(), moo);
        assertEqualsAnyOrder(DXSearch.findDataObjects().withIdsIn(ImmutableList.of(moo, invisible))
                .withVisibility(VisibilityQuery.EITHER).execute().asList(), moo, invisible);

        // TODO: withLinkTo, withMinimumAccessLevel

    }

    /**
     * Tests filtering by class.
     */
    @Test
    public void testFindDataObjectsByClass() {
        DXRecord record = DXRecord.newRecord().setProject(testProject).setName("arecord").build();
        DXFile file = DXFile.newFile().setProject(testProject).setName("afile").build();
        DXGTable gtable =
                DXGTable.newGTable(
                        ImmutableList.of(ColumnSpecification.getInstance("num_goats",
                                ColumnType.INT16))).setProject(testProject).setName("agtable")
                        .build();
        DXApplet applet =
                DXApplet.newApplet().setProject(testProject).setName("anapplet")
                        .setRunSpecification(RunSpecification.newRunSpec("bash", "").build())
                        .build();
        DXWorkflow workflow =
                DXWorkflow.newWorkflow().setProject(testProject).setName("aworkflow").build();

        DXRecord recordResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().inProject(testProject)
                        .withClassRecord().execute().asList());
        Assert.assertEquals(record, recordResult);
        Assert.assertEquals("arecord", recordResult.describe().getName());
        DXFile fileResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().inProject(testProject)
                        .withClassFile().execute().asList());
        Assert.assertEquals(file, fileResult);
        Assert.assertEquals("afile", fileResult.describe().getName());
        DXGTable gtableResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().inProject(testProject)
                        .withClassGTable().execute().asList());
        Assert.assertEquals(gtable, gtableResult);
        Assert.assertEquals("agtable", gtableResult.describe().getName());
        DXApplet appletResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().inProject(testProject)
                        .withClassApplet().execute().asList());
        Assert.assertEquals(applet, appletResult);
        Assert.assertEquals("anapplet", appletResult.describe().getName());
        DXWorkflow workflowResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().inProject(testProject)
                        .withClassWorkflow().execute().asList());
        Assert.assertEquals(workflow, workflowResult);
        Assert.assertEquals("aworkflow", workflowResult.describe().getName());
    }

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
        Assert.assertEquals(
                DXJSON.parseJson("{\"describe\": true}"),
                mapper.valueToTree(DXSearch.findDataObjects().includeDescribeOutput()
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"describe\": {\"properties\": true}}"),
                mapper.valueToTree(DXSearch.findDataObjects()
                        .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
                        .buildRequestHash()));

        Assert.assertEquals(DXJSON.parseJson("{\"tags\": \"a\"}"),
                mapper.valueToTree(DXSearch.findDataObjects().withTag("a").buildRequestHash()));
        Assert.assertEquals(DXJSON
                .parseJson("{\"tags\": {\"$or\": [{\"$and\": [\"a\", \"b\"]}, \"c\"]}}"), mapper
                .valueToTree(DXSearch
                        .findDataObjects()
                        .withTags(
                                DXSearch.TagsQuery.anyOf(DXSearch.TagsQuery.allOf("a", "b"),
                                        DXSearch.TagsQuery.of("c"))).buildRequestHash()));

        Assert.assertEquals(DXJSON.parseJson("{\"type\": \"a\"}"),
                mapper.valueToTree(DXSearch.findDataObjects().withType("a").buildRequestHash()));
        Assert.assertEquals(DXJSON
                .parseJson("{\"type\": {\"$or\": [{\"$and\": [\"a\", \"b\"]}, \"c\"]}}"), mapper
                .valueToTree(DXSearch
                        .findDataObjects()
                        .withTypes(
                                DXSearch.TypeQuery.anyOf(DXSearch.TypeQuery.allOf("a", "b"),
                                        DXSearch.TypeQuery.of("c"))).buildRequestHash()));

        Assert.assertEquals(
                DXJSON.parseJson("{\"properties\": {\"foo\": true}}"),
                mapper.valueToTree(DXSearch.findDataObjects().withProperty("foo")
                        .buildRequestHash()));
        Assert.assertEquals(DXJSON
                .parseJson("{\"properties\": {\"$and\": [{\"foo\": true}, {\"bar\": \"a\"}]}}"),
                mapper.valueToTree(DXSearch.findDataObjects().withProperty("foo")
                        .withProperty("bar", "a").buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"properties\": {\"$and\": [{\"foo\": true}, {\"bar\": \"a\"}, {\"baz\": \"b\"}]}}"),
                mapper.valueToTree(DXSearch.findDataObjects().withProperty("foo")
                        .withProperty("bar", "a")
                        .withProperties(PropertiesQuery.withKeyAndValue("baz", "b"))
                        .buildRequestHash()));

        Assert.assertEquals(DXJSON.parseJson("{\"id\": [\"record-111100000000000000000000\"]}"),
                mapper.valueToTree(DXSearch
                        .findDataObjects()
                        .withIdsIn(
                                ImmutableList.of(DXRecord
                                        .getInstance("record-111100000000000000000000")))
                        .buildRequestHash()));

        try {
            DXSearch.findDataObjects()
                    .inProject(DXProject.getInstance("project-000000000000000000000000"))
                    .inProject(DXProject.getInstance("project-111100000000000000000000"));
            Assert.fail("Expected double setting of inProject to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findDataObjects()
                    .inFolder(DXProject.getInstance("project-000000000000000000000000"), "/1")
                    .inFolder(DXProject.getInstance("project-000000000000000000000000"), "/2");
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

        try {
            DXSearch.findDataObjects().withClassApplet().withClassFile();
            Assert.fail("Expected setting multiple class constraints to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findDataObjects().withIdsIn(ImmutableList.<DXDataObject>of())
                    .withIdsIn(ImmutableList.<DXDataObject>of());
            Assert.fail("Expected double setting of withIdsIn to fail");
        } catch (IllegalStateException e) {
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
        DXJSON.safeTreeToValue(
                DXJSON.parseJson("{\"notAField\": true, \"results\":[{\"id\": \"record-000000000000000000000000\", \"notAField\": true}]}"),
                DXSearch.FindDataObjectsResponse.class);
    }

    /**
     * findDataObjects smoke test.
     */
    @Test
    public void testFindDataObjectsSimple() {
        // List access
        Assert.assertTrue(DXSearch.findDataObjects().inProject(testProject)
                .nameMatchesExactly("foobarbaz").execute().asList().isEmpty());
        // Iterable access
        assertEqualsAnyOrder(DXSearch.findDataObjects().inProject(testProject)
                .nameMatchesExactly("foobarbaz").execute());
    }

    /**
     * Tests retrieving Describe output with findDataObjects.
     */
    @Test
    public void testFindDataObjectsWithDescribe() {
        DXRecord.newRecord().setProject(testProject).setName("record1")
                .putProperty("sampleId", "1234").build();
        DXFile.newFile().setProject(testProject).setName("file1").putProperty("sampleId", "2345")
                .build();
        DXGTable.newGTable(
                ImmutableList.of(ColumnSpecification.getInstance("num_goats", ColumnType.INT16)))
                .setProject(testProject).setName("gtable1").build();
        DXApplet.newApplet().setProject(testProject).setName("applet1")
                .setRunSpecification(RunSpecification.newRunSpec("bash", "").build()).build();
        DXWorkflow.newWorkflow().setProject(testProject).setName("workflow1").build();

        DXRecord recordResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().withClassRecord()
                        .inProject(testProject).nameMatchesExactly("record1")
                        .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
                        .execute().asList());
        Assert.assertEquals(recordResult.getCachedDescribe().getName(), "record1");
        // Called includeDescribeOutput with properties: true so properties should be returned
        Assert.assertEquals(recordResult.getCachedDescribe().getProperties().get("sampleId"),
                "1234");

        DXGTable gtableResult =
                Iterables.getOnlyElement(DXSearch.findDataObjects().withClassGTable()
                        .inProject(testProject).nameMatchesExactly("gtable1")
                        .includeDescribeOutput().execute().asList());
        Assert.assertEquals(gtableResult.getCachedDescribe().getName(), "gtable1");
        // Called includeDescribeOutput with default settings so properties should NOT be returned
        try {
            gtableResult.getCachedDescribe().getProperties();
            Assert.fail("Expected IllegalStateException to be thrown because properties should not have been returned");
        } catch (IllegalStateException e) {
            // Expected
        }

        Assert.assertEquals(
                Iterables
                        .getOnlyElement(
                                DXSearch.findDataObjects().inProject(testProject)
                                        .nameMatchesExactly("file1").includeDescribeOutput()
                                        .execute().asList()).getCachedDescribe().getName(), "file1");
        Assert.assertEquals(
                Iterables
                        .getOnlyElement(
                                DXSearch.findDataObjects().inProject(testProject)
                                        .nameMatchesExactly("applet1").includeDescribeOutput()
                                        .execute().asList()).getCachedDescribe().getName(),
                "applet1");
        Assert.assertEquals(
                Iterables
                        .getOnlyElement(
                                DXSearch.findDataObjects().inProject(testProject)
                                        .nameMatchesExactly("workflow1").includeDescribeOutput()
                                        .execute().asList()).getCachedDescribe().getName(),
                "workflow1");

        DXRecord findWithoutDescribe =
                Iterables.getOnlyElement(DXSearch.findDataObjects().inProject(testProject)
                        .nameMatchesExactly("record1").withClassRecord().execute().asList());
        try {
            findWithoutDescribe.getCachedDescribe();
            Assert.fail("Expected IllegalStateException to be thrown");
        } catch (IllegalStateException e) {
            // Expected
        }
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

        List<DXRecord> outputRecordStreamWithPaging =
                ImmutableList.copyOf(DXSearch.findDataObjects().inProject(testProject)
                        .nameMatchesGlob("foo*").withClassRecord().execute(3));
        Assert.assertEquals(outputRecords, outputRecordStreamWithPaging);
    }

    /**
     * Tests a variety of findExecutions features.
     */
    @Test
    public void testFindExecutions() {
        if (!TestEnvironment.canRunTest(ConfigOption.RUN_JOBS)) {
            System.err.println("Skipping test that would run jobs");
            return;
        }

        DXApplet applet = createMinimalApplet();

        // A sample input: {input_string: "java"}
        SampleAppInput appInput = new SampleAppInput("java");

        // Run the applet!
        DXJob job =
                applet.newRun().setInput(appInput).setProject(testProject).setName("javatest")
                        .addTags(ImmutableList.of("t1")).putProperty("k1", "v1").run();

        // Some findJobs queries
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject)
                .withExecutable(applet).execute().asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject).withTag("t1")
                .execute().asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject).withTag("t2")
                .execute().asList());
        assertEqualsAnyOrder(
                DXSearch.findExecutions().inProject(testProject).withProperty("k1", "v1").execute()
                        .asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject).withProperty("k1")
                .execute().asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject)
                .withProperty("k1", "v2").execute().asList());
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject).withProperty("k2")
                .execute().asList());
        assertEqualsAnyOrder(
                DXSearch.findExecutions()
                        .inProject(testProject)
                        .withProperties(
                                PropertiesQuery.anyOf(PropertiesQuery.withKey("k1"),
                                        PropertiesQuery.withKeyAndValue("does", "not exist")))
                        .execute().asList(), job);
        assertEqualsAnyOrder(DXSearch
                .findExecutions()
                .inProject(testProject)
                .withProperties(
                        PropertiesQuery.allOf(PropertiesQuery.withKey("k1"),
                                PropertiesQuery.withKeyAndValue("does", "not exist"))).execute()
                .asList());

        assertEqualsAnyOrder(
                DXSearch.findExecutions().inProject(testProject).nameMatchesExactly("javatest")
                        .execute().asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject)
                .nameMatchesExactly("java").execute().asList());
        assertEqualsAnyOrder(
                DXSearch.findExecutions().inProject(testProject).nameMatchesGlob("*test").execute()
                        .asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject)
                .nameMatchesGlob("python*").execute().asList());
        assertEqualsAnyOrder(DXSearch.findExecutions().inProject(testProject).withOriginJob(job)
                .execute().asList(), job);

        // With describe calls
        DXJob resultJobWithDescribe =
                Iterables.getOnlyElement(DXSearch.findExecutions().inProject(testProject)
                        .withTag("t1").withClassJob().includeDescribeOutput().execute().asList());
        Assert.assertEquals("javatest", resultJobWithDescribe.getCachedDescribe().getName());
        DXJob resultJobWithoutDescribe =
                Iterables.getOnlyElement(DXSearch.findExecutions().inProject(testProject)
                        .withTag("t1").withClassJob().execute().asList());
        try {
            resultJobWithoutDescribe.getCachedDescribe();
            Assert.fail("Expected IllegalStateException to be thrown because includeDescribeOutput was not specified");
        } catch (IllegalStateException e) {
            // Expected
        }

        assertEqualsAnyOrder(DXSearch.findExecutions().withIdsIn(ImmutableList.of(job)).execute()
                .asList(), job);
        assertEqualsAnyOrder(DXSearch.findExecutions().withIdsIn(ImmutableList.<DXExecution>of())
                .execute().asList());
    }

    /**
     * Tests formulating findExecutions queries without actually issuing them.
     */
    @Test
    public void testFindExecutionsQuerySerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"launchedBy\":\"user-user1\"}"),
                mapper.valueToTree(DXSearch.findExecutions().launchedBy("user-user1")
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000000000000000000000000\"}"),
                mapper.valueToTree(DXSearch.findExecutions()
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .buildRequestHash()));

        Assert.assertEquals(
                DXJSON.parseJson("{\"includeSubjobs\": false}"),
                mapper.valueToTree(DXSearch.findExecutions().includeSubjobs(false)
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": \"dnanexus\"}"),
                mapper.valueToTree(DXSearch.findExecutions().nameMatchesExactly("dnanexus")
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"regexp\": \"(DNA|dna)nexus\"}}"),
                mapper.valueToTree(DXSearch.findExecutions().nameMatchesRegexp("(DNA|dna)nexus")
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"regexp\": \"[dr]nanexus\"}}"),
                mapper.valueToTree(DXSearch.findExecutions()
                        .nameMatchesRegexp("[dr]nanexus", false).buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"name\": {\"regexp\": \"[dr]nanexus\", \"flags\": \"i\"}}"),
                mapper.valueToTree(DXSearch.findExecutions().nameMatchesRegexp("[dr]nanexus", true)
                        .buildRequestHash()));

        Assert.assertEquals(DXJSON.parseJson("{\"class\": \"job\"}"),
                mapper.valueToTree(DXSearch.findExecutions().withClassJob().buildRequestHash()));
        Assert.assertEquals(DXJSON.parseJson("{\"class\": \"analysis\"}"), mapper
                .valueToTree(DXSearch.findExecutions().withClassAnalysis().buildRequestHash()));

        Assert.assertEquals(
                DXJSON.parseJson("{\"executable\": \"applet-000011112222333344445555\"}"),
                mapper.valueToTree(DXSearch.findExecutions()
                        .withExecutable(DXApplet.getInstance("applet-000011112222333344445555"))
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"originJob\": \"job-000011112222333344445555\"}"),
                mapper.valueToTree(DXSearch.findExecutions()
                        .withOriginJob(DXJob.getInstance("job-000011112222333344445555"))
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"parentAnalysis\": \"analysis-000011112222333344445555\"}"),
                mapper.valueToTree(DXSearch
                        .findExecutions()
                        .withParentAnalysis(
                                DXAnalysis.getInstance("analysis-000011112222333344445555"))
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"parentJob\": \"job-000011112222333344445555\"}"),
                mapper.valueToTree(DXSearch.findExecutions()
                        .withParentJob(DXJob.getInstance("job-000011112222333344445555"))
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"properties\": {\"$and\": [{\"a\": \"b\"}, {\"c\": true}]}}"),
                mapper.valueToTree(DXSearch.findExecutions().withProperty("a", "b")
                        .withProperty("c").buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"rootExecution\": \"analysis-000011112222333344445555\"}"),
                mapper.valueToTree(DXSearch
                        .findExecutions()
                        .withRootExecution(
                                DXAnalysis.getInstance("analysis-000011112222333344445555"))
                        .buildRequestHash()));

        Assert.assertEquals(DXJSON.parseJson("{\"tags\": \"a\"}"),
                mapper.valueToTree(DXSearch.findExecutions().withTag("a").buildRequestHash()));
        Assert.assertEquals(DXJSON
                .parseJson("{\"tags\": {\"$or\": [{\"$and\": [\"a\", \"b\"]}, \"c\"]}}"), mapper
                .valueToTree(DXSearch
                        .findExecutions()
                        .withTags(
                                DXSearch.TagsQuery.anyOf(DXSearch.TagsQuery.allOf("a", "b"),
                                        DXSearch.TagsQuery.of("c"))).buildRequestHash()));

        Assert.assertEquals(
                DXJSON.parseJson("{\"state\": \"done\"}"),
                mapper.valueToTree(DXSearch.findExecutions().withState(JobState.DONE)
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"state\": [\"in_progress\", \"done\"]}"),
                mapper.valueToTree(DXSearch.findExecutions()
                        .withState(AnalysisState.IN_PROGRESS, AnalysisState.DONE)
                        .buildRequestHash()));

        Assert.assertEquals(DXJSON.parseJson("{\"properties\": {\"foo\": true}}"), mapper
                .valueToTree(DXSearch.findExecutions().withProperty("foo").buildRequestHash()));
        Assert.assertEquals(DXJSON
                .parseJson("{\"properties\": {\"$and\": [{\"foo\": true}, {\"bar\": \"a\"}]}}"),
                mapper.valueToTree(DXSearch.findExecutions().withProperty("foo")
                        .withProperty("bar", "a").buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"properties\": {\"$and\": [{\"foo\": true}, {\"bar\": \"a\"}, {\"baz\": \"b\"}]}}"),
                mapper.valueToTree(DXSearch.findExecutions().withProperty("foo")
                        .withProperty("bar", "a")
                        .withProperties(PropertiesQuery.withKeyAndValue("baz", "b"))
                        .buildRequestHash()));

        Assert.assertEquals(
                DXJSON.parseJson("{\"id\": [\"job-111100000000000000000000\"]}"),
                mapper.valueToTree(DXSearch
                        .findExecutions()
                        .withIdsIn(
                                ImmutableList.of(DXJob.getInstance("job-111100000000000000000000")))
                        .buildRequestHash()));

        // Conversion of dates to milliseconds since epoch
        GregorianCalendar january15 = new GregorianCalendar(2013, 0, 15);
        january15.setTimeZone(TimeZone.getTimeZone("UTC"));
        GregorianCalendar january16 = new GregorianCalendar(2013, 0, 16);
        january16.setTimeZone(TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(
                DXJSON.parseJson("{\"created\": {\"before\": 1358208000000}}"),
                mapper.valueToTree(DXSearch.findExecutions().createdBefore(january15.getTime())
                        .buildRequestHash()));
        Assert.assertEquals(
                DXJSON.parseJson("{\"created\": {\"after\": 1358208000000}}"),
                mapper.valueToTree(DXSearch.findExecutions().createdAfter(january15.getTime())
                        .buildRequestHash()));
        Assert.assertEquals(DXJSON
                .parseJson("{\"created\": {\"after\": 1358208000000, \"before\": 1358294400000}}"),
                mapper.valueToTree(DXSearch.findExecutions().createdAfter(january15.getTime())
                        .createdBefore(january16.getTime()).buildRequestHash()));

        // Setting multiple fields
        Assert.assertEquals(
                DXJSON.parseJson("{\"launchedBy\":\"user-user1\", \"project\":\"project-000000000000000000000000\"}"),
                mapper.valueToTree(DXSearch.findExecutions().launchedBy("user-user1")
                        .inProject(DXProject.getInstance("project-000000000000000000000000"))
                        .buildRequestHash()));

        // Setting the same field more than once is disallowed
        try {
            DXSearch.findExecutions().launchedBy("user-user1").launchedBy("user-user2");
            Assert.fail("Expected double setting of launchedBy to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findExecutions().inProject(DXProject.getInstance("project-0"))
                    .inProject(DXProject.getInstance("project-1"));
            Assert.fail("Expected double setting of inProject to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findExecutions().includeSubjobs(true).includeSubjobs(false);
            Assert.fail("Expected double setting of includeSubjobs to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findExecutions().nameMatchesExactly("foo").nameMatchesGlob("g*");
            Assert.fail("Expected double setting of name queries to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findExecutions().withClassAnalysis().withClassJob();
            Assert.fail("Expected double setting of class constraints to fail");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            DXSearch.findExecutions().withIdsIn(ImmutableList.<DXExecution>of())
                    .withIdsIn(ImmutableList.<DXExecution>of());
            Assert.fail("Expected double setting of withIdsIn to fail");
        } catch (IllegalStateException e) {
            // Expected
        }

        // TODO: includeDescribeOutput
    }

    /**
     * Tests deserialization of findExecutions results without making real API calls.
     */
    @Test
    public void testFindExecutionsResponseSerialization() throws IOException {
        DXJSON.safeTreeToValue(
                DXJSON.parseJson("{\"results\":[{\"id\": \"job-000000000000000000000000\"}]}"),
                DXSearch.FindExecutionsResponse.class);

        // Extra fields in the response should not cause us to choke (for API
        // forward compatibility)
        DXJSON.safeTreeToValue(
                DXJSON.parseJson("{\"notAField\": true, \"results\":[{\"id\": \"job-000000000000000000000000\", \"notAField\": true}]}"),
                DXSearch.FindExecutionsResponse.class);
    }

    /**
     * Tests paging through results.
     */
    @Test
    public void testFindExecutionsWithPaging() {
        if (!TestEnvironment.canRunTest(ConfigOption.RUN_JOBS)) {
            System.err.println("Skipping test that would run jobs");
            return;
        }

        DXApplet applet = createMinimalApplet();

        // A sample input: {input_string: "java"}
        SampleAppInput appInput = new SampleAppInput("java");

        // Instantiate to create a bunch of jobs, and save them
        List<DXJob> jobs = Lists.newArrayList();
        for (int i = 0; i < 8; ++i) {
            jobs.add(applet.newRun().setInput(appInput).setProject(testProject)
                    .setName("javaFindExecutionsPagingTest").run());
        }

        // Set a small page size
        assertEqualsAnyOrder(
                DXSearch.findExecutions().inProject(testProject)
                        .nameMatchesExactly("javaFindExecutionsPagingTest").execute(3),
                jobs.toArray(new DXJob[0]));
        // Page size is a multiple of the number of results
        assertEqualsAnyOrder(
                DXSearch.findExecutions().inProject(testProject)
                        .nameMatchesExactly("javaFindExecutionsPagingTest").execute(4),
                jobs.toArray(new DXJob[0]));
    }
}
