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

import com.dnanexus.TestEnvironment.ConfigOption;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * Tests for creating and running applets.
 */
public class DXAppletTest {

    @JsonInclude(Include.NON_NULL)
    private static class SampleAppInput {
        @SuppressWarnings("unused")
        @JsonProperty("input_string")
        public final String inputString;
        @SuppressWarnings("unused")
        @JsonProperty("input_record")
        public final DXRecord inputRecord;

        public SampleAppInput(String inputString, DXRecord inputRecord) {
            this.inputString = inputString;
            this.inputRecord = inputRecord;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class SampleAppOutput {
        @JsonProperty("output_record")
        public DXRecord outputRecord;

        private SampleAppOutput() {}
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXAppletTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy();
        }
    }

    // External tests

    @Test
    public void testCreateAppletSimple() {
        DXApplet a =
                DXApplet.newApplet().setProject(testProject)
                        .setRunSpecification(RunSpecification.newRunSpec("bash", "false;").build())
                        .build();

        DXApplet.Describe d = a.describe();
        Assert.assertEquals(testProject, d.getProject());
    }

    @Test
    public void testGetInstance() {
        DXApplet applet = DXApplet.getInstance("applet-0000");
        Assert.assertEquals("applet-0000", applet.getId());
        Assert.assertEquals(null, applet.getProject());

        DXApplet applet2 =
                DXApplet.getInstance("applet-0001",
                        DXProject.getInstance("project-123412341234123412341234"));
        Assert.assertEquals("applet-0001", applet2.getId());
        Assert.assertEquals("project-123412341234123412341234", applet2.getProject().getId());

        try {
            DXApplet.getInstance(null);
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXApplet.getInstance("applet-1234", (DXContainer) null);
            Assert.fail("Expected creation without setting project to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXApplet.getInstance(null, DXProject.getInstance("project-123412341234123412341234"));
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testDataObjectMethods() {
        final InputParameter input1 =
                InputParameter.newInputParameter("input_string", IOClass.STRING).build();
        final InputParameter input2 =
                InputParameter.newInputParameter("input_record", IOClass.RECORD).build();

        final OutputParameter output1 =
                OutputParameter.newOutputParameter("output_record", IOClass.RECORD).build();

        DXDataObjectTest.BuilderFactory<DXApplet.Builder, DXApplet> builderFactory =
                new DXDataObjectTest.BuilderFactory<DXApplet.Builder, DXApplet>() {
                    @Override
                    public DXApplet.Builder getBuilder() {
                        return DXApplet
                                .newApplet()
                                .setTitle("mytitle")
                                .setSummary("mysummary")
                                .setDescription("mydescription")
                                .setRunSpecification(
                                        RunSpecification.newRunSpec("bash", "false;").build())
                                .setInputSpecification(ImmutableList.of(input1, input2))
                                .setOutputSpecification(ImmutableList.of(output1));
                    }
                };

        DXDataObjectTest.testClosedDataObjectMethods(testProject, builderFactory, true);

        DXApplet applet = builderFactory.getBuilder().setProject(testProject).build();
        DXApplet.Describe d = applet.describe();

        Assert.assertEquals(DataObjectState.CLOSED, d.getState());
        Assert.assertEquals("bash", d.getRunSpecification().getInterpreter());
        Assert.assertEquals(null, d.getRunSpecification().getCode());
        Assert.assertEquals(testProject, d.getProject());
        Assert.assertEquals(2, d.getInputSpecification().size());
        Assert.assertEquals("input_string", d.getInputSpecification().get(0).getName());
        Assert.assertEquals(IOClass.STRING, d.getInputSpecification().get(0).getIOClass());
        Assert.assertEquals(false, d.getInputSpecification().get(0).isOptional());
        Assert.assertEquals("input_record", d.getInputSpecification().get(1).getName());
        Assert.assertEquals(IOClass.RECORD, d.getInputSpecification().get(1).getIOClass());
        Assert.assertEquals(false, d.getInputSpecification().get(1).isOptional());
        Assert.assertEquals(1, d.getOutputSpecification().size());
        Assert.assertEquals("output_record", d.getOutputSpecification().get(0).getName());
        Assert.assertEquals(IOClass.RECORD, d.getOutputSpecification().get(0).getIOClass());
        Assert.assertEquals(false, d.getOutputSpecification().get(0).isOptional());
        Assert.assertEquals("mytitle", d.getTitle());
        Assert.assertEquals("mysummary", d.getSummary());
        Assert.assertEquals("mydescription", d.getDescription());
    }

    /**
     * Simple end to end test of building and running an app.
     */
    @Test
    public void testRunApplet() {
        if (!TestEnvironment.canRunTest(ConfigOption.RUN_JOBS)) {
            System.err.println("Skipping test that would run jobs");
            return;
        }

        final InputParameter input1 =
                InputParameter.newInputParameter("input_string", IOClass.STRING).build();
        final InputParameter input2 =
                InputParameter.newInputParameter("input_record", IOClass.RECORD).build();
        final OutputParameter output1 =
                OutputParameter.newOutputParameter("output_record", IOClass.RECORD).build();

        // Minimal applet that just passes the record through
        String code =
                "dx-jobutil-add-output output_record `dx-jobutil-parse-link \"$input_record\"` --class=record\n";
        DXApplet applet =
                DXApplet.newApplet().setProject(testProject).setName("simple_test_java_app")
                        .setRunSpecification(RunSpecification.newRunSpec("bash", code).build())
                        .setInputSpecification(ImmutableList.of(input1, input2))
                        .setOutputSpecification(ImmutableList.of(output1)).build();

        // A sample input: {input_string: "java", input_record: {$dnanexus_link: "record-xxxx"}}
        DXRecord record = DXRecord.newRecord().setName("R").setProject(testProject).build();
        record.close();
        SampleAppInput appInput = new SampleAppInput("java", record);

        // Run the applet!
        DXJob job = applet.newRun().withInput(appInput).inProject(testProject).run();
        job.waitUntilDone();

        // Examine and verify the job's output
        SampleAppOutput output = job.getOutput(SampleAppOutput.class);
        // The output object reference will have dropped the project field, so we can't directly
        // compare record to outputRecord
        Assert.assertEquals(DXRecord.getInstance(record.getId()), output.outputRecord);
    }

    @Test
    public void testBuilder() {
        DXDataObjectTest.testBuilder(testProject,
                new DXDataObjectTest.BuilderFactory<DXApplet.Builder, DXApplet>() {
                    @Override
                    public DXApplet.Builder getBuilder() {
                        return DXApplet.newApplet().setRunSpecification(
                                RunSpecification.newRunSpec("bash", "false;").build());
                    }
                });
    }

    // Internal tests

    /**
     * Tests serialization of the input hash to applet/new
     */
    @Test
    public void testCreateAppletSerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000011112222333344445555\", \"dxapi\": \"1.0.0\", \"name\": \"foo\", \"runSpec\": {\"interpreter\": \"bash\", \"code\": \"false;\"}}"),
                MAPPER.valueToTree(DXApplet.newApplet()
                        .setProject(DXProject.getInstance("project-000011112222333344445555"))
                        .setRunSpecification(RunSpecification.newRunSpec("bash", "false;").build())
                        .setName("foo").buildRequestHash()));
    }

    /**
     * Tests serialization of the input hash to applet-xxxx/run.
     */
    @Test
    public void testRunSerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"input\": {\"input_string\": \"foo\", \"input_record\": {\"$dnanexus_link\": \"record-000011112222333344445555\"}}}"),
                ExecutableRunner
                        .getAppletRunnerWithEnvironment("applet-1234", DXEnvironment.create())
                        .withInput(
                                new SampleAppInput("foo", DXRecord
                                        .getInstance("record-000011112222333344445555")))
                        .buildRequestHash());

        Assert.assertEquals(
                DXJSON.parseJson("{\"project\": \"project-000011112222333344445555\", \"folder\": \"/asdf\", \"name\": \"myjob\", \"delayWorkspaceDestruction\": true}"),
                ExecutableRunner
                        .getAppletRunnerWithEnvironment("applet-1234", DXEnvironment.create())
                        .inProject(DXProject.getInstance("project-000011112222333344445555"))
                        .inFolder("/asdf").withName("myjob").delayWorkspaceDestruction()
                        .buildRequestHash());

        Assert.assertEquals(
                // Note: order of objects subject to change
                DXJSON.parseJson("{\"dependsOn\": [\"job-111111111111111111111111\", \"gtable-222222222222222222222222\"]}"),
                ExecutableRunner
                        .getAppletRunnerWithEnvironment("applet-1234", DXEnvironment.create())
                        .dependsOn(DXJob.getInstance("job-111111111111111111111111"))
                        .dependsOn(DXGTable.getInstance("gtable-222222222222222222222222"))
                        .buildRequestHash());
    }

}
