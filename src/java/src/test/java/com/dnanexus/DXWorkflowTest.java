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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;

/**
 * Tests for creating workflows.
 */
public class DXWorkflowTest {

    @JsonInclude(Include.NON_NULL)
    private static class WorkflowAddStageInput {
        @JsonProperty
        public int editVersion;

        @JsonProperty
        public String executable;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class WorkflowAddStageOutput {
        @JsonProperty
        public int editVersion;

        @JsonProperty
        public String stage;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class WorkflowRunOutput {
        @JsonProperty
        public String id;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DXProject testProject;

    private ObjectNode makeDXLink(DXDataObject dataObject) {
        return DXJSON.getObjectBuilder().put("$dnanexus_link", dataObject.getId()).build();
    }

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXWorkflowTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy();
        }
    }

    // External tests

    @Test
    public void testCreateWorkflowSimple() {
        DXWorkflow a = DXWorkflow.newWorkflow().setProject(testProject).build();

        DXWorkflow.Describe d = a.describe();
        Assert.assertEquals(testProject, d.getProject());
    }

    @Test
    public void testDescribeWithOptions() {
        DXWorkflow a = DXWorkflow.newWorkflow().setProject(testProject).build();

        DXWorkflow.Describe d = a.describe(DescribeOptions.get());
        Assert.assertEquals(testProject, d.getProject());
    }

    @Test
    public void testGetInstance() {
        DXWorkflow workflow = DXWorkflow.getInstance("workflow-000011112222333344445555");
        Assert.assertEquals("workflow-000011112222333344445555", workflow.getId());
        Assert.assertEquals(null, workflow.getProject());

        DXWorkflow workflow2 =
                DXWorkflow.getInstance("workflow-000100020003000400050006",
                        DXProject.getInstance("project-123412341234123412341234"));
        Assert.assertEquals("workflow-000100020003000400050006", workflow2.getId());
        Assert.assertEquals("project-123412341234123412341234", workflow2.getProject().getId());

        try {
            DXWorkflow.getInstance(null);
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXWorkflow.getInstance("workflow-123412341234123412341234", (DXContainer) null);
            Assert.fail("Expected creation without setting project to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXWorkflow.getInstance(null, DXProject.getInstance("project-123412341234123412341234"));
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testDataObjectMethods() {
        DXDataObjectTest.BuilderFactory<DXWorkflow.Builder, DXWorkflow> builderFactory =
                new DXDataObjectTest.BuilderFactory<DXWorkflow.Builder, DXWorkflow>() {
                    @Override
                    public DXWorkflow.Builder getBuilder() {
                        return DXWorkflow.newWorkflow();
                    }
                };

        DXDataObjectTest.testOpenDataObjectMethods(testProject, builderFactory);
        DXDataObjectTest.testClosedDataObjectMethods(testProject, builderFactory);
    }

    @Test
    public void testBuilder() {
        DXDataObjectTest.testBuilder(testProject,
                new DXDataObjectTest.BuilderFactory<DXWorkflow.Builder, DXWorkflow>() {
                    @Override
                    public DXWorkflow.Builder getBuilder() {
                        return DXWorkflow.newWorkflow();
                    }
                });
    }

    @Test
    public void testRunWorkflow() {
        DXWorkflow workflow = DXWorkflow.newWorkflow().setProject(testProject).build();

        // Create an applet to be added to the workflow and some inputs to be supplied to it
        InputParameter inputString = InputParameter.newInputParameter("input_string",
                IOClass.STRING).build();
        InputParameter inputRecord = InputParameter.newInputParameter("input_record",
                IOClass.RECORD).build();
        OutputParameter outputRecord = OutputParameter.newOutputParameter("output_record",
                IOClass.RECORD).build();

        DXRecord myRecord = DXRecord.newRecord().setProject(testProject).setName("myRecord")
                .build().close();

        String code = "dx-jobutil-add-output output_record `dx-jobutil-parse-link \"$input_record\"` --class=record\n";

        DXApplet applet = DXApplet.newApplet().setProject(testProject)
                .setName("applet_for_java_test")
                .setRunSpecification(RunSpecification.newRunSpec("bash", code).build())
                .setInputSpecification(ImmutableList.of(inputString, inputRecord))
                .setOutputSpecification(ImmutableList.of(outputRecord)).build();

        // ---
        // Here is some low-level code for building up the workflow. Eventually there will be
        // high-level bindings to this functionality.
        WorkflowAddStageInput addStage0Input = new WorkflowAddStageInput();
        addStage0Input.editVersion = 0;
        addStage0Input.executable = applet.getId();

        WorkflowAddStageOutput addStage0Output = DXAPI.workflowAddStage(workflow.getId(),
                addStage0Input, WorkflowAddStageOutput.class);

        WorkflowAddStageInput addStage1Input = new WorkflowAddStageInput();
        addStage1Input.editVersion = addStage0Output.editVersion;
        addStage1Input.executable = applet.getId();

        WorkflowAddStageOutput addStage1Output = DXAPI.workflowAddStage(workflow.getId(),
                addStage1Input, WorkflowAddStageOutput.class);
        // End low-level code
        // ---

        // Supply workflow inputs in the format STAGE.INPUTNAME
        ObjectNode runInput = DXJSON.getObjectBuilder()
                .put(addStage0Output.stage + ".input_string", "foo")
                .put(addStage0Output.stage + ".input_record", makeDXLink(myRecord))
                .put(addStage1Output.stage + ".input_string", "bar")
                .put(addStage1Output.stage + ".input_record", makeDXLink(myRecord)).build();

        // We run a workflow here, but do not wait for its result, so it's fine that this test
        // doesn't check for ConfigOption.RUN_JOBS.
        DXAnalysis analysis = workflow.newRun().setInput(runInput).setProject(testProject).run();
        analysis.terminate();
    }

    // Internal tests

    /**
     * Tests serialization of the input hash to /workflow/new
     */
    @Test
    public void testCreateWorkflowSerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000011112222333344445555\", \"name\": \"foo\"}"),
                MAPPER.valueToTree(DXWorkflow.newWorkflow()
                        .setProject(DXProject.getInstance("project-000011112222333344445555"))
                        .setName("foo").buildRequestHash()));
    }

}
