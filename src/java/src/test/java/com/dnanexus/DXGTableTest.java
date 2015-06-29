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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class DXGTableTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = DXProject.newProject().setName("DXGTableTest").build();
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            // We may be closing GTables in this project, so forcibly terminate
            // jobs when we're done with the project.
            testProject.destroy(true);
        }
    }

    @Test
    public void testCreateGTableSerialization() throws IOException {
        Assert.assertEquals(
                DXJSON.parseJson("{\"project\":\"project-000011112222333344445555\", \"name\": \"foo\", \"columns\": [{\"name\": \"name\", \"type\": \"string\"}]}"),
                mapper.valueToTree(DXGTable
                        .newGTable(
                                ImmutableList.of(ColumnSpecification.getInstance("name",
                                        ColumnType.STRING)))
                        .setProject(DXProject.getInstance("project-000011112222333344445555"))
                        .setName("foo").buildRequestHash()));
    }

    @Test
    public void testCreateGTableSimple() {
        ColumnSpecification column1 = ColumnSpecification.getInstance("name", ColumnType.STRING);
        DXGTable g = DXGTable.newGTable(ImmutableList.of(column1)).setProject(testProject).build();

        DXGTable.Describe describe = g.describe();

        Assert.assertEquals(ImmutableList.of(column1), describe.getColumns());


    }

    @Test
    public void testCustomFields() {
        ColumnSpecification column1 = ColumnSpecification.getInstance("name", ColumnType.STRING);
        DXGTable g = DXGTable.newGTable(ImmutableList.of(column1)).setName("mygtable")
                .setProject(testProject).build();
        g.closeAndWait();

        // Retrieve some fields and verify that the ones we want are there and the ones we don't
        // want are not there
        DXGTable.Describe describe = g.describe(DescribeOptions.get().withCustomFields(
                ImmutableList.of("columns", "size")));

        Assert.assertEquals(ImmutableList.of(column1), describe.getColumns());
        Assert.assertEquals(0, describe.getByteSize());
        try {
            describe.getNumRows();
            Assert.fail("Expected getNumRows to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getName();
            Assert.fail("Expected getName to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }

        // Now describe with some complementary fields and perform the same check
        describe = g.describe(DescribeOptions.get().withCustomFields(ImmutableList.of("name", "length")));

        Assert.assertEquals("mygtable", describe.getName());
        Assert.assertEquals(0, describe.getNumRows());
        try {
            describe.getColumns();
            Assert.fail("Expected getColumns to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
        try {
            describe.getByteSize();
            Assert.fail("Expected getByteSize to fail with IllegalStateException");
        } catch (IllegalStateException e) {
            // Expected
        }
    }

    @Test
    public void testDescribeWithOptions() {
        ColumnSpecification column1 = ColumnSpecification.getInstance("name", ColumnType.STRING);
        DXGTable g = DXGTable.newGTable(ImmutableList.of(column1)).setProject(testProject).build();

        DXGTable.Describe describe = g.describe(DescribeOptions.get());

        Assert.assertEquals(ImmutableList.of(column1), describe.getColumns());
    }

    @Test
    public void testGetInstance() {
        DXGTable gtable = DXGTable.getInstance("gtable-000000000000000000000000");
        Assert.assertEquals("gtable-000000000000000000000000", gtable.getId());
        Assert.assertEquals(null, gtable.getProject());

        DXGTable gtable2 =
                DXGTable.getInstance("gtable-000000000000000000000001",
                        DXProject.getInstance("project-123412341234123412341234"));
        Assert.assertEquals("gtable-000000000000000000000001", gtable2.getId());
        Assert.assertEquals("project-123412341234123412341234", gtable2.getProject().getId());

        try {
            DXGTable.getInstance(null);
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXGTable.getInstance("gtable-123412341234123412341234", (DXContainer) null);
            Assert.fail("Expected creation without setting project to fail");
        } catch (NullPointerException e) {
            // Expected
        }
        try {
            DXGTable.getInstance(null, DXProject.getInstance("project-123412341234123412341234"));
            Assert.fail("Expected creation without setting ID to fail");
        } catch (NullPointerException e) {
            // Expected
        }
    }

    @Test
    public void testDataObjectMethods() {
        final ColumnSpecification column1 =
                ColumnSpecification.getInstance("name", ColumnType.STRING);

        DXDataObjectTest.BuilderFactory<DXGTable.Builder, DXGTable> builderFactory =
                new DXDataObjectTest.BuilderFactory<DXGTable.Builder, DXGTable>() {
                    @Override
                    public DXGTable.Builder getBuilder() {
                        return DXGTable.newGTable(ImmutableList.of(column1));
                    }
                };

        // Testing operations on open tables
        DXDataObjectTest.testOpenDataObjectMethods(testProject, builderFactory);

        DXGTable gtable =
                DXGTable.newGTable(ImmutableList.of(column1)).setProject(testProject).build();
        try {
            gtable.describe().getNumRows();
            Assert.fail("Expected calling getNumRows on an open table to fail");
        } catch (IllegalStateException e) {
            // Expected
        }

        // Testing operations on closed tables
        DXDataObjectTest.testClosedDataObjectMethods(testProject, builderFactory);

        gtable.closeAndWait();
        Assert.assertEquals(0, gtable.describe().getNumRows());
        Assert.assertEquals(0, gtable.describe().getByteSize());
    }

    @Test
    public void testBuilder() {
        final ColumnSpecification column1 =
                ColumnSpecification.getInstance("name", ColumnType.STRING);

        DXDataObjectTest.testBuilder(testProject,
                new DXDataObjectTest.BuilderFactory<DXGTable.Builder, DXGTable>() {
                    @Override
                    public DXGTable.Builder getBuilder() {
                        return DXGTable.newGTable(ImmutableList.of(column1));
                    }
                });

        // Retrieving the columns is tested in testCreateGTableSimple
    }

}
