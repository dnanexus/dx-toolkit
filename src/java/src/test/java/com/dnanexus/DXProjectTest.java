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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.exceptions.InvalidStateException;
import com.dnanexus.exceptions.ResourceNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;

/**
 * Tests for projects and containers.
 */
public class DXProjectTest {

    /**
     * Tests may set this field to have the project automatically destroyed at
     * the end of the test.
     */
    private DXProject testProject;

    @Before
    public void setUp() {
        testProject = null;
    }

    @After
    public void tearDown() {
        if (testProject != null) {
            testProject.destroy();
        }
    }

    // External tests

    @Test
    public void testContainer() throws JsonProcessingException {
        try {
            DXContainer.getInstance(null);
            Assert.fail("Expected DXContainer creation to fail with a null ID");
        } catch (NullPointerException e) {
            // Expected
        }

        try {
            DXContainer.getInstance("file-012301230123012301230123");
            Assert.fail("Expected DXContainer creation to fail with a non-container ID");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            DXProject.getInstance("container-012301230123012301230123");
            Assert.fail("Expected DXProject.getInstance to fail when a container ID is supplied");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        // Create some handles successfully, but no such container exists on the
        // server, so we can't really use the resulting objects to make API
        // calls.
        DXContainer.getInstance("container-012301230123012301230123");
        DXContainer.getInstance("project-012301230123012301230123");
        DXProject.getInstance("project-012301230123012301230123");

        DXProject p = DXProject.newProject().setName("Java test project").build();
        this.testProject = p;

        DXContainer c = DXContainer.getInstance(p.getId());
        Assert.assertEquals(p.getId(), c.getId());

        // newFolder
        c.newFolder("/a");
        c.newFolder("/a/b");
        try {
            c.newFolder("/a/b/c/d");
            Assert.fail("Expected creation of /a/b/c/d to fail since its parent doesn't exist");
        } catch (ResourceNotFoundException e) {
            Assert.assertTrue(e.getMessage().matches(".*parent folder.+does not exist.*"));
        }
        c.newFolder("/a/b/c/d", true);
        Assert.assertEquals(ImmutableList.of("/a/b/c"), c.listFolder("/a/b").getSubfolders());

        // renameFolder
        Assert.assertEquals(ImmutableList.of("/a/b/c/d"), c.listFolder("/a/b/c").getSubfolders());
        c.renameFolder("/a/b/c/d", "e");
        try {
            c.renameFolder("/a/b/c/d", "e");
            Assert.fail("Expected rename of folder to fail since it was previously renamed");
        } catch (ResourceNotFoundException e) {
            // Expected
        }
        Assert.assertEquals(ImmutableList.of("/a/b/c/e"), c.listFolder("/a/b/c").getSubfolders());

        // removeFolder
        try {
            c.removeFolder("/a");
            Assert.fail("Expected removeFolder of non-empty folder to fail");
        } catch (InvalidStateException e) {
            // Expected
        }
        c.removeFolder("/a/b/c/e");
        c.removeFolder("/a/b", true);
        Assert.assertEquals(ImmutableList.of(), c.listFolder("/a").getSubfolders());
    }

    @Test
    public void testCreateProject() throws JsonProcessingException {
        try {
            DXProject.newProject().setName("").build();
            Assert.fail("Expected project creation to fail due to empty name");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            DXProject.newProject().setName("foo").setName("bar").build();
            Assert.fail("Expected project creation to fail due to empty name");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            DXProject.newProject().setName(null).build();
            Assert.fail("Expected project creation to fail due to null name");
        } catch (NullPointerException e) {
            // Expected
        }

        DXProject p = DXProject.newProject().setName("Java test project").build();
        this.testProject = p;
        Assert.assertEquals("Java test project", p.describe().getName());

        // System.out.println(p.getId());
    }

    // Internal tests

    @Test
    public void testCreatProjectSerialization() throws IOException {
        Assert.assertEquals(DXJSON.parseJson("{\"name\": \"projectname\"}"),
                DXProject.newProject().setName("projectname").buildRequestHash());
    }

}
