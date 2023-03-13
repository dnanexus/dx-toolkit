// Copyright (C) 2013-2016 DNAnexus, Inc.
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.dnanexus.exceptions.InvalidStateException;
import com.dnanexus.exceptions.ResourceNotFoundException;
import com.google.common.collect.ImmutableList;

/**
 * Tests for projects and containers.
 */
public class DXProjectTest {

    /**
     * Tests may set this field to have the project automatically destroyed at the end of the test.
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

    private DXFile createMinimalFile(String name) {
        return createMinimalFile(name, null);
    }

    private DXFile createMinimalFile(String name, String folder) {
        DXFile.Builder fileBuilder = DXFile.newFile()
                .setProject(testProject)
                .setName(name);

        if (folder != null) {
            fileBuilder.setFolder(folder);
        }

        DXFile file = fileBuilder.build();
        try {
            file.upload("content".getBytes());
        } catch(Exception ex) {
            Assert.fail("Creation of test file " + (folder != null ? folder : "/") + "/" + name + " failed!");
        }
        file.closeAndWait();
        return file;
    }

    /**
     * Delayes execution by i milliseconds.
     */
    private void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // External tests

    @Test
    public void testContainer() {
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

        // move objects
        c.newFolder("/destination");
        DXRecord record = DXRecord.newRecord().setProject(c).build();
        try {
            c.moveObjects(ImmutableList.of(record), "/nonexistent");
            Assert.fail("Expected moving object into a nonexistent folder to fail");
        } catch (ResourceNotFoundException e) {
            // Expected
        }
        c.moveObjects(ImmutableList.of(record), "/destination");
        Assert.assertEquals("/destination", record.describe().getFolder());
        Assert.assertEquals(record, c.listFolder("/destination").getObjects().get(0));

        // move folders
        c.newFolder("/sample");
        c.moveFolders(ImmutableList.of("/sample"), "/destination");
        Assert.assertEquals("/destination/sample", c.listFolder("/destination").getSubfolders()
                .get(0));

        // remove objects
        Assert.assertEquals(1, c.listFolder("/destination").getObjects().size());
        c.removeObjects(ImmutableList.of(record));
        Assert.assertEquals(0, c.listFolder("/destination").getObjects().size());
    }

    @Test
    public void testCreateProject() {
        try {
            DXProject.newProject().setName("");
            Assert.fail("Expected project creation to fail due to empty name");
        } catch (IllegalArgumentException e) {
            // Expected
        }

        try {
            DXProject.newProject().setName("foo").setName("bar");
            Assert.fail("Expected project creation to fail due to duplicate name setting");
        } catch (IllegalStateException e) {
            // Expected
        }

        try {
            DXProject.newProject().setName(null);
            Assert.fail("Expected project creation to fail due to null name");
        } catch (NullPointerException e) {
            // Expected
        }

        DXProject p = DXProject.newProject().setName("Java test project").build();
        this.testProject = p;
        Assert.assertEquals("Java test project", p.describe().getName());

        // System.out.println(p.getId());
    }

    @Test
    public void testArchive() throws IOException {
        this.testProject = DXProject.newProject().setName("DXProjectTest").build();

        final DXFile fakeFile = DXFile.getInstance("file-" + Strings.repeat("x", 24));
        List<DXFile> fakeFiles = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            fakeFiles.add(DXFile.getInstance("file-" + Strings.padStart(String.valueOf(i), 24, '0')));
        }
        String fakeJson = "{\"files\": [";
        for (DXFile f : fakeFiles) {
            fakeJson += "\"" + f.getId() + "\",";
        }
        fakeJson = fakeJson.substring(0, fakeJson.length() - 1) + "]}";

        // Test all possible methods and their repetitive calls
        Assert.assertEquals(DXJSON.parseJson(fakeJson), testProject.archive()
                .addFile(fakeFiles.get(0))
                .addFiles(fakeFiles.get(1), fakeFiles.get(2))
                .addFiles(ImmutableList.of(fakeFiles.get(3), fakeFiles.get(4)))
                .addFile(fakeFiles.get(5))
                .addFiles(fakeFiles.get(6), fakeFiles.get(7))
                .addFiles(ImmutableList.of(fakeFiles.get(8), fakeFiles.get(9)))
                .buildRequestHash());

        Assert.assertEquals(DXJSON.parseJson("{\"folder\": \"/folder\"}"), testProject.archive()
                .setFolder("/folder").buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"folder\": \"/folder\", \"recurse\": true}"), testProject.archive()
                .setFolder("/folder", true).buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"folder\": \"/folder\", \"recurse\": false}"), testProject.archive()
                .setFolder("/folder", false).buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"allCopies\": true}"), testProject.archive()
                .setAllCopies(true).buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"allCopies\": false}"), testProject.archive()
                .setAllCopies(false).buildRequestHash());

        // null params
        try {
            testProject.archive().addFile(null);
            Assert.fail("Expected archival to fail due to null file");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.archive().addFiles(fakeFile, null);
            Assert.fail("Expected archival to fail due to null file");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.archive().addFiles(ImmutableList.of(fakeFile, null));
            Assert.fail("Expected archival to fail due to null file");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder(null);
            Assert.fail("Expected archival to fail due to null folder");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder(null, true);
            Assert.fail("Expected archival to fail due to null folder");
        } catch (NullPointerException ex) {
            // Expected
        }

        // files and folder mutual exclusivity
        try {
            testProject.archive().addFile(fakeFile).setFolder("/folder");
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.archive().addFile(fakeFile).setFolder("/folder", true);
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder("/folder").addFile(fakeFile);
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder("/folder", true).addFile(fakeFile);
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        // setFolder single call restriction
        try {
            testProject.archive().setFolder("/folder").setFolder("/folder2");
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder("/folder", true).setFolder("/folder2");
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder("/folder").setFolder("/folder2", true);
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.archive().setFolder("/folder", true).setFolder("/folder2", true);
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        // setAllCopies single call restriction
        try {
            testProject.archive().setAllCopies(true).setAllCopies(true);
            Assert.fail("Expected archival to fail because setAllCopies should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        DXFile file1 = createMinimalFile("archiveFile1");
        DXFile file2 = createMinimalFile("archiveFile2");
        testProject.newFolder("/folder");
        testProject.newFolder("/folder/subfolder");
        createMinimalFile("archiveFile10", "/folder");
        createMinimalFile("archiveFile11", "/folder/subfolder");

        Assert.assertEquals(2, testProject.archive().addFiles(file1, file2).execute().getCount());
        Assert.assertEquals(2, testProject.archive().setFolder("/folder", true).execute().getCount());
    }

    @Test
    public void testUnarchive() throws IOException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        this.testProject = DXProject.newProject().setName("DXProjectTest").build();

        final DXFile fakeFile = DXFile.getInstance("file-" + Strings.repeat("x", 24));
        List<DXFile> fakeFiles = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            fakeFiles.add(DXFile.getInstance("file-" + Strings.padStart(String.valueOf(i), 24, '0')));
        }
        String fakeJson = "{\"files\": [";
        for (DXFile f : fakeFiles) {
            fakeJson += "\"" + f.getId() + "\",";
        }
        fakeJson = fakeJson.substring(0, fakeJson.length() - 1) + "]}";

        // Test all possible methods and their repetitive calls
        Assert.assertEquals(DXJSON.parseJson(fakeJson), testProject.unarchive()
                .addFile(fakeFiles.get(0))
                .addFiles(fakeFiles.get(1), fakeFiles.get(2))
                .addFiles(ImmutableList.of(fakeFiles.get(3), fakeFiles.get(4)))
                .addFile(fakeFiles.get(5))
                .addFiles(fakeFiles.get(6), fakeFiles.get(7))
                .addFiles(ImmutableList.of(fakeFiles.get(8), fakeFiles.get(9)))
                .buildRequestHash());

        Assert.assertEquals(DXJSON.parseJson("{\"folder\": \"/folder\"}"), testProject.unarchive()
                .setFolder("/folder").buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"folder\": \"/folder\", \"recurse\": true}"), testProject.unarchive()
                .setFolder("/folder", true).buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"folder\": \"/folder\", \"recurse\": false}"), testProject.unarchive()
                .setFolder("/folder", false).buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"dryRun\": true}"), testProject.unarchive()
                .setDryRun(true).buildRequestHash());
        Assert.assertEquals(DXJSON.parseJson("{\"dryRun\": false}"), testProject.unarchive()
                .setDryRun(false).buildRequestHash());
        Method rateGetValue = UnarchivingRate.class.getDeclaredMethod("getValue");
        rateGetValue.setAccessible(true);
        for (UnarchivingRate rate : UnarchivingRate.values()) {
            Assert.assertEquals(DXJSON.parseJson("{\"rate\": \"" + rateGetValue.invoke(rate) + "\"}"), testProject.unarchive()
                    .setRate(rate).buildRequestHash());
        }

        // null params
        try {
            testProject.unarchive().addFile(null);
            Assert.fail("Expected archival to fail due to null file");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.unarchive().addFiles(fakeFile, null);
            Assert.fail("Expected archival to fail due to null file");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.unarchive().addFiles(ImmutableList.of(fakeFile, null));
            Assert.fail("Expected archival to fail due to null file");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder(null);
            Assert.fail("Expected archival to fail due to null folder");
        } catch (NullPointerException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder(null, true);
            Assert.fail("Expected archival to fail due to null folder");
        } catch (NullPointerException ex) {
            // Expected
        }

        // files and folder mutual exclusivity
        try {
            testProject.unarchive().addFile(fakeFile).setFolder("/folder");
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.unarchive().addFile(fakeFile).setFolder("/folder", true);
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder("/folder").addFile(fakeFile);
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder("/folder", true).addFile(fakeFile);
            Assert.fail("Expected archival to fail due to the mutual exclusivity of files and folder");
        } catch (IllegalStateException ex) {
            // Expected
        }

        // setFolder single call restriction
        try {
            testProject.unarchive().setFolder("/folder").setFolder("/folder2");
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder("/folder", true).setFolder("/folder2");
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder("/folder").setFolder("/folder2", true);
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        try {
            testProject.unarchive().setFolder("/folder", true).setFolder("/folder2", true);
            Assert.fail("Expected archival to fail because setFolder should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        // setRate single call restriction
        try {
            testProject.unarchive().setRate(UnarchivingRate.EXPEDITED).setRate(UnarchivingRate.EXPEDITED);
            Assert.fail("Expected archival to fail because setRate should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        // setDryRun single call restriction
        try {
            testProject.unarchive().setDryRun(true).setDryRun(true);
            Assert.fail("Expected archival to fail because setDryRun should be callable only once");
        } catch (IllegalStateException ex) {
            // Expected
        }

        DXFile file1 = createMinimalFile("archiveFile1");
        DXFile file2 = createMinimalFile("archiveFile2");
        testProject.newFolder("/folder");
        testProject.newFolder("/folder/subfolder");
        createMinimalFile("archiveFile10", "/folder");
        createMinimalFile("archiveFile11", "/folder/subfolder");
        testProject.archive().setFolder("/", true).execute();

        // Wait for archival to complete
        final int maxRetries = 24;
        for (int i = 1; i <= maxRetries; ++i) {
            if (file1.describe().getArchivalState() == ArchivalState.ARCHIVED) {
                break;
            }
            if (i == maxRetries) {
                Assert.fail("Could not archive test files. Test cannot proceed...");
            }
            sleep(5000);
        }

        Assert.assertEquals(2, testProject.unarchive().addFiles(file1, file2).execute().getFiles());
        Assert.assertEquals(2, testProject.unarchive().setFolder("/folder", true).execute().getFiles());
    }

    // Internal tests

    @Test
    public void testCreatProjectSerialization() throws IOException {
        Assert.assertEquals(DXJSON.parseJson("{\"name\": \"projectname\"}"), DXProject.newProject()
                .setName("projectname").buildRequestHash());
    }

}
