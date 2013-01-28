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

import org.junit.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.dnanexus.DXAPI;

public class DXAPITest {
    @BeforeClass public static void setUpClass() throws Exception {
        // Code executed before the first test method       
    }
    
    @Before public void setUp() throws Exception {
        // Code executed before each test    
    }

    @Test public void testDXAPI() throws Exception {
        DXAPI dx = new DXAPI();
        JsonNode input = (JsonNode)(new MappingJsonFactory().createJsonParser("{}").readValueAsTree());
        JsonNode root = dx.systemFindDataObjects(input);
        System.out.println(root);
    }

    @After public void tearDown() throws Exception {
        // Code executed after each test   
    }
 
    @AfterClass public static void tearDownClass() throws Exception {
        // Code executed after the last test method 
    }
}
