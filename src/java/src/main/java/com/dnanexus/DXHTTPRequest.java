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

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import org.apache.http.*;
import org.apache.http.client.*;
import org.apache.http.util.*;
import org.apache.http.entity.*;
import org.apache.http.client.methods.*;
import org.apache.http.impl.client.DefaultHttpClient;
import java.io.*;
import org.apache.commons.io.IOUtils;

public class DXHTTPRequest {
    public String APISERVER_HOST = System.getenv("DX_APISERVER_HOST");
    public String APISERVER_PORT = System.getenv("DX_APISERVER_PORT");
    public String SECURITY_CONTEXT = System.getenv("DX_SECURITY_CONTEXT");
    public String APISERVER_PROTOCOL = System.getenv("DX_APISERVER_PROTOCOL");
    public String JOB_ID = System.getenv("DX_JOB_ID");
    public String WORKSPACE_ID = System.getenv("DX_WORKSPACE_ID");
    public String PROJECT_CONTEXT_ID = System.getenv("DX_PROJECT_CONTEXT_ID");

    private JsonNode SecurityContext;
    private String apiserver;
    private DefaultHttpClient httpclient;
    private ObjectMapper mapper;
    private JsonFactory dxJsonFactory;

    public DXHTTPRequest() throws Exception {
        if (APISERVER_HOST == null) { APISERVER_HOST = "api.dnanexus.com"; }
        if (APISERVER_PORT == null) { APISERVER_PORT = "443"; }
        if (APISERVER_PROTOCOL == null) { APISERVER_PROTOCOL = "https"; }
        if (SECURITY_CONTEXT == null) { System.err.println("Warning: No security context found"); }
        
        httpclient = new DefaultHttpClient();
        apiserver = APISERVER_PROTOCOL + "://" + APISERVER_HOST + ":" + APISERVER_PORT;
        dxJsonFactory = new MappingJsonFactory();
        SecurityContext = dxJsonFactory.createJsonParser(SECURITY_CONTEXT).readValueAsTree();
    }

    public String request(String resource, String data) throws Exception {
        HttpPost request = new HttpPost(apiserver + resource);
        
        request.setHeader("Content-Type", "application/json");
        request.setHeader("Authorization", SecurityContext.get("auth_token_type").textValue()
        		+ " "
        		+ SecurityContext.get("auth_token").textValue());
        request.setEntity(new StringEntity(data));

        HttpResponse response = httpclient.execute(request);
        
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        	System.err.println(response.getStatusLine());
        	System.err.println(EntityUtils.toString(response.getEntity()));
        	throw new Exception();
        }

        HttpEntity entity = response.getEntity();
        return EntityUtils.toString(entity);
    }

    public JsonNode request(String resource, JsonNode data) throws Exception {
        String dataAsString = data.toString();
        String response = this.request(resource, dataAsString);
        JsonNode root = dxJsonFactory.createJsonParser(response).readValueAsTree();
        return root;
    }
}
