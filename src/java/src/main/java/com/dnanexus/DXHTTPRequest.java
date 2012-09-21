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
        if (APISERVER_HOST == null) { APISERVER_HOST = "preprodapi.dnanexus.com"; }
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
