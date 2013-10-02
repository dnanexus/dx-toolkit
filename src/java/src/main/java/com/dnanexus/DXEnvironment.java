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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;

/**
 * Immutable class storing configuration for selecting, authenticating to, and
 * communicating with a DNAnexus API server.
 */
public class DXEnvironment {
    private final String apiserverHost;
    private final String apiserverPort;
    private final String apiserverProtocol;
    private final JsonNode securityContext;

    // TODO: provide accessors for these methods. Not exactly sure yet what the
    // interface will look like, or if these variables even belong here.
    private final String jobId;
    private final String workspaceId;
    private final String projectContextId;

    private static final JsonFactory jsonFactory = new MappingJsonFactory();;

    private DXEnvironment(String apiserverHost,
                          String apiserverPort,
                          String apiserverProtocol,
                          JsonNode securityContext,
                          String jobId,
                          String workspaceId,
                          String projectContextId) {
        this.apiserverHost = apiserverHost;
        this.apiserverPort = apiserverPort;
        this.apiserverProtocol = apiserverProtocol;
        this.securityContext = securityContext;
        this.jobId = jobId;
        this.workspaceId = workspaceId;
        this.projectContextId = projectContextId;

        // TODO: additional validation on the project/workspace, and check that
        // apiserverProtocol is either "http" or "https".

        if (this.securityContext == null) {
            System.err.println("Warning: no DNAnexus security context found.");
        }
    }

    private static String getTextValue(JsonNode jsonNode, String key) {
        JsonNode value = jsonNode.get(key);
        if (value == null) {
            return null;
        }
        return value.asText();
    }

    public String getApiserverPath() {
        return this.apiserverProtocol + "://" + this.apiserverHost + ":" + this.apiserverPort;
    }

    public JsonNode getSecurityContext() {
        return this.securityContext;
    }

    public static class Builder {
        private static final String DEFAULT_APISERVER_HOST = "api.dnanexus.com";
        private static final String DEFAULT_APISERVER_PORT = "443";
        private static final String DEFAULT_APISERVER_PROTOCOL = "https";

        private String apiserverHost;
        private String apiserverPort;
        private String apiserverProtocol;
        private JsonNode securityContext;

        private String jobId;
        private String workspaceId;
        private String projectContextId;

        /**
         * Upon construction, DXEnvironment.Builder loads configuration from (lowest to
         * highest precedence) (1) hardcoded defaults (2) JSON config in the file
         * ~/.dnanexus_config/environment.json and (3) the DX_* environment variables.
         */
        public Builder() {
            // (1) System defaults
            String securityContextTxt = null;
            apiserverHost = DEFAULT_APISERVER_HOST;
            apiserverPort = DEFAULT_APISERVER_PORT;
            apiserverProtocol = DEFAULT_APISERVER_PROTOCOL;
            jobId = null;
            workspaceId = null;
            projectContextId = null;

            // (2) JSON config file: ~/.dnanexus_config/environment.json
            File jsonConfigFile = new File(System.getProperty("user.home")
                                           + "/.dnanexus_config/environment.json");
            if (jsonConfigFile.exists()) {
                try {
                    JsonNode jsonConfig
                        = jsonFactory.createJsonParser(jsonConfigFile).readValueAsTree();
                    if (getTextValue(jsonConfig, "DX_APISERVER_HOST") != null) {
                        apiserverHost = getTextValue(jsonConfig, "DX_APISERVER_HOST");
                    }
                    if (getTextValue(jsonConfig, "DX_APISERVER_PORT") != null) {
                        apiserverPort = getTextValue(jsonConfig, "DX_APISERVER_PORT");
                    }
                    if (getTextValue(jsonConfig, "DX_APISERVER_PROTOCOL") != null) {
                        apiserverProtocol = getTextValue(jsonConfig, "DX_APISERVER_PROTOCOL");
                    }
                    if (getTextValue(jsonConfig, "DX_SECURITY_CONTEXT") != null) {
                        securityContextTxt = getTextValue(jsonConfig, "DX_SECURITY_CONTEXT");
                    }
                    if (getTextValue(jsonConfig, "DX_JOB_ID") != null) {
                        jobId = getTextValue(jsonConfig, "DX_JOB_ID");
                    }
                    if (getTextValue(jsonConfig, "DX_WORKSPACE_ID") != null) {
                        workspaceId = getTextValue(jsonConfig, "DX_WORKSPACE_ID");
                    }
                    if (getTextValue(jsonConfig, "DX_PROJECT_CONTEXT_ID") != null) {
                        projectContextId = getTextValue(jsonConfig, "DX_PROJECT_CONTEXT_ID");
                    }
                } catch (IOException e) {
                    System.err.println("WARNING: JSON config file " + jsonConfigFile.getPath()
                                       + " could not be parsed, skipping it");
                }
            }

            // (3) Environment variables
            Map<String, String> sysEnv = System.getenv();
            if (sysEnv.containsKey("DX_APISERVER_HOST")) {
                apiserverHost = sysEnv.get("DX_APISERVER_HOST");
            }
            if (sysEnv.containsKey("DX_APISERVER_PORT")) {
                apiserverPort = sysEnv.get("DX_APISERVER_PORT");
            }
            if (sysEnv.containsKey("DX_APISERVER_PROTOCOL")) {
                apiserverProtocol = sysEnv.get("DX_APISERVER_PROTOCOL");
            }
            if (sysEnv.containsKey("DX_SECURITY_CONTEXT")) {
                securityContextTxt = sysEnv.get("DX_SECURITY_CONTEXT");
            }
            if (sysEnv.containsKey("DX_JOB_ID")) {
                jobId = sysEnv.get("DX_JOB_ID");
            }
            if (sysEnv.containsKey("DX_WORKSPACE_ID")) {
                workspaceId = sysEnv.get("DX_WORKSPACE_ID");
            }
            if (sysEnv.containsKey("DX_PROJECT_CONTEXT_ID")) {
                projectContextId = sysEnv.get("DX_PROJECT_CONTEXT_ID");
            }

            try {
                if (securityContextTxt != null) {
                    securityContext = jsonFactory.createJsonParser(securityContextTxt).readValueAsTree();
                } else {
                    securityContext = null;
                }
            } catch (IOException exn) {
                throw new RuntimeException(exn);
            }
        }

        public Builder setSecurityContext(JsonNode json) {
            securityContext = json.deepCopy();
            return this;
        }

        // TODO: remaining setters

        /**
        * Build the DXEnvironment from the settings configured thusfar.
        */
        public DXEnvironment build() {
            return new DXEnvironment(apiserverHost, apiserverPort, apiserverProtocol,
                                     securityContext, jobId, workspaceId, projectContextId);
        }
    }

    /* backwards-compatible */
    public static DXEnvironment create() {
        return new Builder().build();
    }
}
