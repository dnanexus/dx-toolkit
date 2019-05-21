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

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Preconditions;

/**
 * Immutable class storing configuration for selecting, authenticating to, and communicating with a
 * DNAnexus API server.
 */
public class DXEnvironment {

    /**
     * Builder class for creating DXEnvironment objects.
     *
     * <p>
     * The resulting DXEnvironment has its fields set from the following sources, in order (with
     * later items overriding earlier items):
     * </p>
     * <ol>
     * <li>Hardcoded defaults</li>
     * <li>JSON config in the file specified at the Builder object's creation time (you can specify
     * the file using {@link DXEnvironment.Builder#fromFile(File)}).</li>
     * <li>DX_* environment variables</li>
     * <li>Fields set by calling methods on the Builder object</li>
     * </ol>
     */
    public static class Builder {
        private static final String DEFAULT_APISERVER_HOST = "api.dnanexus.com";
        private static final String DEFAULT_APISERVER_PORT = "443";
        private static final String DEFAULT_APISERVER_PROTOCOL = "https";

        /**
         * Creates a Builder object using the JSON config in the file
         * <tt>~/.dnanexus_config/environment.json</tt>.
         *
         * @return new Builder object
         */
        public static Builder fromDefaults() {
            return new Builder();
        }

        /**
         * Creates a Builder object with initial settings copied from the specified environment.
         *
         * @param templateEnvironment environment to initialize this Builder from
         *
         * @return new Builder object
         */
        public static Builder fromEnvironment(DXEnvironment templateEnvironment) {
            return new Builder(templateEnvironment);
        }

        /**
         * Creates a Builder object using the JSON config in the specified file.
         *
         * @param environmentJsonFile JSON file from which to load configuration defaults
         *
         * @return new Builder object
         */
        public static Builder fromFile(File environmentJsonFile) {
            return new Builder(environmentJsonFile);
        }

        private String apiserverHost;
        private String apiserverPort;
        private String apiserverProtocol;
        private JsonNode securityContext;
        private String jobId;
        private String workspaceId;
        private String projectContextId;
        private boolean disableRetry;
        private int socketTimeout;
        private int connectionTimeout;
        private String httpProxy;
        private String httpProxyMethod;
        private String httpProxyDomain;

        /**
         * Initializes a Builder object using JSON config in the file
         * <tt>~/.dnanexus_config/environment.json</tt>.
         *
         * @deprecated Use {@link #fromDefaults()} instead
         */
        @Deprecated
        public Builder() {
            this(new File(System.getProperty("user.home") + "/.dnanexus_config/environment.json"));
        }

        private Builder(DXEnvironment templateEnvironment) {
            apiserverHost = templateEnvironment.apiserverHost;
            apiserverPort = templateEnvironment.apiserverPort;
            apiserverProtocol = templateEnvironment.apiserverProtocol;
            securityContext = templateEnvironment.securityContext;
            jobId = templateEnvironment.jobId;
            workspaceId = templateEnvironment.workspaceId;
            projectContextId = templateEnvironment.projectContextId;
            disableRetry = templateEnvironment.disableRetry;
            if (templateEnvironment.proxy != null) {
                httpProxy = templateEnvironment.proxy.rawDefinition;
                httpProxyMethod = templateEnvironment.proxy.method;
                httpProxyDomain = templateEnvironment.proxy.domain;
            }
        }

        private Builder(File jsonConfigFile) {
            // (1) System defaults
            String securityContextTxt = null;
            apiserverHost = DEFAULT_APISERVER_HOST;
            apiserverPort = DEFAULT_APISERVER_PORT;
            apiserverProtocol = DEFAULT_APISERVER_PROTOCOL;
            jobId = null;
            workspaceId = null;
            projectContextId = null;
            disableRetry = false;
            httpProxy = null;
            httpProxyMethod = null;
            httpProxyDomain = null;

            // (2) JSON file
            if (jsonConfigFile.exists()) {
                try {
                    JsonNode jsonConfig = jsonFactory.createParser(jsonConfigFile)
                            .readValueAsTree();
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
                        doDebug("DX_SECURITY_CONTEXT property found in environment file: %s", "init", jsonConfigFile);
                    }
                    if (getTextValue(jsonConfig, "DX_JOB_ID") != null) {
                        jobId = getTextValue(jsonConfig, "DX_JOB_ID");
                    }
                    String maybeWorkspaceId = getTextValue(jsonConfig, "DX_WORKSPACE_ID");
                    if (maybeWorkspaceId != null && !maybeWorkspaceId.isEmpty()) {
                        workspaceId = maybeWorkspaceId;
                    }
                    if (getTextValue(jsonConfig, "DX_PROJECT_CONTEXT_ID") != null) {
                        projectContextId = getTextValue(jsonConfig, "DX_PROJECT_CONTEXT_ID");
                        doDebug("DX_PROJECT_CONTEXT_ID property %s found in environment file: %s", "init", projectContextId, jsonConfigFile);
                    }
                    if (getIntValue(jsonConfig, "DX_SOCKET_TIMEOUT") != 0) {
                        socketTimeout = getIntValue(jsonConfig, "DX_SOCKET_TIMEOUT");
                    }
                    if (getIntValue(jsonConfig, "DX_CONNECTION_TIMEOUT") != 0) {
                        connectionTimeout = getIntValue(jsonConfig, "DX_CONNECTION_TIMEOUT");
                    }
                    if (getTextValue(jsonConfig, "DX_DISABLE_RETRY") != null) {
                        //disableRetry = getBooleanValue(jsonConfig, "DX_DISABLE_RETRY");
                        disableRetry = jsonConfig.findValue("DX_DISABLE_RETRY").asBoolean();
                    }
                    if (getTextValue(jsonConfig, "HTTP_PROXY") != null) {
                        httpProxy = getTextValue(jsonConfig, "HTTP_PROXY");
                    }
                    if (getTextValue(jsonConfig, "HTTPS_PROXY") != null) {
                        httpProxy = getTextValue(jsonConfig, "HTTPS_PROXY");
                    }
                    if (getTextValue(jsonConfig, "HTTP_PROXY_METHOD") != null) {
                        httpProxyMethod = getTextValue(jsonConfig, "HTTP_PROXY_METHOD");
                    }
                    if (getTextValue(jsonConfig, "HTTP_PROXY_DOMAIN") != null) {
                        httpProxyDomain = getTextValue(jsonConfig, "HTTP_PROXY_DOMAIN");
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
                doDebug("DX_SECURITY_CONTEXT env variable found", "init");
            }
            if (sysEnv.containsKey("DX_JOB_ID")) {
                jobId = sysEnv.get("DX_JOB_ID");
            }
            if (sysEnv.containsKey("DX_WORKSPACE_ID")) {
                workspaceId = sysEnv.get("DX_WORKSPACE_ID");
            }
            if (sysEnv.containsKey("DX_PROJECT_CONTEXT_ID")) {
                projectContextId = sysEnv.get("DX_PROJECT_CONTEXT_ID");
                doDebug("DX_PROJECT_CONTEXT_ID env variable found: %s","init", projectContextId);
            }
            if (sysEnv.containsKey("DX_SOCKET_TIMEOUT")) {
                socketTimeout = Integer.valueOf(sysEnv.get("DX_SOCKET_TIMEOUT"));
            }
            if (sysEnv.containsKey("DX_CONNECTION_TIMEOUT")) {
                connectionTimeout = Integer.valueOf(sysEnv.get("DX_CONNECTION_TIMEOUT"));
            }
            if (sysEnv.containsKey("DX_DISABLE_RETRY")) {
                disableRetry = Boolean.valueOf(sysEnv.get("DX_DISABLE_RETRY"));
            }
            if (sysEnv.containsKey("HTTP_PROXY")) {
                httpProxy = sysEnv.get("HTTP_PROXY");
            }
            if (sysEnv.containsKey("HTTPS_PROXY")) {
                httpProxy = sysEnv.get("HTTPS_PROXY");
            }
            if (sysEnv.containsKey("HTTP_PROXY_METHOD")) {
                httpProxyMethod = sysEnv.get("HTTP_PROXY_METHOD");
            }
            if (sysEnv.containsKey("HTTP_PROXY_DOMAIN")) {
                httpProxyDomain = sysEnv.get("HTTP_PROXY_DOMAIN");
            }

            try {
                if (securityContextTxt != null) {
                    securityContext = jsonFactory.createParser(securityContextTxt)
                            .readValueAsTree();
                } else {
                    securityContext = null;
                }
            } catch (IOException exn) {
                throw new RuntimeException(exn);
            }
        }

        /**
         * Build the DXEnvironment from the settings configured so far.
         *
         * @return newly created DXEnvironment
         */
        public DXEnvironment build() {
            return new DXEnvironment(apiserverHost, apiserverPort, apiserverProtocol,
                                     securityContext, jobId, workspaceId, projectContextId, disableRetry,
                                     socketTimeout, connectionTimeout,
                                     httpProxy, httpProxyMethod, httpProxyDomain);
        }

        /**
         * Sets the API server hostname.
         *
         * @param apiserverHost API server hostname
         *
         * @return the same Builder object
         */
        public Builder setApiserverHost(String apiserverHost) {
            this.apiserverHost = apiserverHost;
            return this;
        }

        /**
         * Sets the API server port.
         *
         * @param apiserverPort API server port
         *
         * @return the same Builder object
         */
        public Builder setApiserverPort(int apiserverPort) {
            this.apiserverPort = Integer.toString(apiserverPort);
            return this;
        }

        /**
         * Sets the API server protocol ("http" or "https").
         *
         * @param apiserverProtocol API server protocol
         *
         * @return the same Builder object
         */
        public Builder setApiserverProtocol(String apiserverProtocol) {
            this.apiserverProtocol = apiserverProtocol;
            return this;
        }

        /**
         * Sets the token to use to authenticate to the Platform.
         *
         * @param token bearer token
         *
         * @return the same Builder object
         */
        public Builder setBearerToken(String token) {
            securityContext =
                    DXJSON.getObjectBuilder().put("auth_token_type", "Bearer")
                            .put("auth_token", token).build();
            return this;
        }

        /**
         * Sets the current job to the specified job.
         *
         * @param job job object
         *
         * @return the same Builder object
         */
        public Builder setJob(DXJob job) {
            jobId = Preconditions.checkNotNull(job).getId();
            return this;
        }

        /**
         * Sets the project context to the specified project.
         *
         * @param projectContext project context
         *
         * @return the same Builder object
         */
        public Builder setProjectContext(DXProject projectContext) {
            projectContextId = Preconditions.checkNotNull(projectContext).getId();
            return this;
        }

        /**
         * Sets the security context to use to authenticate to the Platform.
         *
         * @param json security context JSON
         *
         * @return the same Builder object
         *
         * @deprecated Use {@link #setBearerToken(String)} instead.
         */
        @Deprecated
        public Builder setSecurityContext(JsonNode json) {
            securityContext = json.deepCopy();
            return this;
        }

        /**
         * Sets the workspace to the specified container.
         *
         * @param workspace workspace container
         *
         * @return the same Builder object
         */
        public Builder setWorkspace(DXContainer workspace) {
            workspaceId = Preconditions.checkNotNull(workspace).getId();
            return this;
        }

        /**
         * Disables automatic retry of HTTP requests.
         *
         * @param disableRetryLogic boolean
         *
         * @return the same Builder object
         */
        public Builder disableRetry() {
            disableRetry = true;
            return this;
        }


        /**
         * Sets connection timeout of HTTP requests.
         *
         * @param connectionTimeout integer
         *
         * @return the same Builder object
         */
        public Builder setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets socket timeout of HTTP requests.
         *
         * @param socketTimeout integer
         *
         * @return the same Builder object
         */
        public Builder setSocketTimeout(int socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        /**
         * Sets the HTTP proxy server
         *
         * @param httpProxy String
         *
         * @return the same Builder object
         */
        public Builder setHttpProxy(String httpProxy) {
            this.httpProxy = httpProxy;
            return this;
        }
    }

    public static class ProxyDesc {
        HttpHost host;
        Boolean authRequired;
        String username;
        String password;
        String rawDefinition;
        String method;   // NTLM, or user-password are supported.
        String domain;   // Currently used just for NTLM

        ProxyDesc(HttpHost host,
                  Boolean authRequired,
                  String username,
                  String password,
                  String rawDefinition,
                  String method,
                  String domain) {
            this.host = host;
            this.authRequired = authRequired;
            this.username = username;
            this.password = password;
            this.rawDefinition = rawDefinition;
            this.method = method;
            this.domain = domain;
        }
    }

    private final String apiserverHost;
    private final String apiserverPort;
    private final String apiserverProtocol;
    private final JsonNode securityContext;
    private final String jobId;
    private final String workspaceId;
    private final String projectContextId;
    private final boolean disableRetry;
    private int socketTimeout;
    private int connectionTimeout;
    private final ProxyDesc proxy;

    private static final JsonFactory jsonFactory = new MappingJsonFactory();
    /**
     * Creates a DXEnvironment from the default settings.
     *
     * <p>
     * This is the same as <code>Builder.fromDefaults().build()</code>.
     * </p>
     *
     * @return newly created DXEnvironment
     */
    public static DXEnvironment create() {
        return Builder.fromDefaults().build();
    }

    private static String getTextValue(JsonNode jsonNode, String key) {
        JsonNode value = jsonNode.get(key);
        if (value == null || value.isNull()) {
            return null;
        }
        return value.asText();
    }

    private static int getIntValue(JsonNode jsonNode, String key) {
        JsonNode value = jsonNode.get(key);
        if (value == null || value.isNull()) {
            return 0;
        }
        return value.asInt();
    }

    private DXEnvironment(String apiserverHost, String apiserverPort, String apiserverProtocol,
                          JsonNode securityContext, String jobId, String workspaceId, String projectContextId, boolean
                          disableRetry, int socketTimeout, int connectionTimeout,
                          String httpProxy, String httpProxyMethod, String httpProxyDomain) {
        this.apiserverHost = apiserverHost;
        this.apiserverPort = apiserverPort;
        this.apiserverProtocol = apiserverProtocol;
        this.securityContext = securityContext;
        this.jobId = jobId;
        this.workspaceId = workspaceId;
        this.projectContextId = projectContextId;
        this.disableRetry = disableRetry;
        this.socketTimeout = socketTimeout;
        this.connectionTimeout = connectionTimeout;
        this.proxy = parseProxyDefinition(httpProxy, httpProxyMethod, httpProxyDomain);

        // TODO: additional validation on the project/workspace, and check that
        // apiserverProtocol is either "http" or "https".

        if (this.securityContext == null) {
            System.err.println("Warning: no DNAnexus security context found.");
        }
    }

    /**
     * Returns the fully qualified API server address (including protocol, host, and port).
     *
     * @return API server path
     */
    public String getApiserverPath() {
        return this.apiserverProtocol + "://" + this.apiserverHost + ":" + this.apiserverPort;
    }

    /**
     * Returns a handler to the currently running job.
     *
     * @return job object, or {@code null} if the currently running job cannot be determined
     */
    public DXJob getJob() {
        if (jobId == null) {
            return null;
        }
        return DXJob.getInstanceWithEnvironment(jobId, this);
    }

    /**
     * Returns the current project context.
     *
     * @return project, or {@code null} if the project context cannot be determined
     */
    public DXProject getProjectContext() {
        if (projectContextId == null) {
            return null;
        }
        return DXProject.getInstanceWithEnvironment(projectContextId, this);
    }

    /**
     * Returns the security context JSON.
     *
     * @return security context
     *
     * @deprecated
     */
    @Deprecated
    public JsonNode getSecurityContext() {
        return this.securityContext;
    }

    /**
     * Returns the security context JSON (for use by {@link DXHTTPRequest}).
     *
     * @return security context
     */
    JsonNode getSecurityContextJson() {
        return this.securityContext;
    }

    /**
     * Returns the temporary workspace of the currently running job, or the current project if this
     * method is called outside the Execution Environment.
     *
     * @return {@code DXContainer} for the container, or {@code null} if the container cannot be
     *         determined
     */
    public DXContainer getWorkspace() {
        if (this.workspaceId == null) {
            return null;
        }
        return DXContainer.getInstanceWithEnvironment(this.workspaceId, this);
    }

    /**
     * Returns whether the retry of HTTP requests should be disabled.
     *
     * @return boolean
     */
    public boolean isRetryDisabled() {
        return this.disableRetry;
    }

    /**
     * Returns socket read timeout for http client
     */
    public int getSocketTimeout() {
        return this.socketTimeout;
    }

    /**
     * Parse the proxy definition. This could take one of several forms:
     *    localhost:3128
     *    http://localhost:3128
     *    https://localhost:3128
     *    https://dnanexus:welcome@localhost:3128
     */
    public ProxyDesc parseProxyDefinition(String proxyDfn, String proxyMethod, String proxyDomain) {
        if (proxyDfn == null)
            return null;

        // Configure a proxy
        boolean authRequired = proxyDfn.contains("@");
        if (!authRequired) {
            HttpHost proxyHost = HttpHost.create(proxyDfn);
            return new ProxyDesc(proxyHost, false, null, null, proxyDfn, null, null);
        }

        // We need to authenticate with a username and password.
        // The format is something like this: "https://dnanexus:welcome@localhost:3128"

        // setup the proxy host ("localhost:3128")
        String proxyHostWithPort = proxyDfn.substring(proxyDfn.indexOf('@') + 1);
        HttpHost proxyHost = HttpHost.create(proxyHostWithPort);

        // strip out the "dnanexus:welcome" portion.
        String userPass = proxyDfn.substring(0, proxyDfn.indexOf('@') - 1);
        if (userPass.contains("://"))
            userPass = userPass.substring(userPass.indexOf("://") + 3);
        if (!userPass.contains(":"))
            throw new RuntimeException("proxy definition (" + proxyDfn +
                                       ") does not specify a user:password tuple");
        String user = userPass.substring(0, userPass.indexOf(':') - 1);
        String pass = userPass.substring(userPass.indexOf(':') +1);

        if (proxyMethod != null &&
            proxyMethod.toLowerCase().equals("ntlm")) {
            doDebug("NTLM authentication method specified", null);
            return new ProxyDesc(proxyHost, true, user, pass, proxyDfn, "ntlm", proxyDomain);
        }
        return new ProxyDesc(proxyHost, true, user, pass, proxyDfn, null, null);
    }

    /**
     * Returns the proxy
     */
    public ProxyDesc getProxy() {
        return this.proxy;
    }

    /**
     * Returns connection read timeout for http client
     */
    public int getConnectionTimeout() {
        return this.connectionTimeout;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DXEnvironment.class);

    private static boolean isDebug() {
        return LOG.isDebugEnabled();
    }

    private static void doDebug(String msg, String method, Object... args) {
        if (LOG.isDebugEnabled()) {
            if (method == null) {
                LOG.debug(String.format(msg, args));
            } else {
                LOG.debug(String.format("[" + method + "] " + msg, args));
            }
        }
    }
}
