// Copyright (C) 2013-2015 DNAnexus, Inc.
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import com.dnanexus.DXHTTPRequest.RetryStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

/**
 * A file (an opaque sequence of bytes).
 */
public class DXFile extends DXDataObject {

    /**
     * Builder class for creating a new {@code DXFile} object. To obtain an instance, call
     * {@link DXFile#newFile()}.
     */
    public static class Builder extends DXDataObject.Builder<Builder, DXFile> {
        private String media;
        private InputStream uploadData;

        private Builder() {
            super();
        }

        private Builder(DXEnvironment env) {
            super(env);
        }

        /**
         * Creates the file.
         *
         * @return a {@code DXFile} object corresponding to the newly created object
         */
        @Override
        public DXFile build() {
            DXFile file = new DXFile(DXAPI.fileNew(this.buildRequestHash(), ObjectNewResponse.class, this.env).getId(),
                    this.project, this.env, null);

            if (uploadData != null) {
                file.upload(uploadData);
            }

            return file;
        }

        /**
         * Use this method to test the JSON hash created by a particular builder call without
         * actually executing the request.
         *
         * @return a JsonNode
         */
        @VisibleForTesting
        JsonNode buildRequestHash() {
            checkAndFixParameters();
            return MAPPER.valueToTree(new FileNewRequest(this));
        }

        /*
         * (non-Javadoc)
         *
         * @see com.dnanexus.DXDataObject.Builder#getThisInstance()
         */
        @Override
        protected Builder getThisInstance() {
            return this;
        }

        /**
         * Sets the Internet Media Type of the file to be created.
         *
         * @param mediaType Internet Media Type
         *
         * @return the same {@code Builder} object
         */
        public Builder setMediaType(String mediaType) {
            Preconditions.checkState(this.media == null, "Cannot call setMediaType more than once");
            this.media = Preconditions.checkNotNull(mediaType, "mediaType may not be null");
            return getThisInstance();
        }

        /**
         * Uploads the data in the specified byte array to the file to be created.
         *
         * @param data data to be uploaded
         *
         * @return the same {@code Builder} object
         */
        public Builder upload(byte[] data) {
            Preconditions.checkNotNull(data, "data may not be null");
            InputStream dataStream = new ByteArrayInputStream(data);
            return this.upload(dataStream);
        }

        /**
         * Uploads the data in the specified stream to the file to be created.
         *
         * @param data stream containing data to be uploaded
         *
         * @return the same {@code Builder} object
         */
        public Builder upload(InputStream data) {
            Preconditions.checkNotNull(this.uploadData == null, "Cannot call upload more than once");
            this.uploadData = Preconditions.checkNotNull(data, "data may not be null");
            return getThisInstance();
        }
    }

    /**
     * Contains metadata for a file.
     */
    public static class Describe extends DXDataObject.Describe {
        @JsonProperty
        private String media;

        private Describe() {
            super();
        }

        /**
         * Returns the Internet Media Type of the file.
         *
         * @return Internet Media Type
         */
        public String getMediaType() {
            Preconditions.checkState(this.media != null,
                    "media type is not accessible because it was not retrieved with the describe call");
            return media;
        }
    }

    /**
     * Request to /file-xxxx/download.
     */
    @JsonInclude(Include.NON_NULL)
    private static class FileDownloadRequest {
        @JsonProperty("preauthenticated")
        private boolean preauth;

        private FileDownloadRequest(boolean preauth) {
            this.preauth = preauth;
        }
    }

    /**
     * Deserialized output from the /file-xxxx/download route.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class FileDownloadResponse {
        @JsonProperty
        private String url;
    }

    @JsonInclude(Include.NON_NULL)
    private static class FileNewRequest extends DataObjectNewRequest {
        @JsonProperty
        private final String media;

        public FileNewRequest(Builder builder) {
            super(builder);
            this.media = builder.media;
        }
    }

    /**
     * Request to /file-xxxx/upload.
     */
    @JsonInclude(Include.NON_NULL)
    private static class FileUploadRequest {
        @JsonProperty
        private String md5;
        @JsonProperty
        private int size;

        private FileUploadRequest(int size, String md5) {
            this.size = size;
            this.md5 = md5;
        }
    }

    /**
     * Response from /file-xxxx/upload
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class FileUploadResponse {
        @JsonProperty
        private Map<String, String> headers;
        @JsonProperty
        private String url;
    }

    private static final String USER_AGENT = DXUserAgent.getUserAgent();

    /**
     * Deserializes a DXFile from JSON containing a DNAnexus link.
     *
     * @param value JSON object map
     *
     * @return data object
     */
    @JsonCreator
    private static DXFile create(Map<String, Object> value) {
        checkDXLinkFormat(value);
        // TODO: how to set the environment?
        return DXFile.getInstance((String) value.get("$dnanexus_link"));
    }

    /**
     * Returns a {@code DXFile} associated with an existing file.
     *
     * @throws NullPointerException If {@code fileId} is null
     */
    public static DXFile getInstance(String fileId) {
        return new DXFile(fileId, null);
    }

    /**
     * Returns a {@code DXFile} associated with an existing file in a particular project or
     * container.
     *
     * @throws NullPointerException If {@code fileId} or {@code container} is null
     */
    public static DXFile getInstance(String fileId, DXContainer project) {
        return new DXFile(fileId, project, null, null);
    }

    /**
     * Returns a {@code DXFile} associated with an existing file in a particular project using the
     * specified environment, with the specified cached describe output.
     *
     * <p>
     * This method is for use exclusively by bindings to the "find" routes when describe hashes are
     * returned with the find output.
     * </p>
     *
     * @throws NullPointerException If any argument is null
     */
    static DXFile getInstanceWithCachedDescribe(String fileId, DXContainer project,
            DXEnvironment env, JsonNode describe) {
        return new DXFile(fileId, project, Preconditions.checkNotNull(env, "env may not be null"),
                Preconditions.checkNotNull(describe, "describe may not be null"));
    }

    /**
     * Returns a {@code DXFile} associated with an existing file in a particular project using the
     * specified environment.
     *
     * @throws NullPointerException If {@code fileId} or {@code container} is null
     */
    public static DXFile getInstanceWithEnvironment(String fileId, DXContainer project,
            DXEnvironment env) {
        return new DXFile(fileId, project, Preconditions.checkNotNull(env, "env may not be null"),
                null);
    }

    /**
     * Returns a {@code DXFile} associated with an existing file using the specified environment.
     *
     * @throws NullPointerException If {@code fileId} is null
     */
    public static DXFile getInstanceWithEnvironment(String fileId, DXEnvironment env) {
        return new DXFile(fileId, Preconditions.checkNotNull(env, "env may not be null"));
    }

    /**
     * Returns a Builder object for creating a new {@code DXFile}.
     *
     * @return a newly initialized builder object
     */
    public static Builder newFile() {
        return new Builder();
    }

    /**
     * Returns a Builder object for creating a new {@code DXFile} using the specified environment.
     *
     * @param env environment to use to make API calls
     *
     * @return a newly initialized builder object
     */
    public static Builder newFileWithEnvironment(DXEnvironment env) {
        return new Builder(env);
    }

    private DXFile(String fileId, DXContainer project, DXEnvironment env, JsonNode describe) {
        super(fileId, "file", project, env, describe);
    }

    private DXFile(String fileId, DXEnvironment env) {
        super(fileId, "file", env, null);
    }

    @Override
    public DXFile close() {
        super.close();
        return this;
    }

    @Override
    public DXFile closeAndWait() {
        super.closeAndWait();
        return this;
    }

    @Override
    public Describe describe() {
        return DXJSON.safeTreeToValue(apiCallOnObject("describe", RetryStrategy.SAFE_TO_RETRY),
                Describe.class);
    }

    @Override
    public Describe describe(DescribeOptions options) {
        return DXJSON.safeTreeToValue(
                apiCallOnObject("describe", MAPPER.valueToTree(options),
                        RetryStrategy.SAFE_TO_RETRY), Describe.class);
    }

    /**
     * Downloads the file and returns a byte array of its contents. <b>This implementation buffers
     * the contents of the file in-memory; therefore, the file must be small.</b>
     *
     * @return byte array containing file contents
     */
    // TODO: set project ID containing the file to be downloaded
    public byte[] downloadBytes() {
        // API call returns URL for HTTP GET requests
        JsonNode output = apiCallOnObject("download", MAPPER.valueToTree(new FileDownloadRequest(true)),
                RetryStrategy.SAFE_TO_RETRY);

        FileDownloadResponse apiResponse;
        HttpClient httpclient = HttpClientBuilder.create().setUserAgent(USER_AGENT).build();
        InputStream content = null;
        byte[] data;

        try {
            apiResponse = MAPPER.treeToValue(output, FileDownloadResponse.class);

            // HTTP GET request to download URL
            HttpGet request = new HttpGet(apiResponse.url);
            HttpResponse response = httpclient.execute(request);
            content = response.getEntity().getContent();

            data = IOUtils.toByteArray(content);
            content.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }

    /**
     * Downloads the file and returns a stream of its contents. <b>This implementation buffers the
     * contents of the file in-memory before the contents are written into the stream; therefore,
     * the file must be small enough to be buffered in memory.</b>
     *
     * @return stream containing file contents
     */
    public OutputStream downloadStream() {
        byte[] dataBytes = this.downloadBytes();
        OutputStream data = new ByteArrayOutputStream(dataBytes.length);
        try {
            data.write(dataBytes, 0, dataBytes.length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return DXJSON.safeTreeToValue(this.cachedDescribe, Describe.class);
    }

    /**
     * Uploads data from the specified byte array to the file. <b>This implementation buffers the
     * data in-memory before being uploaded to the server; therefore, the data must be small.</b>
     *
     * <p>
     * The file must be in the "open" state. This method assumes exclusive access to the file: the
     * file must have no parts uploaded before this call is made, and no other clients may upload
     * data to the same file concurrently.
     * </p>
     *
     * @param data data in bytes to be uploaded
     */
    public void upload(byte[] data) {
        Preconditions.checkNotNull(data, "data may not be null");

        // MD5 digest as 32 character hex string
        String dataMD5 = DigestUtils.md5Hex(data);

        // API call returns URL and headers
        JsonNode output = apiCallOnObject("upload", MAPPER.valueToTree(new FileUploadRequest(data.length, dataMD5)),
                RetryStrategy.SAFE_TO_RETRY);

        FileUploadResponse apiResponse;
        try {
            apiResponse = MAPPER.treeToValue(output, FileUploadResponse.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        // Check that the content-length received by the apiserver is the same
        // as the length of the data
        if (apiResponse.headers.containsKey("content-length")) {
            int apiserverContentLength = Integer.parseInt(apiResponse.headers.get("content-length"));
            if (apiserverContentLength != data.length) {
                throw new AssertionError(
                        "Content-length received by the apiserver did not match that of the input data");
            }
        }

        // HTTP PUT request to upload URL and headers
        HttpPut request = new HttpPut(apiResponse.url);
        request.setEntity(new ByteArrayEntity(data));

        for (Map.Entry<String, String> header : apiResponse.headers.entrySet()) {
            String key = header.getKey();

            // The request implicitly supplies the content length in the headers
            // when executed
            if (key.equals("content-length")) {
                continue;
            }

            request.setHeader(key, header.getValue());
        }

        HttpClient httpclient = HttpClientBuilder.create().setUserAgent(USER_AGENT).build();
        try {
            httpclient.execute(request);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Uploads data from the specified stream to the file. <b>This implementation buffers the
     * data in-memory before being uploaded to the server; therefore, the data must be small.</b>
     *
     * <p>
     * The file must be in the "open" state. This method assumes exclusive access to the file: the
     * file must have no parts uploaded before this call is made, and no other clients may upload
     * data to the same file concurrently.
     * </p>
     *
     * @param data stream containing data to be uploaded
     */
    public void upload(InputStream data) {
        Preconditions.checkNotNull(data, "data may not be null");
        try {
            this.upload(ByteStreams.toByteArray(data));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
