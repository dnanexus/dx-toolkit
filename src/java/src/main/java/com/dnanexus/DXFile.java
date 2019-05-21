// Copyright (C) 2013-2016 DNAnexus, Inc.
//
// This file is part of dx-toolkit (DNAnexus platform client libraries).
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain a
// copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package com.dnanexus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
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
import com.google.common.collect.ImmutableList;

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
                try {
                    file.upload(uploadData);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
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
        @JsonIgnoreProperties(ignoreUnknown = true)
        private static class PartValue {
            @JsonProperty
            private String md5;
            @JsonProperty
            private long size;
        }

        @JsonProperty
        private String media;
        @JsonProperty
        private Map<Integer, PartValue> parts;
        @JsonProperty
        private Long size;

        private Describe() {
            super();
        }

        /**
         * Returns the size the specified file part in bytes.
         *
         * @param index part index that was provided to the file upload call
         *
         * @return size of the file part
         *
         * @throws IllegalArgumentException when the index supplied is not a file part
         */
        public Long getChunkSize(int index) {
            try {
                return parts.get(index).size;
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("File part does not exist");
            }
        }

        /**
         * Returns a list of indices corresponding to parts of the file
         *
         * @return list of file part indices
         */
        public List<Integer> getFilePartsList() {
            ArrayList<Integer> fileParts = new ArrayList<>(parts.keySet());
            Collections.sort(fileParts);
            return ImmutableList.copyOf(fileParts);
        }

        /**
         * Returns the hexadecimal encoded value of MD5 message-digest of the specified file part.
         *
         * @param index part index that was provided to the file upload call
         *
         * @return file part md5 digested as a string
         *
         * @throws IllegalArgumentException when the index supplied is not a file part
         */
        public String getMD5(int index) {
            try {
                return parts.get(index).md5;
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("File part does not exist");
            }
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

        /**
         * Returns the size of the file in bytes.
         *
         * @return size of file
         */
        public long getSize() {
            Preconditions.checkState(this.size != null,
                    "file size is not accessible because it was not retrieved with the describe call");
            return size;
        }
    }

    private class FileApiInputStream extends InputStream {
        private FileDownloadResponse apiResponse;
        // The size of the range in the download API request
        private long chunkSize = minDownloadChunkSize;
        // The file position of the first byte in finishQueue that has not yet been returned to the client
        private long currentPos = 0;
        // GET request to download a range of bytes
        private final PartDownloader downloader;
        // The index of the file part
        private int filePartInd = 0;
        private final List<Integer> fileParts;
        // The queue of bytes that has already been checksummed and will be returned to the client
        private Queue<InputStream> finishQueue = new LinkedList<>();
        // The starting byte of the range in the download API request
        private long nextByteFromApi = 0;
        // The total number of unread bytes in the elements of rawFileBytesQueue
        private long numBytesNotYetChecksummed = 0;
        // Contains the list of file parts
        private final Describe partsMetadata;
        // The queue of bytes retrieved from the download API request
        private Queue<InputStream> rawFileBytesQueue = new LinkedList<>();
        // The last byte (exclusive) to be downloaded and returned to the client
        private final long readEnd;
        // The first byte (inclusive) to be downloaded and returned to the client
        private final long readStart;
        // Counter used for ramping
        private int request = 1;

        private FileApiInputStream(long readStart, long readEnd, PartDownloader downloader) {
            // API call returns URL and headers for HTTP GET requests
            JsonNode output = apiCallOnObject("download", MAPPER.valueToTree(new FileDownloadRequest(true)),
                    RetryStrategy.SAFE_TO_RETRY);
            try {
                apiResponse = MAPPER.treeToValue(output, FileDownloadResponse.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            partsMetadata = describe(DXDataObject.DescribeOptions.get().withCustomFields("parts"));

            // Get a sorted list of file parts
            fileParts = partsMetadata.getFilePartsList();

            if (readEnd == -1) {
                readEnd = describe().getSize();
            }
            Preconditions.checkArgument(readEnd >= readStart, "The start byte cannot be larger than the end byte");
            this.readStart = readStart;
            this.readEnd = readEnd;
            this.downloader = downloader;

            // Determine which file part readStart belongs to
            long filePartNext = partsMetadata.getChunkSize(fileParts.get(filePartInd));
            while (readStart >= filePartNext && filePartNext != 0) {
                nextByteFromApi = filePartNext;
                currentPos = nextByteFromApi;
                filePartInd++;
                filePartNext = filePartNext + partsMetadata.getChunkSize(fileParts.get(filePartInd));
            }
        }

        private long getNextChunkSize() {
            if (chunkSize < maxDownloadChunkSize) {
                if (request > numRequestsBetweenRamp) {
                    request = 1;
                    // Ramp up download request size
                    chunkSize = Math.min(chunkSize * ramp, maxDownloadChunkSize);
                }
            }
            request++;
            return chunkSize;
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int numBytesRead = read(b);
            if (numBytesRead != -1) {
                return b[0];
            }
            return -1;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        // The implementation of this method is as follows:
        // API requests are made to download ranges of bytes, which are stored in rawFileBytesQueue,
        // until the number of bytes in rawFileBytesQueue is at least the size of a file part. The
        // bytes inside rawFileBytesQueue are checksummed, and then propagated into finishQueue. The
        // bytes in finishQueue are returned to the client.
        // Bytes propagation in summary:
        // API request -> rawFileBytesQueue -> (checksumming) -> finishQueue -> client
        @Override
        public int read(byte[] b, int off, int numBytes) throws IOException {
            if (off < 0 || numBytes < 0 || numBytes > b.length - off) {
                throw new IndexOutOfBoundsException();
            }

            if (numBytes == 0) {
                return 0;
            }

            while (finishQueue.size() != 0 && finishQueue.peek().available() == 0) {
                finishQueue.remove();
            }

            // Request more data to buffer
            if (finishQueue.size() == 0) {
                if (nextByteFromApi >= readEnd) {
                    return -1;
                }

                long filePartSize = partsMetadata.getChunkSize(fileParts.get(filePartInd));
                if (filePartSize > Integer.MAX_VALUE) {
                    throw new IOException("File part size exceeds 2GB");
                }
                while (numBytesNotYetChecksummed < filePartSize) {
                    long chunkSize = getNextChunkSize();

                    // API request to download bytes
                    long endRange = Math.min(nextByteFromApi + chunkSize, describe().getSize());
                    byte[] bytesFromApiCall = this.downloader.get(apiResponse.url, nextByteFromApi, endRange - 1);

                    // Stream of bytes retrieved from the API call pre-checksum
                    rawFileBytesQueue.add(new ByteArrayInputStream(bytesFromApiCall));
                    numBytesNotYetChecksummed = numBytesNotYetChecksummed + bytesFromApiCall.length;

                    // Update the next starting byte range
                    nextByteFromApi = endRange;
                }
                assert (numBytesNotYetChecksummed >= filePartSize);

                // Checksum verification
                // Multiple file parts may need to be checksummed
                while (numBytesNotYetChecksummed >= filePartSize) {
                    // File part's MD5 stored in file's meta data
                    String metadataChecksum = partsMetadata.getMD5(fileParts.get(filePartInd));
                    byte[] checksumBytes = new byte[(int)filePartSize];
                    int checksumOff = 0;
                    while (checksumOff < checksumBytes.length) {
                        InputStream rawFileStream = rawFileBytesQueue.peek();
                        int bytesReadForChecksum =
                                rawFileStream.read(checksumBytes, checksumOff, (int)filePartSize - checksumOff);
                        if (bytesReadForChecksum == -1) {
                            rawFileBytesQueue.remove();
                        } else {
                            checksumOff += bytesReadForChecksum;
                        }
                    }
                    numBytesNotYetChecksummed = numBytesNotYetChecksummed - filePartSize;

                    // Verify that MD5 of the downloaded data is the same as the MD5 in the metadata
                    if (!DigestUtils.md5Hex(checksumBytes).equals(metadataChecksum)) {
                        throw new IOException("Checksumming failed with file part " + fileParts.get(filePartInd));
                    }

                    // The most recently checksummed bytes is pushed into queue
                    finishQueue.add(new ByteArrayInputStream(checksumBytes));

                    filePartInd++;

                    if (filePartInd >= fileParts.size()) {
                        // Read past end of file
                        break;
                    }
                    filePartSize = partsMetadata.getChunkSize(fileParts.get(filePartInd));
                }
            }

            // Return bytes to caller
            int bytesToRead;

            // Skips bytes that occur before readStart
            InputStream checksumBuffer = finishQueue.peek();
            if (readStart > currentPos) {
                long skip = checksumBuffer.skip(readStart - currentPos);
                assert (skip == readStart - currentPos);
                currentPos = readStart;
            }
            long endByteChecksumBuffer = currentPos + checksumBuffer.available();

            // Makes sure that only bytes up to readEnd are returned
            if (readEnd < endByteChecksumBuffer) {
                if (checksumBuffer.available() == (int) (endByteChecksumBuffer - readEnd)) {
                    return -1;
                }
                bytesToRead = Math.min(numBytes, checksumBuffer.available() - (int) (endByteChecksumBuffer - readEnd));
            } else {
                bytesToRead = Math.min(numBytes, finishQueue.peek().available());
            }
            int bytesRead = checksumBuffer.read(b, off, bytesToRead);
            assert (bytesToRead == bytesRead);
            currentPos = currentPos + bytesRead;

            return bytesRead;
        }
    }

    private class FileApiOutputStream extends OutputStream {
        private int index = 1;
        private ByteArrayOutputStream unwrittenBytes = new ByteArrayOutputStream();

        @Override
        public void close() throws IOException {
            // Flush out remaining bytes to upload
            partUploadRequest(unwrittenBytes.toByteArray(), index);
            unwrittenBytes = new ByteArrayOutputStream();
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int numBytes) throws IOException {
            unwrittenBytes.write(b, off, numBytes);
            if (unwrittenBytes.size() >= uploadChunkSize) {
                byte[] bytesToWrite = unwrittenBytes.toByteArray();
                int chunkStart = 0;
                while (bytesToWrite.length - chunkStart >= uploadChunkSize) {
                    partUploadRequest(Arrays.copyOfRange(bytesToWrite, chunkStart, chunkStart + uploadChunkSize),
                            index);
                    chunkStart += uploadChunkSize;
                    index++;
                }
                unwrittenBytes = new ByteArrayOutputStream();
                IOUtils.write(Arrays.copyOfRange(bytesToWrite, chunkStart, bytesToWrite.length), unwrittenBytes);
            }
        }

        @Override
        public void write(int b) throws IOException {
            byte[] byteAsArray = new byte[1];
            byteAsArray[0] = (byte) b;
            write(byteAsArray);
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
        private Map<String, String> headers;
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
        private int index = 1;
        @JsonProperty
        private String md5;
        @JsonProperty
        private int size;

        private FileUploadRequest(int size, String md5, int index) {
            this.size = size;
            this.md5 = md5;
            this.index = index;
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

    private static class HttpPartDownloader implements PartDownloader {
        /**
         * HTTP GET request to download part of the file.
         *
         * @param url URL to which an HTTP GET request is made to download the file
         * @param chunkStart beginning of the part (in the byte array containing the file contents) to
         *        be downloaded. This index is inclusive in the range.
         * @param chunkEnd end of the part (in the byte array containing the file contents) to be
         *        downloaded. This index is inclusive in the range.
         *
         * @return byte array containing the part of the file contents that is downloaded
         *
         * @throws ClientProtocolException HTTP request to the download URL cannot be executed
         * @throws IOException unable to get file contents from HTTP response
         */
        @Override
        public byte[] get(String url, long start, long end) throws ClientProtocolException, IOException {
            Preconditions.checkState(end - start <= (long) 2 * 1024 * 1024 * 1024,
                    "Download chunk size cannot be larger than 2GB");
            HttpClient httpclient = HttpClientBuilder.create().setUserAgent(USER_AGENT).build();

            // HTTP GET request with bytes/_ge range header
            HttpGet request = new HttpGet(url);
            request.addHeader("Range", "bytes=" + start + "-" + end);

            HttpResponse response = executeRequestWithRetry(httpclient, request);
            InputStream content = response.getEntity().getContent();

            return IOUtils.toByteArray(content);
        }
    }

    @VisibleForTesting
    interface PartDownloader {
        byte[] get(String url, long start, long end) throws ClientProtocolException, IOException;
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
     * Executes HTTP Request with retry logic
     *
     * @param httpclient
     * @param request HttpGet, HttpPost, or HttpPut request
     *
     * @return response to the HTTP Request
     *
     * @throws IOException
     */
    private static HttpResponse executeRequestWithRetry(HttpClient httpclient, HttpRequestBase request)
            throws IOException {
        HttpResponse response;
        int retryAttempts = 1;
        final int maxRetryAttempts = 5;
        int timeoutSeconds = 1;
        while (true) {
            try {
                response = httpclient.execute(request);
            } catch (NoHttpResponseException e) {
                retryAttempts++;
                if (retryAttempts > maxRetryAttempts) {
                    throw e;
                }
                System.out.println("HTTP error (no response). Waiting " + timeoutSeconds + " second(s) before retrying...");
                sleep(timeoutSeconds);
                timeoutSeconds *= 2;
                continue;
            }

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK && statusCode != HttpStatus.SC_PARTIAL_CONTENT) {
                retryAttempts++;
                if (retryAttempts > maxRetryAttempts) {
                    throw new IOException("HTTP error (status code " + statusCode + ")");
                }
                System.out.println("HTTP error (status code " + statusCode + "). Waiting " + timeoutSeconds + " second(s) before retrying...");
                sleep(timeoutSeconds);
                timeoutSeconds *= 2;
                continue;
            }

            return response;
        }
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
    static DXFile getInstanceWithCachedDescribe(String fileId, DXContainer project, DXEnvironment env,
            JsonNode describe) {
        return new DXFile(fileId, project, Preconditions.checkNotNull(env, "env may not be null"),
                Preconditions.checkNotNull(describe, "describe may not be null"));
    }

    /**
     * Returns a {@code DXFile} associated with an existing file in a particular project using the
     * specified environment.
     *
     * @throws NullPointerException If {@code fileId} or {@code container} is null
     */
    public static DXFile getInstanceWithEnvironment(String fileId, DXContainer project, DXEnvironment env) {
        return new DXFile(fileId, project, Preconditions.checkNotNull(env, "env may not be null"), null);
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

    /**
     * Sleeps for the specified amount of time. Throws a {@link RuntimeException} if interrupted.
     *
     * @param seconds number of seconds to sleep for
     */
    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Variables for download
    private final int maxDownloadChunkSize = 16 * 1024 * 1024;
    private final int minDownloadChunkSize = 64 * 1024;
    private final int numRequestsBetweenRamp = 4;

    // Ramp up factor for downloading by parts
    private final int ramp = 2;

    // Variables for upload
    // Upload chunk size must not exceed 2GB
    @VisibleForTesting
    int uploadChunkSize = 16 * 1024 * 1024;

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
        return DXJSON.safeTreeToValue(apiCallOnObject("describe", RetryStrategy.SAFE_TO_RETRY), Describe.class);
    }

    @Override
    public Describe describe(DXDataObject.DescribeOptions options) {
        return DXJSON.safeTreeToValue(
                apiCallOnObject("describe", MAPPER.valueToTree(options), RetryStrategy.SAFE_TO_RETRY), Describe.class);
    }

    /**
     * Downloads the entire file into a byte array.
     *
     * @return byte array containing file contents
     * @throws IOException if an error occurs while downloading the data
     */
    public byte[] downloadBytes() throws IOException {
        // -1 indicates the end of the file
        return downloadBytes(0, -1);
    }

    /**
     * Downloads the specified byte range of the file into a byte array. Range requested must be no
     * larger than 2 GB.
     *
     * @param start first byte of the range within the file to be downloaded. The start byte is
     *        inclusive in the range, and 0 is indexed as the first byte in the file.
     * @param end last byte of the range within the file to be downloaded. The end byte is exclusive
     *        (not included in the range). An input of -1 specifies the end of the file.
     *
     * @return byte array containing file contents within range specified
     * @throws IOException if an error occurs while downloading the data
     */
    public byte[] downloadBytes(long start, long end) throws IOException {
        Preconditions.checkState(end - start <= (long) 2 * 1024 * 1024 * 1024,
                "Range of file larger than 2GB cannot be downloaded with downloadBytes");
        InputStream is = getDownloadStream(start, end);

        return IOUtils.toByteArray(is);
    }

    /**
     * Downloads the entire file and writes the data to an OutputStream.
     *
     * @param os output stream downloaded file contents are written into
     * @throws IOException
     */
    public void downloadToOutputStream(OutputStream os) throws IOException {
        downloadToOutputStream(os, 0, describe().getSize());
    }

    /**
     * Downloads the specified byte range of the file into an OutputStream.
     *
     * @param os output stream downloaded file contents are written into
     * @param start first byte of the range within the file to be downloaded. The start byte is
     *        inclusive in the range, and 0 is indexed as the first byte in the file.
     * @param end last byte of the range within the file to be downloaded. The end byte is exclusive
     *        (not included in the range). An input of -1 specifies the end of the file.
     *
     * @throws IOException
     */
    public void downloadToOutputStream(OutputStream os, long start, long end) throws IOException {
        InputStream is = getDownloadStream(start, end);
        IOUtils.copyLarge(is, os);
    }

    @Override
    public Describe getCachedDescribe() {
        this.checkCachedDescribeAvailable();
        return DXJSON.safeTreeToValue(this.cachedDescribe, Describe.class);
    }

    /**
     * Returns a stream of the file's contents.
     *
     * @return stream containing file contents
     */
    public InputStream getDownloadStream() {
        // -1 indicates the end of the file
        return getDownloadStream(0, -1);
    }

    /**
     * Returns a stream of the specified byte range of the file's contents.
     *
     * @param start first byte of the range within the file to be downloaded. The start byte is
     *        inclusive in the range, and 0 is indexed as the first byte in the file.
     * @param end last byte of the range within the file to be downloaded. The end byte is exclusive
     *        (not included in the range). An input of -1 specifies the end of the file.
     *
     * @return stream containing file contents within range specified
     */
    public InputStream getDownloadStream(long start, long end) {
        return new FileApiInputStream(start, end, new HttpPartDownloader());
    }

    @VisibleForTesting
    InputStream getDownloadStream(long start, long end, PartDownloader downloader) {
        return new FileApiInputStream(start, end, downloader);
    }

    @VisibleForTesting
    InputStream getDownloadStream(PartDownloader downloader) {
        return getDownloadStream(0, -1, downloader);
    }


    /**
     * Returns an OutputStream that uploads any data written to it
     * <p>
     * The file must be in the "open" state. This method assumes exclusive access to the file: the
     * file must have no parts uploaded before this call is made, and no other clients may upload
     * data to the same file concurrently.
     * </p>
     *
     * @return OutputStream to which file contents are written
     */
    public OutputStream getUploadStream() {
        return new FileApiOutputStream();
    }

    /**
     * HTTP PUT request to upload the data part to the server.
     *
     * @param dataChunk data part that is uploaded
     * @param index position for which the data lies in the file
     * @throws IOException if unable to execute HTTP request
     */
    private void partUploadRequest(byte[] dataChunk, int index) throws IOException {
        // MD5 digest as 32 character hex string
        String dataMD5 = DigestUtils.md5Hex(dataChunk);

        // API call returns URL and headers
        JsonNode output =
                apiCallOnObject("upload", MAPPER.valueToTree(new FileUploadRequest(dataChunk.length, dataMD5, index)),
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
            if (apiserverContentLength != dataChunk.length) {
                throw new AssertionError(
                        "Content-length received by the apiserver did not match that of the input data");
            }
        }

        // HTTP PUT request to upload URL and headers
        HttpPut request = new HttpPut(apiResponse.url);
        request.setEntity(new ByteArrayEntity(dataChunk));

        // Set headers
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
        executeRequestWithRetry(httpclient, request);
    }

    /**
     * Uploads data from the specified byte array to the file.
     *
     * <p>
     * The file must be in the "open" state. This method assumes exclusive access to the file: the
     * file must have no parts uploaded before this call is made, and no other clients may upload
     * data to the same file concurrently.
     * </p>
     *
     * @param data data in bytes to be uploaded
     *
     * @throws IOException if an error occurs while uploading the data
     */
    public void upload(byte[] data) throws IOException {
        Preconditions.checkNotNull(data, "data may not be null");
        try (OutputStream uploadOutputStream = this.getUploadStream()) {
            IOUtils.write(data, uploadOutputStream);
        }
    }

    /**
     * Uploads data from the specified stream to the file.
     *
     * <p>
     * The file must be in the "open" state. This method assumes exclusive access to the file: the
     * file must have no parts uploaded before this call is made, and no other clients may upload
     * data to the same file concurrently.
     * </p>
     *
     * @param data stream containing data to be uploaded
     *
     * @throws IOException if an error occurs while uploading the data
     */
    public void upload(InputStream data) throws IOException {
        Preconditions.checkNotNull(data, "data may not be null");
        try (OutputStream uploadOutputStream = this.getUploadStream()) {
            IOUtils.copyLarge(data, uploadOutputStream);
        }
    }
}
