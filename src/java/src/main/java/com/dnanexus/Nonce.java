// Copyright (C) 2016 DNAnexus, Inc.
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

import java.security.SecureRandom;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
/**
 * Utility class for generating nonces.
 */
class Nonce {

    // Prevent instantiation of utility class
    private Nonce() {
    }

    private static final SecureRandom random = new SecureRandom();
    final private static char[] hexArray = "0123456789abcdef".toCharArray();
    private static int counter = 0;

    private static ObjectMapper mapper = new ObjectMapper();

    // From
    // http://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];

        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Generates a unique sequence of bytes and concatenates their hexadecimal
     * representation with the current time and a local counter to make a unique nonce.
     * Returns the unique nonce as a string.
     *
     * @return String
     */
    @VisibleForTesting
    static String nonce() {
        byte bytes[] = new byte[32];
        random.nextBytes(bytes);
        String nonce = bytesToHex(bytes);
        nonce += String.valueOf(System.currentTimeMillis()) + String.valueOf(counter++);
        return nonce;
    }

    /**
     * Returns a copy of an input object with an additional nonce field.
     *
     * @param input a JsonNode object
     *
     * @return a Copy of the given JsonNode containing a nonce.
     */
    public static JsonNode updateNonce(JsonNode input) {
        ObjectNode inputJson = (ObjectNode)(input.deepCopy());
        if (!inputJson.has("nonce")) {
            inputJson.put("nonce", nonce());
        }
        return inputJson;
    }
}
