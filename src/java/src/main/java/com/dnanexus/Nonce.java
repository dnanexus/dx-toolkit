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

import java.security.SecureRandom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Utility class for generating nonces.
 */
public class Nonce {

    public static final SecureRandom random = new SecureRandom();
    final protected static char[] hexArray = "0123456789abcdef".toCharArray();
    public static int counter = 0;

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

    public static String nonce() {
        byte bytes[] = new byte[32];
        random.nextBytes(bytes);
        String nonce = bytesToHex(bytes);
        nonce += String.valueOf(System.currentTimeMillis()) + String.valueOf(counter++);
        return nonce;
    }

    public static Object updateNonce(Object input) {
        JsonNode json = mapper.valueToTree(input);
        ObjectNode inputJson = DXJSON.safeTreeToValue(json, ObjectNode.class);
        if (!inputJson.has("nonce")) {
            inputJson.put("nonce", nonce());
        }
        return inputJson;
    }
}
