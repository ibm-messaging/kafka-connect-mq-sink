/**
 * Copyright 2026 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsink.builders;

/**
 * Utility methods for hex string conversion.
 *
 * <p>This class exists as a polyfill for {@link java.util.HexFormat}, which was introduced
 * in Java 17. It can be retired once this project's minimum Java version is raised to 17
 * or above.
 *
 * <p>TODO: Replace {@link #hexStringToBytes(String)} with {@code HexFormat.of().parseHex(hex)}
 * and delete this class when the project upgrades to Java 17+.
 */
public final class HexUtils {

    private HexUtils() {}

    /**
     * Decodes a lowercase or uppercase hex string into a byte array.
     *
     * @param hex an even-length string of hexadecimal characters
     * @return the decoded bytes
     * @throws IllegalArgumentException if the string has odd length or contains non-hex characters
     */
    public static byte[] hexStringToBytes(final String hex) {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("Invalid hex string (odd length): '" + hex + "'");
        }
        final byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            final int high = Character.digit(hex.charAt(i), 16);
            final int low  = Character.digit(hex.charAt(i + 1), 16);
            if (high == -1 || low == -1) {
                throw new IllegalArgumentException("Invalid hex character at position " + i + " in '" + hex + "'");
            }
            bytes[i / 2] = (byte) ((high << 4) | low);
        }
        return bytes;
    }
}
