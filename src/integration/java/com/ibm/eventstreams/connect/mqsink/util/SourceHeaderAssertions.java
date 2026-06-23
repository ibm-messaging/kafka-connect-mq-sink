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
package com.ibm.eventstreams.connect.mqsink.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

import com.ibm.msg.client.jms.JmsConstants;

/**
 * Asserts that a sink-built JMS message reflects Kafka headers as produced by the MQ source connector.
 */
public final class SourceHeaderAssertions {

    private static final Set<String> SKIPPED_BY_SINK = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT
    ));

    private static final Set<String> MQMD_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_REPORT,
            JmsConstants.JMS_IBM_MQMD_MSGTYPE,
            JmsConstants.JMS_IBM_MQMD_EXPIRY,
            JmsConstants.JMS_IBM_MQMD_FEEDBACK,
            JmsConstants.JMS_IBM_MQMD_ENCODING,
            JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID,
            JmsConstants.JMS_IBM_MQMD_PRIORITY,
            JmsConstants.JMS_IBM_MQMD_PERSISTENCE,
            JmsConstants.JMS_IBM_MQMD_PUTAPPLTYPE,
            JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER,
            JmsConstants.JMS_IBM_MQMD_OFFSET,
            JmsConstants.JMS_IBM_MQMD_MSGFLAGS,
            JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH
    ));

    private static final Set<String> MQMD_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_FORMAT,
            JmsConstants.JMS_IBM_MQMD_REPLYTOQ,
            JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR,
            JmsConstants.JMS_IBM_MQMD_USERIDENTIFIER,
            JmsConstants.JMS_IBM_MQMD_APPLIDENTITYDATA,
            JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME,
            JmsConstants.JMS_IBM_MQMD_PUTDATE,
            JmsConstants.JMS_IBM_MQMD_PUTTIME,
            JmsConstants.JMS_IBM_MQMD_APPLORIGINDATA
    ));

    private static final Set<String> MQMD_BYTES_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_CORRELID,
            JmsConstants.JMS_IBM_MQMD_MSGID,
            JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN,
            JmsConstants.JMS_IBM_MQMD_GROUPID
    ));

    private static final Set<String> JMS_IBM_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_REPORT_EXCEPTION,
            JmsConstants.JMS_IBM_REPORT_EXPIRATION,
            JmsConstants.JMS_IBM_REPORT_COA,
            JmsConstants.JMS_IBM_REPORT_COD,
            JmsConstants.JMS_IBM_REPORT_PAN,
            JmsConstants.JMS_IBM_REPORT_NAN,
            JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID,
            JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID,
            JmsConstants.JMS_IBM_REPORT_DISCARD_MSG,
            JmsConstants.JMS_IBM_MSGTYPE,
            JmsConstants.JMS_IBM_FEEDBACK,
            JmsConstants.JMS_IBM_ENCODING,
            JmsConstants.JMS_IBM_PUTAPPLTYPE
    ));

    private static final Set<String> JMS_IBM_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_FORMAT,
            JmsConstants.JMS_IBM_CHARACTER_SET
    ));

    private static final Set<String> JMS_IBM_BOOLEAN_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP
    ));

    private static final HexFormat HEX = HexFormat.of();

    private SourceHeaderAssertions() {
    }

    public static void assertSinkMatchesSourceHeaders(final Message sinkMessage, final Headers sourceHeaders,
            final boolean mqmdWriteEnabled) throws JMSException {
        final Iterator<Header> iterator = sourceHeaders.iterator();
        while (iterator.hasNext()) {
            final Header header = iterator.next();
            assertHeaderApplied(sinkMessage, header, mqmdWriteEnabled);
        }
    }

    /**
     * Asserts only the given header keys from the source record, ignoring system headers
     * (e.g. {@code JMSXUserID}) that MQ may add when the source task reads the message.
     */
    public static void assertSinkMatchesSourceHeaders(final Message sinkMessage, final Headers sourceHeaders,
            final boolean mqmdWriteEnabled, final Collection<String> headerKeys) throws JMSException {
        for (final String key : headerKeys) {
            final Header header = sourceHeaders.lastWithName(key);
            assertThat(header)
                    .as("source record should contain header '%s'", key)
                    .isNotNull();
            assertHeaderApplied(sinkMessage, header, mqmdWriteEnabled);
        }
    }

    private static void assertHeaderApplied(final Message sinkMessage, final Header header,
            final boolean mqmdWriteEnabled) throws JMSException {
        final String key = header.key();
        final Object value = header.value();

        if (SKIPPED_BY_SINK.contains(key)) {
            return;
        }

        if (!mqmdWriteEnabled && isMqmdProperty(key)) {
            return;
        }

        if (value == null) {
            assertThat(sinkMessage.propertyExists(key))
                    .as("header '%s': property should exist with null value", key)
                    .isTrue();
            assertThat(sinkMessage.getObjectProperty(key))
                    .as("header '%s': property value should be null", key)
                    .isNull();
            return;
        }

        if (JmsConstants.JMS_IBM_MQMD_CORRELID.equals(key)) {
            assertThat(sinkMessage.getJMSCorrelationID())
                    .as("header '%s': JMSCorrelationID", key)
                    .isEqualTo(value.toString());
            return;
        }

        if (JmsConstants.JMS_IBM_MQMD_MSGID.equals(key)) {
            assertMsgId(sinkMessage, header);
            return;
        }

        if (header.schema() != null && header.schema().type() == Schema.Type.BYTES && value instanceof byte[]) {
            assertByteArrayProperty(sinkMessage, key, (byte[]) value);
            return;
        }

        if (MQMD_INTEGER_PROPERTIES.contains(key) || JMS_IBM_INTEGER_PROPERTIES.contains(key)) {
            final int expected = Integer.parseInt(value.toString());
            assertThat(sinkMessage.getIntProperty(key))
                    .as("header '%s': int property", key)
                    .isEqualTo(expected);
            return;
        }

        if (JMS_IBM_BOOLEAN_PROPERTIES.contains(key)) {
            assertBooleanProperty(sinkMessage, key, value.toString());
            return;
        }

        if (MQMD_STRING_PROPERTIES.contains(key) || JMS_IBM_STRING_PROPERTIES.contains(key)) {
            assertThat(sinkMessage.getStringProperty(key))
                    .as("header '%s': string property", key)
                    .isEqualTo(value.toString());
            return;
        }

        if (MQMD_BYTES_PROPERTIES.contains(key)) {
            assertByteArrayProperty(sinkMessage, key, decodeBytes(value));
            return;
        }

        assertThat(sinkMessage.getStringProperty(key))
                .as("header '%s': default string property", key)
                .isEqualTo(value.toString());
    }

    private static void assertMsgId(final Message sinkMessage, final Header header) throws JMSException {
        final String key = header.key();
        final Object value = header.value();
        if (value instanceof byte[]) {
            assertByteArrayProperty(sinkMessage, key, (byte[]) value);
            return;
        }

        String hexString = value.toString();
        if (hexString.startsWith("ID:")) {
            hexString = hexString.substring(3);
        }
        final byte[] expected = HexUtils.parseHex(hexString);
        final Object actual = sinkMessage.getObjectProperty(key);
        assertThat(actual)
                .as("header '%s': MSGID property should be byte[]", key)
                .isInstanceOf(byte[].class);
        assertThat((byte[]) actual)
                .as("header '%s': MSGID bytes (expected %s)", key, formatBytes(expected))
                .isEqualTo(expected);
    }

    private static void assertBooleanProperty(final Message sinkMessage, final String key, final String value)
            throws JMSException {
        if ("true".equalsIgnoreCase(value) || "1".equals(value)) {
            assertThat(sinkMessage.getBooleanProperty(key))
                    .as("header '%s': boolean property (expected true)", key)
                    .isTrue();
        } else if ("false".equalsIgnoreCase(value) || "0".equals(value)) {
            assertThat(sinkMessage.getBooleanProperty(key))
                    .as("header '%s': boolean property (expected false)", key)
                    .isFalse();
        } else {
            throw new AssertionError(String.format(
                    "header '%s': unexpected boolean value '%s' (expected true/false or 1/0)", key, value));
        }
    }

    private static void assertByteArrayProperty(final Message sinkMessage, final String key, final byte[] expected)
            throws JMSException {
        final Object actual = sinkMessage.getObjectProperty(key);
        assertThat(actual)
                .as("header '%s': property should be byte[] (expected %s)", key, formatBytes(expected))
                .isInstanceOf(byte[].class);
        assertThat((byte[]) actual)
                .as("header '%s': byte[] property (expected %s)", key, formatBytes(expected))
                .isEqualTo(expected);
    }

    private static String formatBytes(final byte[] bytes) {
        return bytes == null ? "null" : HEX.formatHex(bytes);
    }

    private static byte[] decodeBytes(final Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        return java.util.Base64.getDecoder().decode(value.toString());
    }

    private static boolean isMqmdProperty(final String key) {
        return MQMD_INTEGER_PROPERTIES.contains(key)
                || MQMD_STRING_PROPERTIES.contains(key)
                || MQMD_BYTES_PROPERTIES.contains(key);
    }
}
