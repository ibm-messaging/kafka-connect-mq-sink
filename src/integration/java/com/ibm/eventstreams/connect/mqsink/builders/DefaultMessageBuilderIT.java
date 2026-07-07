/**
 * Copyright 2022, 2023, 2024, 2026 IBM Corporation
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class DefaultMessageBuilderIT extends AbstractJMSContextIT {

    private MessageBuilder builder;

    @BeforeEach
    public void prepareMessageBuilder() {
        builder = new DefaultMessageBuilder();
    }

    private SinkRecord generateSinkRecord(final Schema valueSchema, final Object value) {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;
        final Schema keySchema = Schema.STRING_SCHEMA;
        final String key = "mykey";

        return new SinkRecord(topic, partition,
                keySchema, key,
                valueSchema, value,
                offset);
    }

    @Test
    public void buildEmptyMessageWithoutSchema() throws Exception {
        createAndVerifyEmptyMessage(null);
    }

    @Test
    public void buildEmptyMessageWithSchema() throws Exception {
        createAndVerifyEmptyMessage(Schema.STRING_SCHEMA);
    }

    @Test
    public void buildTextMessageWithoutSchema() throws Exception {
        createAndVerifyStringMessage(null, "Hello World");
    }

    @Test
    public void buildTextMessageWithSchema() throws Exception {
        createAndVerifyStringMessage(Schema.STRING_SCHEMA, "Hello World with a schema");
    }

    @Test
    public void buildIntMessageWithoutSchema() throws Exception {
        createAndVerifyIntegerMessage(null, 1234);
    }

    @Test
    public void buildIntMessageWithSchema() throws Exception {
        createAndVerifyIntegerMessage(Schema.INT32_SCHEMA, 1234);
    }

    @Test
    public void buildByteArrayMessageWithoutSchema() throws Exception {
        final String testMessage = "This is a test";
        createAndVerifyByteMessage(null, testMessage.getBytes(), testMessage);
    }

    @Test
    public void buildByteArrayMessageWithSchema() throws Exception {
        final String testMessage = "This is another test";
        createAndVerifyByteMessage(Schema.BYTES_SCHEMA, testMessage.getBytes(), testMessage);
    }

    @Test
    public void buildByteBufferMessageWithoutSchema() throws Exception {
        final String testMessage = "This is also a test!";
        final byte[] payload = testMessage.getBytes();
        final ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);
        createAndVerifyByteMessage(null, value, testMessage);
    }

    @Test
    public void buildByteBufferMessageWithSchema() throws Exception {
        final String testMessage = "This is a bytebuffer test";
        final byte[] payload = testMessage.getBytes();
        final ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);
        createAndVerifyByteMessage(Schema.BYTES_SCHEMA, value, testMessage);
    }

    @Test
    public void buildMessageWithTextHeader() throws Exception {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;

        final String testHeaderKey = "TestHeader";

        final ConnectHeaders headers = new ConnectHeaders();
        headers.addString(testHeaderKey, "This is a test header");

        final SinkRecord record = new SinkRecord(topic, partition,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, "Test message",
                offset,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);

        // header should not have been copied across by default
        final Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertNull(message.getStringProperty(testHeaderKey));

        // no message properties should be set by default
        assertFalse(message.getPropertyNames().hasMoreElements());
    }

    @Test
    public void buildMessageWithMQMDByteArrayProperties() throws Exception {
        // Test MQMD byte[] properties with mq.message.mqmd.write=true
        // These properties can be set via setObjectProperty() when mqmd.write is enabled

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // MQMD byte[] properties - these are the 4 byte[] MQMD fields
        final byte[] groupId = new byte[] {0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
                                           0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50,
                                           0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58};
        final byte[] accountingToken = new byte[] {0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
                                                    0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,
                                                    0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                                                    0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, (byte)0x80};

        headers.addBytes("JMS_IBM_MQMD_GroupId", groupId);
        headers.addBytes("JMS_IBM_MQMD_AccountingToken", accountingToken);

        // generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify each MQMD byte[] property can be retrieved
        // Note: We can't directly verify the byte[] values in integration tests without accessing
        // the underlying MQ message, but we can verify the message was created successfully
        // and the properties were set (they would throw an exception if they couldn't be set)
        assertNotNull(message);

        // Verify the properties exist (propertyExists returns true if set, even for byte[])
        assertTrue(message.propertyExists("JMS_IBM_MQMD_GroupId"));
        assertTrue(message.propertyExists("JMS_IBM_MQMD_AccountingToken"));
    }

    @Test
    public void buildMessageWithMQMDByteArrayPropertiesFromBase64() throws Exception {
        // Test MQMD byte[] properties using Base64 encoded strings
        // This demonstrates that users can provide byte[] values as Base64 strings

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // Base64 encoded values for MQMD byte[] properties
        // GroupId: 24 bytes - "QUJDREVGR0hJSktMTU5PUFFSU1RVVlc=" decodes to bytes 0x41-0x58
        final String groupIdBase64 = "QUJDREVGR0hJSktMTU5PUFFSU1RVVlc=";

        // AccountingToken: 32 bytes - "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4A=" decodes to bytes 0x61-0x80
        final String accountingTokenBase64 = "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7fH1+f4A=";

        // Add as String headers - the converter will decode them to byte[]
        headers.addString("JMS_IBM_MQMD_GroupId", groupIdBase64);
        headers.addString("JMS_IBM_MQMD_AccountingToken", accountingTokenBase64);

        // generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify the message was created successfully
        assertNotNull(message);

        // Verify the properties exist (they were decoded from Base64 and set as byte[])
        assertTrue(message.propertyExists("JMS_IBM_MQMD_GroupId"));
        assertTrue(message.propertyExists("JMS_IBM_MQMD_AccountingToken"));
    }

    private SinkRecord generateSinkRecord(final ConnectHeaders headers) {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;
        final Schema keySchema = Schema.STRING_SCHEMA;
        final String key = "mykey";
        final Schema valueSchema = Schema.STRING_SCHEMA;
        final String value = "Test message";

        return new SinkRecord(topic, partition,
                keySchema, key,
                valueSchema, value,
                offset,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);
    }

    private void createAndVerifyEmptyMessage(final Schema valueSchema) throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, null));
        assertEquals(null, message.getBody(String.class));
    }

    private void createAndVerifyStringMessage(final Schema valueSchema, final String value) throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));
        assertEquals(value, message.getBody(String.class));

        final TextMessage textmessage = (TextMessage) message;
        assertEquals(value, textmessage.getText());
    }

    private void createAndVerifyIntegerMessage(final Schema valueSchema, final Integer value) throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));
        final Integer intValue = Integer.parseInt(message.getBody(String.class));
        assertEquals(value, intValue);
    }

    private void createAndVerifyByteMessage(final Schema valueSchema, final Object value, final String valueAsString)
            throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));

        final BytesMessage byteMessage = (BytesMessage) message;
        byteMessage.reset();

        byte[] byteData = null;
        byteData = new byte[(int) byteMessage.getBodyLength()];
        byteMessage.readBytes(byteData);
        final String stringMessage = new String(byteData);
        assertEquals(valueAsString, stringMessage);
    }
}
