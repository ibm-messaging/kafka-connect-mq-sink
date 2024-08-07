/**
 * Copyright 2022, 2023, 2024 IBM Corporation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class DefaultMessageBuilderIT extends AbstractJMSContextIT {

    private MessageBuilder builder;

    @Before
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
