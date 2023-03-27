/**
 * Copyright 2022 IBM Corporation
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

    private SinkRecord generateSinkRecord(Schema valueSchema, Object value) {
        final String TOPIC = "TOPIC.NAME";
        final int PARTITION = 0;
        final long OFFSET = 0;
        final Schema KEY_SCHEMA = Schema.STRING_SCHEMA;
        final String KEY = "mykey";

        return new SinkRecord(TOPIC, PARTITION,
                              KEY_SCHEMA, KEY,
                              valueSchema, value,
                              OFFSET);
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
        String TEST_MESSAGE = "This is a test";
        createAndVerifyByteMessage(null, TEST_MESSAGE.getBytes(), TEST_MESSAGE);
    }
    @Test
    public void buildByteArrayMessageWithSchema() throws Exception {
        String TEST_MESSAGE = "This is another test";
        createAndVerifyByteMessage(Schema.BYTES_SCHEMA, TEST_MESSAGE.getBytes(), TEST_MESSAGE);
    }
    @Test
    public void buildByteBufferMessageWithoutSchema() throws Exception {
        String TEST_MESSAGE = "This is also a test!";
        byte[] payload = TEST_MESSAGE.getBytes();
        ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);
        createAndVerifyByteMessage(null, value, TEST_MESSAGE);
    }
    @Test
    public void buildByteBufferMessageWithSchema() throws Exception {
        String TEST_MESSAGE = "This is a bytebuffer test";
        byte[] payload = TEST_MESSAGE.getBytes();
        ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);
        createAndVerifyByteMessage(Schema.BYTES_SCHEMA, value, TEST_MESSAGE);
    }
    
    @Test
    public void buildMessageWithTextHeader() throws Exception {
        final String TOPIC = "TOPIC.NAME";
        final int PARTITION = 0;
        final long OFFSET = 0;
        
        final String TEST_HEADER_KEY = "TestHeader";
        
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString(TEST_HEADER_KEY, "This is a test header");

        SinkRecord record = new SinkRecord(TOPIC, PARTITION,
                                           Schema.STRING_SCHEMA, "mykey",
                                           Schema.STRING_SCHEMA, "Test message",
                                           OFFSET, 
                                           null, TimestampType.NO_TIMESTAMP_TYPE,
                                           headers);
        
        // header should not have been copied across by default
        Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertNull(message.getStringProperty(TEST_HEADER_KEY));
        
        // no message properties should be set by default
        assertFalse(message.getPropertyNames().hasMoreElements());
    }


    private void createAndVerifyEmptyMessage(Schema valueSchema) throws Exception {
        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, null));
        assertEquals(null, message.getBody(String.class));
    }

    private void createAndVerifyStringMessage(Schema valueSchema, String value) throws Exception {
        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));
        assertEquals(value, message.getBody(String.class));

        TextMessage textmessage = (TextMessage) message;
        assertEquals(value, textmessage.getText());
    }

    private void createAndVerifyIntegerMessage(Schema valueSchema, Integer value) throws Exception {
        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));
        Integer intValue = Integer.parseInt(message.getBody(String.class));
        assertEquals(value, intValue);
    }

    private void createAndVerifyByteMessage(Schema valueSchema, Object value, String valueAsString) throws Exception {
        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(valueSchema, value));

        BytesMessage byteMessage = (BytesMessage) message;
        byteMessage.reset();

        byte[] byteData = null;
        byteData = new byte[(int) byteMessage.getBodyLength()];
        byteMessage.readBytes(byteData);
        String stringMessage =  new String(byteData);
        assertEquals(valueAsString, stringMessage);
    }
}
