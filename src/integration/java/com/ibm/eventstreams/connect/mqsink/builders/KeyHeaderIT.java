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
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class KeyHeaderIT extends AbstractJMSContextIT {

    private SinkRecord generateSinkRecord(Schema keySchema, Object keyValue) {
        final String TOPIC = "TOPIC.NAME";
        final int PARTITION = 0;
        final long OFFSET = 0;
        final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;
        final String VALUE = "message payload";

        return new SinkRecord(TOPIC, PARTITION,
                              keySchema, keyValue,
                              VALUE_SCHEMA, VALUE,
                              OFFSET);
    }


    @Test
    public void verifyUnsupportedKeyHeader() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.key.header", "unsupported");

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        ConnectException exc = assertThrows(ConnectException.class, () -> {
            builder.configure(props);
        });
        assertEquals("Unsupported MQ message builder key header value", exc.getMessage());
    }

    @Test
    public void buildStringKeyHeaderWithoutSchema() throws Exception {
        createAndVerifyStringKeyHeader(null, "my-message-key");
    }

    @Test
    public void buildStringKeyHeaderWithSchema() throws Exception {
        createAndVerifyStringKeyHeader(Schema.STRING_SCHEMA, "my-message-key");
    }

    @Test
    public void buildByteArrayKeyHeaderWithoutSchema() throws Exception {
        createAndVerifyBytesKeyHeader(null, "my-key-bytes".getBytes(), "my-key-bytes");
    }

    @Test
    public void buildByteArrayKeyHeaderWithSchema() throws Exception {
        createAndVerifyBytesKeyHeader(Schema.BYTES_SCHEMA, "message-key-bytes".getBytes(), "message-key-bytes");
    }

    @Test
    public void buildByteBufferKeyHeaderWithoutSchema() throws Exception {
        String key = "this-is-my-key";
        byte[] payload = key.getBytes();
        ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);

        createAndVerifyBytesKeyHeader(null, value, key);
    }

    @Test
    public void buildByteBufferKeyHeaderWithSchema() throws Exception {
        String key = "this-is-a-key";
        byte[] payload = key.getBytes();
        ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);

        createAndVerifyBytesKeyHeader(Schema.BYTES_SCHEMA, value, key);
    }


    private void createAndVerifyStringKeyHeader(Schema schema, String key) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.key.header", "JMSCorrelationID");

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(schema, key));
        assertEquals(key, message.getJMSCorrelationID());
    }

    private void createAndVerifyBytesKeyHeader(Schema schema, Object key, String keyAsString) throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.key.header", "JMSCorrelationID");

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(schema, key));
        assertEquals(keyAsString, new String(message.getJMSCorrelationIDAsBytes()));
    }
}
