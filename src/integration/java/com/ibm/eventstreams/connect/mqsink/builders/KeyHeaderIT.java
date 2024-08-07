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
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class KeyHeaderIT extends AbstractJMSContextIT {

    private SinkRecord generateSinkRecord(final Schema keySchema, final Object keyValue) {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;
        final Schema valueSchema = Schema.STRING_SCHEMA;
        final String value = "message payload";

        return new SinkRecord(topic, partition,
                keySchema, keyValue,
                valueSchema, value,
                offset);
    }

    @Test
    public void verifyUnsupportedKeyHeader() throws Exception {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.key.header", "unsupported");

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        final MessageBuilderException exc = assertThrows(MessageBuilderException.class, () -> {
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
        final String key = "this-is-my-key";
        final byte[] payload = key.getBytes();
        final ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);

        createAndVerifyBytesKeyHeader(null, value, key);
    }

    @Test
    public void buildByteBufferKeyHeaderWithSchema() throws Exception {
        final String key = "this-is-a-key";
        final byte[] payload = key.getBytes();
        final ByteBuffer value = ByteBuffer.allocate(payload.length);
        value.put(payload);

        createAndVerifyBytesKeyHeader(Schema.BYTES_SCHEMA, value, key);
    }

    private void createAndVerifyStringKeyHeader(final Schema schema, final String key) throws Exception {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.key.header", "JMSCorrelationID");

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(schema, key));
        assertEquals(key, message.getJMSCorrelationID());
    }

    private void createAndVerifyBytesKeyHeader(final Schema schema, final Object key, final String keyAsString)
            throws Exception {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.key.header", "JMSCorrelationID");

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(schema, key));
        assertEquals(keyAsString, new String(message.getJMSCorrelationIDAsBytes()));
    }
}
