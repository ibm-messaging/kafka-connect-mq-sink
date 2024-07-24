/**
 * Copyright 2023, 2023 IBM Corporation
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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class DefaultMessageBuilderWithHeadersIT extends AbstractJMSContextIT {

    private MessageBuilder builder;

    @Before
    public void prepareMessageBuilder() {
        builder = new DefaultMessageBuilder();

        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        builder.configure(props);
    }

    private SinkRecord generateSinkRecord(final ConnectHeaders headers) {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;
        return new SinkRecord(topic, partition,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, "Test message",
                offset,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);
    }

    @Test
    public void buildMessageWithNoHeaders() throws Exception {
        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(null));

        // verify there are no MQ message properties
        assertFalse(message.getPropertyNames().hasMoreElements());
    }

    @Test
    public void buildMessageWithStringHeaders() throws Exception {
        final Map<String, String> testHeaders = new HashMap<>();
        testHeaders.put("HeaderOne", "This is test header one");
        testHeaders.put("HeaderTwo", "This is test header two");
        testHeaders.put("HeaderThree", "This is test header three");
        testHeaders.put("HeaderFour", "This is test header four");

        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        for (final String key : testHeaders.keySet()) {
            headers.addString(key, testHeaders.get(key));
        }

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        for (final String key : testHeaders.keySet()) {
            assertEquals(testHeaders.get(key), message.getStringProperty(key));
        }
    }

    @Test
    public void buildMessageWithBooleanHeaders() throws Exception {
        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addBoolean("TestTrue", true);
        headers.addBoolean("TestFalse", false);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        assertEquals("true", message.getStringProperty("TestTrue"));
        assertEquals("false", message.getStringProperty("TestFalse"));
        assertTrue(message.getBooleanProperty("TestTrue"));
        assertFalse(message.getBooleanProperty("TestFalse"));
    }

    @Test
    public void buildMessageWithIntegerHeaders() throws Exception {
        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("TestOne", 1);
        headers.addInt("TestTwo", 2);
        headers.addInt("TestThree", 3);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        assertEquals("1", message.getStringProperty("TestOne"));
        assertEquals("2", message.getStringProperty("TestTwo"));
        assertEquals("3", message.getStringProperty("TestThree"));
        assertEquals(1, message.getIntProperty("TestOne"));
        assertEquals(2, message.getIntProperty("TestTwo"));
        assertEquals(3, message.getIntProperty("TestThree"));
    }

    @Test
    public void buildMessageWithDoubleHeaders() throws Exception {
        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addDouble("TestPi", 3.14159265359);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        assertEquals("3.14159265359", message.getStringProperty("TestPi"));
        assertEquals(3.14159265359, message.getDoubleProperty("TestPi"), 0.0000000001);
    }
}
