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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples.CustomProperty;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples.IntegerProperty;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples.StringProperty;

/**
 * Unit-level reproduction of the client issue without Testcontainers.
 */
public class MqmdStringKafkaHeadersTest {

    @Test
    public void headerCopyUsesSetStringPropertyForEachIntegerMqmdHeader() throws Exception {
        for (final IntegerProperty sample : MqmdHeaderSamples.integerMqmdProperties()) {
            final Message message = mock(Message.class);
            final JMSException mqTypeError = new JMSException(
                    "JMSCC0051: The property '" + sample.key()
                            + "' should be set using type 'java.lang.Integer', not 'java.lang.String'.");
            doThrow(mqTypeError).when(message).setStringProperty(eq(sample.key()), eq(sample.stringValue()));

            final DefaultMessageBuilder builder = builderReturning(message);

            try {
                builder.fromSinkRecord(mock(JMSContext.class), sinkRecord(sample.asSingleHeader()));
                fail("Expected ConnectException for " + sample.key());
            } catch (final ConnectException e) {
                assertTrue("Expected JMSCC0051 for " + sample.key(), containsJmscc0051(e));
            }

            verify(message).setStringProperty(sample.key(), sample.stringValue());
            verify(message, never()).setIntProperty(eq(sample.key()), org.mockito.ArgumentMatchers.anyInt());
        }
    }

    @Test
    public void headerCopyUsesSetStringPropertyForStringMqmdHeaders() throws Exception {
        for (final StringProperty sample : MqmdHeaderSamples.stringMqmdProperties()) {
            final Message message = mock(Message.class);
            final DefaultMessageBuilder builder = builderReturning(message);

            builder.fromSinkRecord(mock(JMSContext.class), sinkRecord(sample.asSingleHeader()));

            verify(message).setStringProperty(sample.key(), sample.value());
            verify(message, never()).setIntProperty(eq(sample.key()), org.mockito.ArgumentMatchers.anyInt());
        }
    }

    @Test
    public void headerCopyUsesSetStringPropertyForCustomHeaders() throws Exception {
        for (final CustomProperty sample : MqmdHeaderSamples.customProperties()) {
            final Message message = mock(Message.class);
            final DefaultMessageBuilder builder = builderReturning(message);

            builder.fromSinkRecord(mock(JMSContext.class), sinkRecord(sample.asSingleHeader()));

            verify(message).setStringProperty(sample.key(), sample.value());
        }
    }

    private static DefaultMessageBuilder builderReturning(final Message message) {
        final DefaultMessageBuilder builder = new DefaultMessageBuilder() {
            @Override
            public Message getJMSMessage(final JMSContext jmsCtxt, final SinkRecord record) {
                return message;
            }
        };
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        builder.configure(props);
        return builder;
    }

    private static SinkRecord sinkRecord(final ConnectHeaders headers) {
        return new SinkRecord(
                "topic", 0,
                Schema.STRING_SCHEMA, "key",
                Schema.STRING_SCHEMA, "body",
                0L, null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);
    }

    private static boolean containsJmscc0051(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            final String message = current.getMessage();
            if (message != null && message.contains("JMSCC0051")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
