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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples.CustomProperty;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples.IntegerProperty;
import com.ibm.eventstreams.connect.mqsink.util.MqmdHeaderSamples.StringProperty;

/**
 * Reproduces the client issue: MQ source emits MQMD headers as strings; {@link BaseMessageBuilder}
 * copies them with {@code setStringProperty}, which IBM MQ rejects for integer MQMD fields
 * (JMSCC0051).
 */
public class MqmdStringKafkaHeadersIT extends AbstractJMSContextIT {

    private static final String BODY = "mqmd header issue repro";

    @Test
    public void ibmMqRejectsStringSetterForIntegerMqmdProperties() throws Exception {
        for (final IntegerProperty sample : MqmdHeaderSamples.integerMqmdProperties()) {
            final Message message = getJmsContext().createTextMessage(BODY);

            try {
                message.setStringProperty(sample.key(), sample.stringValue());
                fail("IBM MQ should reject setStringProperty for " + sample.key());
            } catch (final JMSException e) {
                assertTrue("Expected JMSCC0051 for " + sample.key() + ": " + e.getMessage(),
                        containsJmscc0051(e));
            }

            message.setIntProperty(sample.key(), sample.intValue());
            assertEquals("Wrong int value for " + sample.key(),
                    sample.intValue(), message.getIntProperty(sample.key()));
        }
    }

    @Test
    public void ibmMqAcceptsStringSetterForStringMqmdProperties() throws Exception {
        for (final StringProperty sample : MqmdHeaderSamples.stringMqmdProperties()) {
            final Message message = getJmsContext().createTextMessage(BODY);
            message.setStringProperty(sample.key(), sample.value());
            assertEquals("Wrong string value for " + sample.key(),
                    sample.value(), message.getStringProperty(sample.key()));
        }
    }

    @Test
    public void currentBuilderFailsOnEachIntegerMqmdHeader() throws Exception {
        final DefaultMessageBuilder builder = builderWithHeaderCopyEnabled();

        for (final IntegerProperty sample : MqmdHeaderSamples.integerMqmdProperties()) {
            try {
                builder.fromSinkRecord(getJmsContext(), sinkRecord(sample.asSingleHeader()));
                fail("Expected ConnectException for string header " + sample.key());
            } catch (final ConnectException e) {
                assertTrue("Expected JMSCC0051 for " + sample.key() + ": " + e.getMessage(),
                        containsJmscc0051(e));
            }
        }
    }

    @Test
    public void currentBuilderSucceedsOnStringMqmdHeaders() throws Exception {
        final DefaultMessageBuilder builder = builderWithHeaderCopyEnabled();

        for (final StringProperty sample : MqmdHeaderSamples.stringMqmdProperties()) {
            final Message message = builder.fromSinkRecord(getJmsContext(), sinkRecord(sample.asSingleHeader()));
            assertEquals(sample.value(), message.getStringProperty(sample.key()));
        }
    }

    @Test
    public void currentBuilderSucceedsOnCustomStringHeaders() throws Exception {
        final DefaultMessageBuilder builder = builderWithHeaderCopyEnabled();

        for (final CustomProperty sample : MqmdHeaderSamples.customProperties()) {
            final Message message = builder.fromSinkRecord(getJmsContext(), sinkRecord(sample.asSingleHeader()));
            assertEquals(sample.value(), message.getStringProperty(sample.key()));
        }
    }

    @Test
    public void currentBuilderFailsOnClientMqmdHeaderSet() throws Exception {
        final DefaultMessageBuilder builder = builderWithHeaderCopyEnabled();

        try {
            builder.fromSinkRecord(getJmsContext(), sinkRecord(MqmdHeaderSamples.clientMqmdPipelineAsStrings()));
            fail("Expected ConnectException when copying full client MQMD string header set");
        } catch (final ConnectException e) {
            assertTrue("Expected JMSCC0051 in cause chain: " + e.getMessage(), containsJmscc0051(e));
        }
    }

    private static DefaultMessageBuilder builderWithHeaderCopyEnabled() {
        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        builder.configure(props);
        return builder;
    }

    private static SinkRecord sinkRecord(final ConnectHeaders headers) {
        return new SinkRecord(
                TOPIC, PARTITION,
                Schema.STRING_SCHEMA, "key",
                Schema.STRING_SCHEMA, BODY,
                0L, null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);
    }

    private static boolean containsJmscc0051(final Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            final String message = current.getMessage();
            if (message != null && (message.contains("JMSCC0051")
                    || message.contains("should be set using type 'java.lang.Integer'"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
