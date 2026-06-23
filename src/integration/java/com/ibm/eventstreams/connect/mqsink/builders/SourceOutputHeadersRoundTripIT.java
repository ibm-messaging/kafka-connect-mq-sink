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

import static com.ibm.eventstreams.connect.mqsink.util.SourceHeaderAssertions.assertSinkMatchesSourceHeaders;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;
import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.eventstreams.connect.mqsink.util.JmsTestPropertySets;
import com.ibm.eventstreams.connect.mqsink.util.JmsTestPropertySets.Scenario;

/**
 * Fast round-trip integration tests using the MQ source header converter directly.
 *
 * <p>For a full MQ source task → Kafka → MQ sink task path, see
 * {@link ConnectKafkaHeadersRoundTripIT}.
 *
 * <p>Mirrors {@code mq-jms-test-put} scenarios using the real {@link JmsToKafkaHeaderConverter}
 * from the MQ source connector and {@link DefaultMessageBuilder} from the sink.
 */
public class SourceOutputHeadersRoundTripIT extends AbstractJMSContextIT {

    private static final String BODY = "round-trip test";

    private DefaultMessageBuilder createBuilder(final boolean mqmdWrite) {
        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        if (mqmdWrite) {
            props.put("mq.message.mqmd.write", "true");
        }
        builder.configure(props);
        return builder;
    }

    private Message roundTrip(final Scenario scenario, final boolean mqmdWrite) throws Exception {
        final TextMessage input = getJmsContext().createTextMessage(BODY);
        JmsTestPropertySets.applyScenario(input, scenario);

        final ConnectHeaders sourceHeaders = new JmsToKafkaHeaderConverter()
                .convertJmsPropertiesToKafkaHeaders(input);

        final SinkRecord record = new SinkRecord(
                TOPIC,
                PARTITION,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, BODY,
                0L,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                sourceHeaders);

        return createBuilder(mqmdWrite).fromSinkRecord(getJmsContext(), record);
    }

    @Test
    public void roundTripCustomPropertiesFromSourceHeaders() throws Exception {
        final ConnectHeaders sourceHeaders = sourceHeadersFor(Scenario.CUSTOM);
        final Message sinkMessage = roundTrip(Scenario.CUSTOM, false);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, false);
    }

    @Test
    public void roundTripLegacyStringPropertiesFromSourceHeaders() throws Exception {
        final ConnectHeaders sourceHeaders = sourceHeadersFor(Scenario.LEGACY);
        final Message sinkMessage = roundTrip(Scenario.LEGACY, false);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, false);
    }

    @Test
    public void roundTripTypedPropertiesAsSourceStringHeaders() throws Exception {
        final ConnectHeaders sourceHeaders = sourceHeadersFor(Scenario.TYPES);
        final Message sinkMessage = roundTrip(Scenario.TYPES, false);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, false);
    }

    @Test
    public void roundTripMqmdPropertiesFromSourceHeaders() throws Exception {
        final ConnectHeaders sourceHeaders = sourceHeadersFor(Scenario.MQMD);
        final Message sinkMessage = roundTrip(Scenario.MQMD, true);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, true);
    }

    private ConnectHeaders sourceHeadersFor(final Scenario scenario) throws Exception {
        final TextMessage input = getJmsContext().createTextMessage(BODY);
        JmsTestPropertySets.applyScenario(input, scenario);
        return new JmsToKafkaHeaderConverter().convertJmsPropertiesToKafkaHeaders(input);
    }
}
