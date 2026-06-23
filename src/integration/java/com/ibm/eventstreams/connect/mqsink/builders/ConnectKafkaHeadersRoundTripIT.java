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
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.MQSourceTask;
import com.ibm.eventstreams.connect.mqsink.AbstractKafkaMqRoundTripIT;
import com.ibm.eventstreams.connect.mqsink.MQSinkTask;
import com.ibm.eventstreams.connect.mqsink.util.JmsTestPropertySets;
import com.ibm.eventstreams.connect.mqsink.util.JmsTestPropertySets.Scenario;
import com.ibm.eventstreams.connect.mqsink.util.KafkaConnectRecordBridge;
import com.ibm.eventstreams.connect.mqsink.util.MQSourceTaskHelper;

/**
 * Full connector round-trip through a real Kafka broker:
 * MQ source task → Kafka topic → MQ sink task → sink MQ queue.
 *
 * <p>Run separately from the faster simulated path in {@link SourceOutputHeadersRoundTripIT}:
 *
 * <pre>
 * mvn failsafe:integration-test failsafe:verify \
 *   -Dmaven.surefire.skip=true \
 *   -Dit.test=ConnectKafkaHeadersRoundTripIT
 * </pre>
 */
public class ConnectKafkaHeadersRoundTripIT extends AbstractKafkaMqRoundTripIT {

    private static final String BODY = "kafka round-trip test";

    private MQSourceTask sourceTask;

    @Before
    public void clearQueues() throws Exception {
        grantAppUserMqmdPutOnQueue(SOURCE_QUEUE);
        grantAppUserMqmdPutOnQueue(SINK_QUEUE);
        clearAllMessages(SOURCE_QUEUE);
        clearAllMessages(SINK_QUEUE);
    }

    @After
    public void stopSourceTask() {
        if (sourceTask != null) {
            sourceTask.stop();
            sourceTask = null;
        }
    }

    @Test
    public void kafkaRoundTripCustomProperties() throws Exception {
        roundTripThroughKafka(Scenario.CUSTOM, false);
    }

    @Test
    public void kafkaRoundTripLegacyStringProperties() throws Exception {
        roundTripThroughKafka(Scenario.LEGACY, false);
    }

    @Test
    public void kafkaRoundTripTypedPropertiesAsSourceStringHeaders() throws Exception {
        roundTripThroughKafka(Scenario.TYPES, false);
    }

    @Test
    public void kafkaRoundTripMqmdProperties() throws Exception {
        roundTripThroughKafka(Scenario.MQMD, true);
    }

    private void roundTripThroughKafka(final Scenario scenario, final boolean mqmdWrite) throws Exception {
        sourceTask = MQSourceTaskHelper.startSourceTask(
                getConnectionName(), SOURCE_QUEUE, ROUND_TRIP_TOPIC, mqmdWrite);

        final TextMessage input = getJmsContext().createTextMessage(BODY);
        JmsTestPropertySets.applyScenario(input, scenario);
        MQSourceTaskHelper.putMessage(getJmsContext(), SOURCE_QUEUE, input);

        final SourceRecord sourceRecord = MQSourceTaskHelper.pollSingleRecord(sourceTask);

        try (KafkaConnectRecordBridge bridge = new KafkaConnectRecordBridge(
                kafkaBootstrapServers(), ROUND_TRIP_TOPIC)) {
            bridge.produce(sourceRecord);
            final SinkRecord sinkRecord = bridge.consumeSinkRecord(Duration.ofSeconds(30));

            final MQSinkTask sinkTask = getMqSinkTask(sinkProperties(mqmdWrite));
            sinkTask.put(Collections.singletonList(sinkRecord));

            final List<Message> sinkMessages = getAllMessagesFromQueue(SINK_QUEUE);
            assertThat(sinkMessages).hasSize(1);
            assertSinkMatchesSourceHeaders(sinkMessages.get(0), sourceRecord.headers(), mqmdWrite);
        }

        MQSourceTaskHelper.commit(sourceTask, sourceRecord);
    }

    private Map<String, String> sinkProperties(final boolean mqmdWrite) {
        final Map<String, String> props = new HashMap<>(getConnectionDetails());
        props.put("mq.queue", SINK_QUEUE);
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        if (mqmdWrite) {
            props.put("mq.message.mqmd.write", "true");
        }
        return props;
    }
}
