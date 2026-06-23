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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.DeliveryMode;
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
import com.ibm.eventstreams.connect.mqsink.util.HexUtils;
import com.ibm.eventstreams.connect.mqsink.util.KafkaConnectRecordBridge;
import com.ibm.eventstreams.connect.mqsink.util.MQSourceTaskHelper;
import com.ibm.msg.client.jms.JmsConstants;

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
        sourceTask = MQSourceTaskHelper.startSourceTask(
                getConnectionName(), SOURCE_QUEUE, ROUND_TRIP_TOPIC, false);

        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setStringProperty("facilityCountryCode", "US");
        input.setIntProperty("volume", 11);
        input.setDoubleProperty("decimalmeaning", 42.0);
        input.setBooleanProperty("enabled", true);
        input.setLongProperty("createdAt", 1_609_459_200_000L);
        MQSourceTaskHelper.putMessage(getJmsContext(), SOURCE_QUEUE, input);

        final SourceRecord sourceRecord = MQSourceTaskHelper.pollSingleRecord(sourceTask);

        try (KafkaConnectRecordBridge bridge = new KafkaConnectRecordBridge(
                kafkaBootstrapServers(), ROUND_TRIP_TOPIC)) {
            bridge.produce(sourceRecord);
            final SinkRecord sinkRecord = bridge.consumeSinkRecord(Duration.ofSeconds(30));

            final MQSinkTask sinkTask = getMqSinkTask(sinkProperties(false));
            sinkTask.put(Collections.singletonList(sinkRecord));

            final List<Message> sinkMessages = getAllMessagesFromQueue(SINK_QUEUE);
            assertThat(sinkMessages).hasSize(1);
            assertSinkMatchesSourceHeaders(sinkMessages.get(0), sourceRecord.headers(), false, Set.of(
                    "facilityCountryCode",
                    "volume",
                    "decimalmeaning",
                    "enabled",
                    "createdAt"));
        }

        MQSourceTaskHelper.commit(sourceTask, sourceRecord);
    }

    @Test
    public void kafkaRoundTripLegacyStringProperties() throws Exception {
        sourceTask = MQSourceTaskHelper.startSourceTask(
                getConnectionName(), SOURCE_QUEUE, ROUND_TRIP_TOPIC, false);

        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setStringProperty("facilityCountryCode", "US");
        input.setStringProperty("volume", "11");
        input.setStringProperty("decimalmeaning", "42.0");
        input.setStringProperty("enabled", "true");
        input.setStringProperty("createdAt", "1609459200000");
        input.setStringProperty("customBytesHex", "01020304");
        MQSourceTaskHelper.putMessage(getJmsContext(), SOURCE_QUEUE, input);

        final SourceRecord sourceRecord = MQSourceTaskHelper.pollSingleRecord(sourceTask);

        try (KafkaConnectRecordBridge bridge = new KafkaConnectRecordBridge(
                kafkaBootstrapServers(), ROUND_TRIP_TOPIC)) {
            bridge.produce(sourceRecord);
            final SinkRecord sinkRecord = bridge.consumeSinkRecord(Duration.ofSeconds(30));

            final MQSinkTask sinkTask = getMqSinkTask(sinkProperties(false));
            sinkTask.put(Collections.singletonList(sinkRecord));

            final List<Message> sinkMessages = getAllMessagesFromQueue(SINK_QUEUE);
            assertThat(sinkMessages).hasSize(1);
            assertSinkMatchesSourceHeaders(sinkMessages.get(0), sourceRecord.headers(), false, Set.of(
                    "facilityCountryCode",
                    "volume",
                    "decimalmeaning",
                    "enabled",
                    "createdAt",
                    "customBytesHex"));
        }

        MQSourceTaskHelper.commit(sourceTask, sourceRecord);
    }

    @Test
    public void kafkaRoundTripTypedPropertiesAsSourceStringHeaders() throws Exception {
        sourceTask = MQSourceTaskHelper.startSourceTask(
                getConnectionName(), SOURCE_QUEUE, ROUND_TRIP_TOPIC, false);

        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setStringProperty("stringProp", "hello");
        input.setIntProperty("intProp", 42);
        input.setLongProperty("longProp", 9_000_000_000L);
        input.setShortProperty("shortProp", (short) 7);
        input.setByteProperty("byteProp", (byte) 3);
        input.setFloatProperty("floatProp", 3.14f);
        input.setDoubleProperty("doubleProp", 2.718);
        input.setBooleanProperty("booleanProp", true);
        MQSourceTaskHelper.putMessage(getJmsContext(), SOURCE_QUEUE, input);

        final SourceRecord sourceRecord = MQSourceTaskHelper.pollSingleRecord(sourceTask);

        try (KafkaConnectRecordBridge bridge = new KafkaConnectRecordBridge(
                kafkaBootstrapServers(), ROUND_TRIP_TOPIC)) {
            bridge.produce(sourceRecord);
            final SinkRecord sinkRecord = bridge.consumeSinkRecord(Duration.ofSeconds(30));

            final MQSinkTask sinkTask = getMqSinkTask(sinkProperties(false));
            sinkTask.put(Collections.singletonList(sinkRecord));

            final List<Message> sinkMessages = getAllMessagesFromQueue(SINK_QUEUE);
            assertThat(sinkMessages).hasSize(1);
            assertSinkMatchesSourceHeaders(sinkMessages.get(0), sourceRecord.headers(), false, Set.of(
                    "stringProp",
                    "intProp",
                    "longProp",
                    "shortProp",
                    "byteProp",
                    "floatProp",
                    "doubleProp",
                    "booleanProp"));
        }

        MQSourceTaskHelper.commit(sourceTask, sourceRecord);
    }

    @Test
    public void kafkaRoundTripMqmdProperties() throws Exception {
        sourceTask = MQSourceTaskHelper.startSourceTask(
                getConnectionName(), SOURCE_QUEUE, ROUND_TRIP_TOPIC, true);

        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setJMSPriority(5);
        input.setJMSDeliveryMode(DeliveryMode.PERSISTENT);

        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID, 1208);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_ENCODING, 273);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGFLAGS, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_OFFSET, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_REPORT, 2);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_FEEDBACK, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGTYPE, 8);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH, 1);

        input.setIntProperty(JmsConstants.JMS_IBM_ENCODING, 273);
        input.setIntProperty(JmsConstants.JMS_IBM_MSGTYPE, 8);
        input.setIntProperty(JmsConstants.JMS_IBM_FEEDBACK, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_RETAIN, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT, 1);
        input.setBooleanProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);

        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_EXCEPTION, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_EXPIRATION, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_COA, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_COD, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_PAN, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_NAN, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_DISCARD_MSG, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE, 1);

        input.setStringProperty(JmsConstants.JMS_IBM_MQMD_REPLYTOQ, "REPLY.Q");
        input.setStringProperty(JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR, "QM1");
        input.setStringProperty(JmsConstants.JMS_IBM_CHARACTER_SET, "UTF-8");

        input.setJMSCorrelationIDAsBytes(
                HexUtils.parseHex("414D51207061756C745639344C545320EBC32F6A01A00740"));
        input.setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID,
                HexUtils.parseHex("414141407061756C745639344C545320EBC32F6A01A00740"));
        input.setStringProperty(JmsConstants.JMSX_GROUPID, "mygroup");
        input.setIntProperty(JmsConstants.JMSX_GROUPSEQ, 1);
        MQSourceTaskHelper.putMessage(getJmsContext(), SOURCE_QUEUE, input);

        final SourceRecord sourceRecord = MQSourceTaskHelper.pollSingleRecord(sourceTask);

        try (KafkaConnectRecordBridge bridge = new KafkaConnectRecordBridge(
                kafkaBootstrapServers(), ROUND_TRIP_TOPIC)) {
            bridge.produce(sourceRecord);
            final SinkRecord sinkRecord = bridge.consumeSinkRecord(Duration.ofSeconds(30));

            final MQSinkTask sinkTask = getMqSinkTask(sinkProperties(true));
            sinkTask.put(Collections.singletonList(sinkRecord));

            final List<Message> sinkMessages = getAllMessagesFromQueue(SINK_QUEUE);
            assertThat(sinkMessages).hasSize(1);
            assertSinkMatchesSourceHeaders(sinkMessages.get(0), sourceRecord.headers(), true, Set.of(
                    JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID,
                    JmsConstants.JMS_IBM_MQMD_ENCODING,
                    JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER,
                    JmsConstants.JMS_IBM_MQMD_MSGFLAGS,
                    JmsConstants.JMS_IBM_MQMD_OFFSET,
                    JmsConstants.JMS_IBM_MQMD_REPORT,
                    JmsConstants.JMS_IBM_MQMD_FEEDBACK,
                    JmsConstants.JMS_IBM_MQMD_MSGTYPE,
                    JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH,
                    JmsConstants.JMS_IBM_ENCODING,
                    JmsConstants.JMS_IBM_MSGTYPE,
                    JmsConstants.JMS_IBM_FEEDBACK,
                    JmsConstants.JMS_IBM_RETAIN,
                    JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP,
                    JmsConstants.JMS_IBM_REPORT_EXCEPTION,
                    JmsConstants.JMS_IBM_REPORT_EXPIRATION,
                    JmsConstants.JMS_IBM_REPORT_COA,
                    JmsConstants.JMS_IBM_REPORT_COD,
                    JmsConstants.JMS_IBM_REPORT_PAN,
                    JmsConstants.JMS_IBM_REPORT_NAN,
                    JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID,
                    JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID,
                    JmsConstants.JMS_IBM_REPORT_DISCARD_MSG,
                    JmsConstants.JMS_IBM_PUTAPPLTYPE,
                    JmsConstants.JMS_IBM_MQMD_REPLYTOQ,
                    JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR,
                    JmsConstants.JMS_IBM_CHARACTER_SET,
                    JmsConstants.JMS_IBM_MQMD_CORRELID,
                    JmsConstants.JMS_IBM_MQMD_MSGID,
                    JmsConstants.JMSX_GROUPID,
                    JmsConstants.JMSX_GROUPSEQ));
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
