/**
 * Copyright 2023 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsink;

import static com.ibm.eventstreams.connect.mqsink.util.MQRestAPIHelper.START_CHANNEL;
import static com.ibm.eventstreams.connect.mqsink.util.MQRestAPIHelper.STOP_CHANNEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

public class MQSinkTaskAtLeastOnceIT extends AbstractJMSContextIT {

    @After
    public void after() throws Exception {
        clearAllMessages(DEFAULT_SINK_QUEUE_NAME);
        clearAllMessages(DEFAULT_SINK_STATE_QUEUE_NAME);
    }

    @Test
    public void testCrashBeforeCommitToKafkaThenRollbackOccurs() throws Exception {
        final Map<String, String> connectorProps = getConnectionDetails();

        final MQSinkTask mqSinkTask = getMqSinkTask(connectorProps);

        final JMSWorker jmsWorkerSpy = configureJMSWorkerSpy(connectorProps, mqSinkTask);

        final List<SinkRecord> sinkRecords = createSinkRecords(10);

        doCallRealMethod()
                .doCallRealMethod()
                .doAnswer(invocation -> {
                    // Send the record as expected
                    final SinkRecord sinkRecord = invocation.getArgument(0);
                    jmsWorkerSpy.send(sinkRecord);

                    // But also do the STOP channel
                    mqRestApiHelper.sendCommand(STOP_CHANNEL);
                    return null;
                })
                .doCallRealMethod() // --> failure happens the next time we call.
                .when(jmsWorkerSpy).send(any(SinkRecord.class));

        assertThrows(ConnectException.class, () -> {
            mqSinkTask.put(sinkRecords);
        });

        mqRestApiHelper.sendCommand(START_CHANNEL);
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME)).isEmpty();
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME)).isEmpty();

        mqSinkTask.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 0 ",
                        "Message with offset 1 ",
                        "Message with offset 2 ",
                        "Message with offset 3 ",
                        "Message with offset 4 ",
                        "Message with offset 5 ",
                        "Message with offset 6 ",
                        "Message with offset 7 ",
                        "Message with offset 8 ",
                        "Message with offset 9 ");
        final List<Message> stateQueueMessages = browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME);
        assertThat(stateQueueMessages)
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .isEmpty();
    }

    @Test
    public void testCrashAfterMQCommitBeforeKafkaCommit() throws Exception {
        final Map<String, String> connectorProps = getConnectionDetails();

        MQSinkTask mqSinkTask = getMqSinkTask(connectorProps);

        final List<SinkRecord> sinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                124L,
                125L,
                126L));

        mqSinkTask.put(sinkRecords);

        List<Message> stateQueueMessages = browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME);
        assertThat(stateQueueMessages)
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .isEmpty();

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 124 ",
                        "Message with offset 125 ",
                        "Message with offset 126 ");

        // Closest we can simulate a connect "crash", the idea being that this would
        // happen after MQ commit, before Kafka committed the records
        mqSinkTask.stop();
        mqSinkTask = getMqSinkTask(connectorProps);

        // Put called again with the same records + a few more.
        final List<SinkRecord> newSinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                127L,
                128L,
                129L));
        sinkRecords.addAll(newSinkRecords);
        mqSinkTask.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 124 ",
                        "Message with offset 125 ", // Duplicate Message
                        "Message with offset 126 ", // Duplicate Message
                        "Message with offset 124 ", // Duplicate Message
                        "Message with offset 125 ",
                        "Message with offset 126 ",
                        "Message with offset 127 ",
                        "Message with offset 128 ",
                        "Message with offset 129 ");
        stateQueueMessages = browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME);
        assertThat(stateQueueMessages)
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .isEmpty();
    }
}
