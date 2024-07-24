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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSConsumer;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.eventstreams.connect.mqsink.util.SinkRecordBuilderForTest;
import com.ibm.eventstreams.connect.mqsink.utils.Configs;


public class MQSinkTaskExactlyOnceIT extends AbstractJMSContextIT {

    @After
    public void after() throws Exception {
        clearAllMessages(DEFAULT_SINK_QUEUE_NAME);
        clearAllMessages(DEFAULT_SINK_STATE_QUEUE_NAME);
    }

    @Test
    public void testCrashBeforeCommitToKafkaThenRollbackOccurs() throws Exception {
        final Map<String, String> connectorProps = getExactlyOnceConnectionDetails();

        final MQSinkTask mqSinkTask = getMqSinkTask(connectorProps);

        final JMSWorker jmsWorkerSpy = configureJMSWorkerSpy(Configs.customConfig(connectorProps), mqSinkTask);

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
                .containsExactly("9");
    }

    @Test
    public void testCrashAfterMQCommitBeforeKafkaCommit() throws Exception {
        final Map<String, String> connectorProps = getExactlyOnceConnectionDetails();

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
                .containsExactly("126");

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
                        "Message with offset 125 ",
                        "Message with offset 126 ",
                        "Message with offset 127 ",
                        "Message with offset 128 ",
                        "Message with offset 129 ");
        stateQueueMessages = browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME);
        assertThat(stateQueueMessages)
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("129");
    }

    @Test
    public void testOnlyOnceWithMultiplePartitionsAndMultipleTopics()
            throws Exception {
        final Map<String, String> connectorProps = getExactlyOnceConnectionDetails();

        final MQSinkTask mqSinkTask = getMqSinkTask(connectorProps);

        final JMSWorker jmsWorkerSpy = configureJMSWorkerSpy(Configs.customConfig(connectorProps), mqSinkTask);

        final List<SinkRecord> sinkRecords = new ArrayList<>();

        final SinkRecordBuilderForTest recordBuilder = new SinkRecordBuilderForTest().keySchema(null).key(null)
                .valueSchema(null);

        sinkRecords.addAll(Arrays.asList(
                recordBuilder.topic("TOPIC-A").partition(0).offset(0L).value("TOPIC-A-0-0L").build(),
                recordBuilder.topic("TOPIC-A").partition(1).offset(0L).value("TOPIC-A-1-0L").build(),
                recordBuilder.topic("TOPIC-A").partition(2).offset(0L).value("TOPIC-A-2-0L").build(),
                recordBuilder.topic("TOPIC-B").partition(0).offset(0L).value("TOPIC-B-0-0L").build(),
                recordBuilder.topic("TOPIC-B").partition(1).offset(0L).value("TOPIC-B-1-0L").build(),
                recordBuilder.topic("TOPIC-B").partition(2).offset(0L).value("TOPIC-B-2-0L").build(),
                recordBuilder.topic("TOPIC-A").partition(0).offset(1L).value("TOPIC-A-0-1L").build(),
                recordBuilder.topic("TOPIC-A").partition(1).offset(1L).value("TOPIC-A-1-1L").build(),
                recordBuilder.topic("TOPIC-A").partition(2).offset(1L).value("TOPIC-A-2-1L").build(),
                recordBuilder.topic("TOPIC-B").partition(0).offset(1L).value("TOPIC-B-0-1L").build(),
                recordBuilder.topic("TOPIC-B").partition(1).offset(1L).value("TOPIC-B-1-1L").build(),
                recordBuilder.topic("TOPIC-B").partition(2).offset(1L).value("TOPIC-B-2-1L").build()));

        checkIfItCrashBeforeCommitToKafkaThenRollbackOccurs(mqSinkTask, jmsWorkerSpy, sinkRecords);
        // ------------------------------
        sinkRecords.clear();
        sinkRecords.addAll(Arrays.asList(
                recordBuilder.topic("TOPIC-A").partition(0).offset(2L).value("TOPIC-A-0-2L").build(),
                recordBuilder.topic("TOPIC-A").partition(1).offset(2L).value("TOPIC-A-1-2L").build(),
                recordBuilder.topic("TOPIC-A").partition(2).offset(2L).value("TOPIC-A-2-2L").build(),
                recordBuilder.topic("TOPIC-B").partition(0).offset(2L).value("TOPIC-B-0-2L").build(),
                recordBuilder.topic("TOPIC-B").partition(1).offset(2L).value("TOPIC-B-1-2L").build(),
                recordBuilder.topic("TOPIC-B").partition(2).offset(2L).value("TOPIC-B-2-2L").build()));

        checkIfItCrashAfterMQCommitBeforeKafkaCommit(connectorProps, mqSinkTask, jmsWorkerSpy, sinkRecords);
    }

    private void checkIfItCrashBeforeCommitToKafkaThenRollbackOccurs(final MQSinkTask mqSinkTask,
            final JMSWorker jmsWorkerSpy,
            final List<SinkRecord> sinkRecords)
            throws IOException, KeyManagementException, NoSuchAlgorithmException, JMSException {
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
                .containsExactly("TOPIC-A-0-0L",
                        "TOPIC-A-1-0L",
                        "TOPIC-A-2-0L",
                        "TOPIC-B-0-0L",
                        "TOPIC-B-1-0L",
                        "TOPIC-B-2-0L",
                        "TOPIC-A-0-1L", // <-- last offset record info saved to state queue
                        "TOPIC-A-1-1L", // <-- last offset record info saved to state queue
                        "TOPIC-A-2-1L", // <-- last offset record info saved to state queue
                        "TOPIC-B-0-1L", // <-- last offset record info saved to state queue
                        "TOPIC-B-1-1L", // <-- last offset record info saved to state queue
                        "TOPIC-B-2-1L"); // <-- last offset record info saved to state queue
        final List<Message> stateQueueMessages = browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME);
        assertThat(stateQueueMessages)
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractHashMapFromString(message))
                .containsExactly(
                        new HashMap<String, String>() {
                            {
                                put("TOPIC-A-0", "1");
                                put("TOPIC-A-1", "1");
                                put("TOPIC-A-2", "1");
                                put("TOPIC-B-0", "1");
                                put("TOPIC-B-1", "1");
                                put("TOPIC-B-2", "1");
                            }
                        });
    }

    private void checkIfItCrashAfterMQCommitBeforeKafkaCommit(final Map<String, String> connectorProps,
            final MQSinkTask mqSinkTask,
            final JMSWorker jmsWorkerSpy, final List<SinkRecord> sinkRecords)
            throws IOException, KeyManagementException, NoSuchAlgorithmException, JMSException {
        final List<Message> stateQueueMessages;
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
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractHashMapFromString(message))
                .containsExactly(
                        new HashMap<String, String>() {
                            {
                                put("TOPIC-A-0", "1");
                                put("TOPIC-A-1", "1");
                                put("TOPIC-A-2", "1");
                                put("TOPIC-B-0", "1");
                                put("TOPIC-B-1", "1");
                                put("TOPIC-B-2", "1");
                            }
                        });
        mqSinkTask.put(sinkRecords);
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractHashMapFromString(message))
                .containsExactly(
                        new HashMap<String, String>() {
                            {
                                put("TOPIC-A-0", "2");
                                put("TOPIC-A-1", "2");
                                put("TOPIC-A-2", "2");
                                put("TOPIC-B-0", "2");
                                put("TOPIC-B-1", "2");
                                put("TOPIC-B-2", "2");
                            }
                        });
        // Closest we can simulate a connect "crash", the idea being that this would
        // happen after MQ commit, before Kafka committed the records
        mqSinkTask.stop();
        final MQSinkTask mqSinkTaskNew = getMqSinkTask(connectorProps);
        // Put called again with the same records + a few more.
        final List<SinkRecord> newSinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                127L,
                128L,
                129L));
        sinkRecords.addAll(newSinkRecords);
        mqSinkTaskNew.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "TOPIC-A-0-0L",
                        "TOPIC-A-1-0L",
                        "TOPIC-A-2-0L",
                        "TOPIC-B-0-0L",
                        "TOPIC-B-1-0L",
                        "TOPIC-B-2-0L",
                        "TOPIC-A-0-1L",
                        "TOPIC-A-1-1L",
                        "TOPIC-A-2-1L",
                        "TOPIC-B-0-1L",
                        "TOPIC-B-1-1L",
                        "TOPIC-B-2-1L",
                        "TOPIC-A-0-2L",
                        "TOPIC-A-1-2L",
                        "TOPIC-A-2-2L",
                        "TOPIC-B-0-2L",
                        "TOPIC-B-1-2L",
                        "TOPIC-B-2-2L",
                        "Message with offset 127 ",
                        "Message with offset 128 ",
                        "Message with offset 129 ");
        stateQueueMessages = browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME);
        assertThat(stateQueueMessages)
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractHashMapFromString(message))
                .containsExactly(
                        new HashMap<String, String>() {
                            {
                                put("TOPIC-A-0", "2");
                                put("TOPIC-A-1", "2");
                                put("TOPIC-A-2", "2");
                                put("TOPIC-B-0", "2");
                                put("TOPIC-B-1", "2");
                                put("TOPIC-B-2", "2");
                                put(TOPIC + "-" + PARTITION, "129");
                            }
                        });
    }

    @Test
    public void testFailureOfWriteLastRecordOffsetToStateQueue() throws JsonProcessingException, JMSException {
        // In this test we simulate a failure of the writeLastRecordOffsetToStateQueue
        // method. We do this by creating a spy of the ObjectMapper and throwing an
        // exception when the method is called. We then check that the state queue is
        // not updated and that the records are not committed to MQ.
        final Map<String, String> connectorProps = getExactlyOnceConnectionDetails();

        // -----------------------------------
        // We send 2 records to MQ so that the last committed offset is saved to the
        // state queue
        final MQSinkTask mqSinkTask = getMqSinkTask(connectorProps);
        // put in two record to see if the put is working
        final List<SinkRecord> sinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                121L,
                122L)); // This will get saved in the state queue
        mqSinkTask.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("122");

        // -----------------------------------
        // recreate JsonProcessingException when calling
        // jmsWorker.writeLastRecordOffsetToStateQueue(lastCommittedOffsetMap); This
        // will cause the put method to fail. We check that the state queue is not
        // updated and that the records are not committed to MQ.
        mqSinkTask.worker.mapper = spy(ObjectMapper.class);
        final List<SinkRecord> failingSinkRecords = getSinkRecordThatThrowsJSONProcessingException(mqSinkTask);
        assertThrows(ConnectException.class, () -> {
            mqSinkTask.put(failingSinkRecords);
        });

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 121 ",
                        "Message with offset 122 ");
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("122");

        // -----------------------------------
        // We recreate the MQSinkTask and send 5 records to MQ, The first 2 records are
        // the records from the previous failed put. The last 3 records are new records.
        // We check that the state queue is updated and that the records are committed
        // to MQ.

        // Closest we can simulate a connect "crash", the idea being that this would
        // happen after MQ commit, before Kafka committed the records
        mqSinkTask.stop();
        mqSinkTask.start(connectorProps);

        // Put called again with the same records + a few more.
        final List<SinkRecord> newSinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                125L,
                126L,
                127L)); // This will get saved in the state queue
        failingSinkRecords.addAll(newSinkRecords);
        sinkRecords.addAll(failingSinkRecords);
        mqSinkTask.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 121 ",
                        "Message with offset 122 ",
                        "Message with offset 123 ",
                        "Message with offset 124 ",
                        "Message with offset 125 ",
                        "Message with offset 126 ",
                        "Message with offset 127 ");
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("127");
    }

    @Test
    public void testFailureOfReadFromStateQueue() throws JsonProcessingException, JMSException {
        // In this test we simulate a failure of the readFromStateQueue method. We do
        // this by creating a spy of the JMSConsumer and throwing an exception when the
        // method is called. We then check that the state queue is not updated and that
        // the records are not committed to MQ.

        final Map<String, String> connectorProps = getExactlyOnceConnectionDetails();

        // -----------------------------------
        // We send 2 records to MQ so that the last committed offset is saved to the
        // state queue
        final MQSinkTask mqSinkTask = getMqSinkTask(connectorProps);
        final List<SinkRecord> sinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                121L,
                122L)); // This will get saved in the state queue
        mqSinkTask.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("122");

        // -----------------------------------
        // We recreate the MQSinkTask and send 2 records to MQ. We check that the state
        // queue is not updated and that the records are not committed to MQ.
        final JMSConsumer jmsComsumerSpy = spy(mqSinkTask.worker.jmsCons);
        mqSinkTask.worker.jmsCons = jmsComsumerSpy;
        final List<SinkRecord> failingSinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                123L,
                124L));

        // This recreates the exception that is thrown when the readFromStateQueue
        doThrow(new JMSRuntimeException("This is a JMSException caused by a spy!!"))
                .when(jmsComsumerSpy)
                .receiveNoWait();

        assertThrows(ConnectException.class, () -> {
            mqSinkTask.put(failingSinkRecords);
        });

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 121 ",
                        "Message with offset 122 ");
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("122");

        // -----------------------------------
        // We recreate the MQSinkTask and send 5 records to MQ, The first 2 records are
        // the records from the previous failed put. The last 3 records are new records.
        // We check that the state queue is updated and that the records are committed
        // to MQ.

        // Closest we can simulate a connect "crash", the idea being that this would
        // happen after MQ commit, before Kafka committed the records
        mqSinkTask.stop();
        mqSinkTask.start(connectorProps);

        // Put called again with the same records + a few more.
        final List<SinkRecord> newSinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                125L,
                126L,
                127L)); // This will get saved in the state queue
        failingSinkRecords.addAll(newSinkRecords);
        sinkRecords.addAll(failingSinkRecords);
        mqSinkTask.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .containsExactly(
                        "Message with offset 121 ",
                        "Message with offset 122 ",
                        "Message with offset 123 ",
                        "Message with offset 124 ",
                        "Message with offset 125 ",
                        "Message with offset 126 ",
                        "Message with offset 127 ");
        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_STATE_QUEUE_NAME))
                .extracting(message -> message.getBody(String.class))
                .extracting(message -> extractOffsetFromHashMapString(message, TOPIC + "-" + PARTITION))
                .containsExactly("127");
    }

    private List<SinkRecord> getSinkRecordThatThrowsJSONProcessingException(final MQSinkTask mqSinkTask)
            throws JsonProcessingException {
        final List<SinkRecord> sinkRecords;
        sinkRecords = createSinkRecordsFromOffsets(Arrays.asList(
                123L,
                124L));

        final HashMap<String, String> lastCommittedOffsetMapThatNeedsToThrowTheException = new HashMap<String, String>() {
            {
                put(TOPIC + "-" + PARTITION, String.valueOf(124));
            }
        };
        when(mqSinkTask.worker.mapper.writeValueAsString(lastCommittedOffsetMapThatNeedsToThrowTheException))
                .thenThrow(new JsonProcessingException("This a test exception") {
                    private static final long serialVersionUID = 1L;
                });
        return sinkRecords;
    }
}
