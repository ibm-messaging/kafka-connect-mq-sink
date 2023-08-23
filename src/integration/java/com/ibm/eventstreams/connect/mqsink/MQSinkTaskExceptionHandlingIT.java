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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.ibm.mq.MQException;

public class MQSinkTaskExceptionHandlingIT extends AbstractJMSContextIT {

    @After
    public void cleanup() throws Exception {
        clearAllMessages(DEFAULT_SINK_QUEUE_NAME);
        clearAllMessages(DEFAULT_SINK_STATE_QUEUE_NAME);
    }

    @Test
    public void testMQSinkTaskStartJMSException() {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new JMSRuntimeException("This is a JMSException caused by a spy!!"))
                .when(jmsWorkerSpy)
                .configureProducer();

        assertThrows(ConnectException.class, () -> {
            connectTaskSpy.start(connectorConfigProps);
        });
    }

    @Test
    public void testMQSinkTaskStartJMSWorkerConnectionException() {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new JMSWorkerConnectionException("This is a JMSWorkerConnectionException caused by a spy!!"))
                .when(jmsWorkerSpy)
                .configure(connectorConfigProps);

        assertThrows(ConnectException.class, () -> {
            connectTaskSpy.start(connectorConfigProps);
        });
    }

    @Test
    public void testMQSinkTaskStartInvalidDestinationRuntimeException() {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new InvalidDestinationRuntimeException(
                "This is a InvalidDestinationRuntimeException caused by a spy!!"))
                .when(jmsWorkerSpy)
                .createConsumerForStateQueue();

        assertThrows(ConnectException.class, () -> {
            connectTaskSpy.start(connectorConfigProps);
        });
    }

    @Test
    public void testPutDoesThrowExceptionDueToMQConnectionError()
            throws JMSException, KeyManagementException, NoSuchAlgorithmException, IOException {

        assertThat(getAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME).size()).isEqualTo(0);

        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();

        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        connectTaskSpy.start(connectorConfigProps);

        final List<SinkRecord> sinkRecords = createSinkRecords(10);

        connectTaskSpy.put(sinkRecords);

        assertThat(browseAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME).size()).isEqualTo(10);

        mqRestApiHelper.sendCommand(STOP_CHANNEL);
        assertThrows(RetriableException.class, () -> {
            connectTaskSpy.put(sinkRecords);
        });

        mqRestApiHelper.sendCommand(START_CHANNEL);

        assertThat(getAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME))
                .extracting((Message message) -> ((TextMessage) message).getText())
                .containsExactly("Message with offset 0 ",
                        "Message with offset 1 ",
                        "Message with offset 2 ",
                        "Message with offset 3 ",
                        "Message with offset 4 ",
                        "Message with offset 5 ",
                        "Message with offset 6 ",
                        "Message with offset 7 ",
                        "Message with offset 8 ",
                        "Message with offset 9 ");
        verify(connectTaskSpy.getContext(), times(1)).timeout(connectTaskSpy.retryBackoffMs);
        verify(connectTaskSpy, times(1)).stop();
    }

    @Test
    public void testMQSinkTaskPutJsonProcessingException()
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new JsonProcessingException("This is a JsonProcessingException caused by a spy!!") {
            private static final long serialVersionUID = 1L;
        })
                .when(jmsWorkerSpy)
                .readFromStateQueue();

        connectTaskSpy.start(connectorConfigProps);
        final List<SinkRecord> sinkRecords = createSinkRecords(10);
        assertThrows(ConnectException.class, () -> {
            connectTaskSpy.put(sinkRecords);
        });
        verify(connectTaskSpy.getContext(), times(0)).timeout(connectTaskSpy.retryBackoffMs);
        verify(connectTaskSpy, times(1)).stop();
    }

    @Test
    public void testMQSinkTaskPutJMSWithRetriableException()
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        final MQException exp = new MQException(1, 2003, getClass());
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new JMSRuntimeException("This is a JMSRuntimeException caused by a spy!!", "custom error code", exp))
                .when(jmsWorkerSpy)
                .readFromStateQueue();

        connectTaskSpy.start(connectorConfigProps);
        final List<SinkRecord> sinkRecords = createSinkRecords(10);
        assertThrows(RetriableException.class, () -> {
            connectTaskSpy.put(sinkRecords);
        });
        verify(connectTaskSpy.getContext(), times(1)).timeout(connectTaskSpy.retryBackoffMs);
        verify(connectTaskSpy, times(1)).stop();
    }

    @Test
    public void testMQSinkTaskPutJMSException()
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new JMSRuntimeException("This is a JMSRuntimeException caused by a spy!!", "custom error code"))
                .when(jmsWorkerSpy)
                .readFromStateQueue();

        connectTaskSpy.start(connectorConfigProps);
        final List<SinkRecord> sinkRecords = createSinkRecords(10);
        assertThrows(ConnectException.class, () -> {
            connectTaskSpy.put(sinkRecords);
        });
        verify(connectTaskSpy.getContext(), times(0)).timeout(connectTaskSpy.retryBackoffMs);
        verify(connectTaskSpy, times(1)).stop();
    }

    @Test
    public void testMQSinkTaskPutJMSWithoutRetriableException()
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        final MQException exp = new MQException(1, 1, getClass());
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new JMSRuntimeException("This is a JMSRuntimeException caused by a spy!!", "custom error code", exp))
                .when(jmsWorkerSpy)
                .readFromStateQueue();

        connectTaskSpy.start(connectorConfigProps);
        final List<SinkRecord> sinkRecords = createSinkRecords(10);
        assertThrows(ConnectException.class, () -> {
            connectTaskSpy.put(sinkRecords);
        });
        verify(connectTaskSpy.getContext(), times(0)).timeout(connectTaskSpy.retryBackoffMs);
        verify(connectTaskSpy, times(1)).stop();
    }

    @Test
    public void testMQSinkTaskPutRetriableException()
            throws JsonProcessingException, JMSRuntimeException, JMSException {
        final Map<String, String> connectorConfigProps = getExactlyOnceConnectionDetails();
        final MQSinkTask connectTaskSpy = spy(getMqSinkTask(connectorConfigProps));

        final JMSWorker jmsWorkerSpy = spy(JMSWorker.class);
        final MQException exp = new MQException(1, 2053, getClass());
        when(connectTaskSpy.newJMSWorker()).thenReturn(jmsWorkerSpy);
        doThrow(new RetriableException("This is a RetriableException caused by a spy!!", exp))
                .when(jmsWorkerSpy)
                .readFromStateQueue();

        connectTaskSpy.start(connectorConfigProps);
        final List<SinkRecord> sinkRecords = createSinkRecords(10);
        assertThrows(RetriableException.class, () -> {
            connectTaskSpy.put(sinkRecords);
        });
        verify(connectTaskSpy.getContext(), times(1)).timeout(connectTaskSpy.retryBackoffMs);
        verify(connectTaskSpy, times(0)).stop();
    }
}
