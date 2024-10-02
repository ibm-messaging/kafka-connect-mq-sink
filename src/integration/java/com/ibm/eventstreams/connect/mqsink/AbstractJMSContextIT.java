/**
 * Copyright 2023, 2023, 2024 IBM Corporation
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.ibm.eventstreams.connect.mqsink.util.MQRestAPIHelper;
import com.ibm.eventstreams.connect.mqsink.utils.Configs;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

/**
 * Helper class for integration tests that have a dependency on JMSContext.
 *
 * It starts a queue manager in a test container, and uses it to create
 * a JMSContext instance, that can be used in tests.
 */
public abstract class AbstractJMSContextIT {

    public static final String QMGR_NAME = "MYQMGR";
    public static final String CHANNEL_NAME = "DEV.APP.SVRCONN";

    public static final int TCP_MQ_HOST_PORT = 9090;
    public static final int TCP_MQ_EXPOSED_PORT = 1414;

    public static final int REST_API_HOST_PORT = 9091;
    public static final int REST_API_EXPOSED_PORT = 9443;

    public static final String DEFAULT_SINK_QUEUE_NAME = "DEV.QUEUE.1";
    public static final String DEFAULT_SINK_STATE_QUEUE_NAME = "DEV.QUEUE.2";

    public static final String APP_USERNAME = "app";
    public static final String APP_PASSWORD = "MySuperSecretPassword";

    public static final String ADMIN_PASSWORD = "passw0rd";

    public static final String TOPIC = "SINK.TOPIC.NAME";
    public static final int PARTITION = 3;

    public static final String DEFAULT_MESSAGE_BUILDER = "com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder";
    public static final String CONNECTION_MODE = "client";
    public static final String HOST_NAME = "localhost";

    public static final String MQ_IMAGE = "icr.io/ibm-messaging/mq:9.4.0.5-r2";
    public static final boolean USER_AUTHENTICATION_MQCSP = false;

    protected final ObjectMapper mapper = new ObjectMapper();

    MQRestAPIHelper mqRestApiHelper = getMQRestApiHelper();

    private static MQRestAPIHelper getMQRestApiHelper() {
        return new MQRestAPIHelper.MQRestAPIHelperBuilder()
                .qmgrname(QMGR_NAME)
                .portnum(REST_API_HOST_PORT)
                .password(ADMIN_PASSWORD)
                .build();
    }

    @SuppressWarnings("resource")
    @ClassRule
    final public static GenericContainer<?> MQ_CONTAINER = new GenericContainer<>(MQ_IMAGE)
            .withEnv("LICENSE", "accept")
            .withEnv("MQ_QMGR_NAME", QMGR_NAME)
            .withExposedPorts(TCP_MQ_EXPOSED_PORT, REST_API_EXPOSED_PORT)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(TCP_MQ_HOST_PORT),
                                    new ExposedPort(TCP_MQ_EXPOSED_PORT)),
                            new PortBinding(Ports.Binding.bindPort(REST_API_HOST_PORT),
                                    new ExposedPort(REST_API_EXPOSED_PORT)))))
            .waitingFor(Wait.forListeningPort());

    @NotNull
    protected static Map<String, String> getConnectionDetails() {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("mq.queue.manager", QMGR_NAME);
        connectorProps.put("mq.connection.mode", CONNECTION_MODE);
        connectorProps.put("mq.connection.name.list",
                HOST_NAME + "(" + MQ_CONTAINER.getMappedPort(TCP_MQ_EXPOSED_PORT).toString() + ")");
        connectorProps.put("mq.channel.name", CHANNEL_NAME);
        connectorProps.put("mq.queue", DEFAULT_SINK_QUEUE_NAME);
        connectorProps.put("mq.user.authentication.mqcsp", String.valueOf(USER_AUTHENTICATION_MQCSP));
        connectorProps.put("mq.message.builder", DEFAULT_MESSAGE_BUILDER);
        return connectorProps;
    }

    @NotNull
    protected static Map<String, String> getExactlyOnceConnectionDetails() {
        final Map<String, String> connectorProps = getConnectionDetails();
        connectorProps.put("mq.exactly.once.state.queue", DEFAULT_SINK_STATE_QUEUE_NAME);
        return connectorProps;
    }

    private JMSContext jmsContext;

    /**
     * Returns a JMS context pointing at a developer queue manager running in a
     * test container.
     */
    public JMSContext getJmsContext() throws Exception {
        if (jmsContext == null) {
            final MQConnectionFactory mqcf = new MQConnectionFactory();
            mqcf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            mqcf.setChannel(CHANNEL_NAME);
            mqcf.setQueueManager(QMGR_NAME);
            mqcf.setConnectionNameList(getConnectionName());

            jmsContext = mqcf.createContext();
        }

        return jmsContext;
    }

    /**
     * Gets the host port that has been mapped to the default MQ 1414 port in the
     * test container.
     */
    public Integer getMQPort() {
        return MQ_CONTAINER.getMappedPort(TCP_MQ_EXPOSED_PORT);
    }

    public String getQmgrName() {
        return QMGR_NAME;
    }

    public String getChannelName() {
        return CHANNEL_NAME;
    }

    public String getConnectionName() {
        return HOST_NAME + "(" + getMQPort().toString() + ")";
    }

    /**
     * Retrieves all messages from the specified MQ queue (destructively). Used in
     * tests to verify that the expected messages were put to the test queue.
     */
    public List<Message> getAllMessagesFromQueue(final String queueName) throws JMSException {
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer consumer = null;

        final JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);

        final JmsConnectionFactory cf = ff.createConnectionFactory();
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, HOST_NAME);
        cf.setIntProperty(WMQConstants.WMQ_PORT, getMQPort());
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, getChannelName());
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, getQmgrName());
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        destination = session.createQueue(queueName);
        consumer = session.createConsumer(destination);

        connection.start();

        final List<Message> messages = new ArrayList<>();
        Message message;
        do {
            message = consumer.receiveNoWait();
            if (message != null) {
                messages.add(message);
            }
        } while (message != null);

        connection.close();

        return messages;
    }

    protected void clearAllMessages(final String queue) throws JMSException {
        getAllMessagesFromQueue(queue);
    }

    @NotNull
    private static JmsConnectionFactory getJmsConnectionFactory(final JmsFactoryFactory ff) throws JMSException {
        final JmsConnectionFactory cf = ff.createConnectionFactory();
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, HOST_NAME);
        cf.setIntProperty(WMQConstants.WMQ_PORT, MQ_CONTAINER.getMappedPort(TCP_MQ_EXPOSED_PORT));
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, CHANNEL_NAME);
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, QMGR_NAME);
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, USER_AUTHENTICATION_MQCSP);
        return cf;
    }

    public static List<Message> browseAllMessagesFromQueue(final String queueName) throws JMSException {
        Connection connection = null;
        Session session = null;

        final JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);

        final JmsConnectionFactory cf = getJmsConnectionFactory(ff);

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final QueueBrowser browser = session.createBrowser(queue);

        connection.start();

        final List<Message> messages = new ArrayList<>();

        final Enumeration<?> e = browser.getEnumeration();
        while (e.hasMoreElements()) {
            messages.add((Message) e.nextElement());
        }

        connection.close();

        return messages;
    }

    @NotNull
    protected static MQSinkTask getMqSinkTask(final Map<String, String> connectorProps) {
        final MQSinkTask mqSinkTask = new MQSinkTask();
        mqSinkTask.initialize(mock(SinkTaskContext.class));
        mqSinkTask.start(connectorProps);
        return mqSinkTask;
    }

    protected List<SinkRecord> createSinkRecords(final int size) {
        return createSinkRecordsFromOffsets(
                LongStream.range(0, size).boxed().collect(Collectors.toList()));
    }

    public List<SinkRecord> createSinkRecordsFromOffsets(final List<Long> offsets) {
        return offsets.stream().map(
                i -> new SinkRecord(
                        TOPIC,
                        PARTITION,
                        null,
                        null,
                        null,
                        "Message with offset " + i + " ",
                        i))
                .collect(Collectors.toList());
    }

    /**
     * Extracts the offset from the message string. This is used to verify that the
     * correct offset was committed to the offset topic.
     */
    protected String extractOffsetFromHashMapString(final String message, final String key)
            throws JsonProcessingException, JsonMappingException {
        final HashMap<String, String> messageMap = extractHashMapFromString(message);
        return messageMap.get(key);
    }

    protected HashMap<String, String> extractHashMapFromString(final String message)
            throws JsonProcessingException, JsonMappingException {
        final HashMap<String, String> messageMap = mapper.readValue(message,
                new TypeReference<HashMap<String, String>>() {
                });
        return messageMap;
    }

    protected JMSWorker configureJMSWorkerSpy( AbstractConfig config, final MQSinkTask mqSinkTask) {
        final JMSWorker jmsWorkerSpy = spy(new JMSWorker());
        mqSinkTask.worker = jmsWorkerSpy;
        mqSinkTask.worker.configure(config);
        mqSinkTask.worker.connect();
        return jmsWorkerSpy;
    }
}
