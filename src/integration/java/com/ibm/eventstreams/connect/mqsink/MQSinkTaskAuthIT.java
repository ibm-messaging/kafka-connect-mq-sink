/**
 * Copyright 2022, 2023, 2024 IBM Corporation
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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MQSinkTaskAuthIT {

    public static final boolean USER_AUTHENTICATION_MQCSP = true;

    @ClassRule
    final public static GenericContainer<?> MQ_CONTAINER = new GenericContainer<>(AbstractJMSContextIT.MQ_IMAGE)
            .withEnv("LICENSE", "accept")
            .withEnv("MQ_QMGR_NAME", AbstractJMSContextIT.QMGR_NAME)
            .withEnv("MQ_APP_PASSWORD", AbstractJMSContextIT.APP_PASSWORD)
            .withExposedPorts(AbstractJMSContextIT.TCP_MQ_EXPOSED_PORT, AbstractJMSContextIT.REST_API_EXPOSED_PORT)
            .withCreateContainerCmdModifier(cmd -> cmd.withHostConfig(
                    new HostConfig().withPortBindings(
                            new PortBinding(Ports.Binding.bindPort(AbstractJMSContextIT.TCP_MQ_HOST_PORT),
                                    new ExposedPort(AbstractJMSContextIT.TCP_MQ_EXPOSED_PORT)),
                            new PortBinding(Ports.Binding.bindPort(AbstractJMSContextIT.REST_API_HOST_PORT),
                                    new ExposedPort(AbstractJMSContextIT.REST_API_EXPOSED_PORT)))))
            .waitingFor(Wait.forListeningPort());

    @Test
    public void testAuthenticatedQueueManager() throws Exception {
        final Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("mq.queue.manager", AbstractJMSContextIT.QMGR_NAME);
        connectorProps.put("mq.connection.mode", AbstractJMSContextIT.CONNECTION_MODE);
        connectorProps.put("mq.connection.name.list", AbstractJMSContextIT.HOST_NAME + "("
                + MQ_CONTAINER.getMappedPort(AbstractJMSContextIT.TCP_MQ_EXPOSED_PORT).toString() + ")");
        connectorProps.put("mq.channel.name", AbstractJMSContextIT.CHANNEL_NAME);
        connectorProps.put("mq.queue", AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME);
        connectorProps.put("mq.user.authentication.mqcsp", String.valueOf(USER_AUTHENTICATION_MQCSP));
        connectorProps.put("mq.user.name", AbstractJMSContextIT.APP_USERNAME);
        connectorProps.put("mq.password", AbstractJMSContextIT.APP_PASSWORD);
        connectorProps.put("mq.message.builder", AbstractJMSContextIT.DEFAULT_MESSAGE_BUILDER);

        final MQSinkTask newConnectTask = new MQSinkTask();
        newConnectTask.start(connectorProps);

        final List<SinkRecord> records = new ArrayList<>();
        final SinkRecord record = new SinkRecord(AbstractJMSContextIT.TOPIC, 0,
                null, null,
                null, "message payload",
                0);
        records.add(record);

        newConnectTask.put(records);

        newConnectTask.stop();

        final List<Message> messages = getAllMessagesFromQueue();
        assertEquals(1, messages.size());
        assertEquals("message payload", messages.get(0).getBody(String.class));
    }

    private List<Message> getAllMessagesFromQueue() throws JMSException {
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer consumer = null;

        final JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);

        final JmsConnectionFactory cf = ff.createConnectionFactory();
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, AbstractJMSContextIT.HOST_NAME);
        cf.setIntProperty(WMQConstants.WMQ_PORT, MQ_CONTAINER.getMappedPort(AbstractJMSContextIT.TCP_MQ_EXPOSED_PORT));
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, AbstractJMSContextIT.CHANNEL_NAME);
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, AbstractJMSContextIT.QMGR_NAME);
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, USER_AUTHENTICATION_MQCSP);
        cf.setStringProperty(WMQConstants.USERID, AbstractJMSContextIT.APP_USERNAME);
        cf.setStringProperty(WMQConstants.PASSWORD, AbstractJMSContextIT.APP_PASSWORD);

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        destination = session.createQueue(AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME);
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
}
