/**
 * Copyright 2022 IBM Corporation
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
import java.util.concurrent.TimeoutException;

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
import org.testcontainers.containers.output.WaitingConsumer;

import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

public class MQSinkTaskAuthIT {

    private static final String QMGR_NAME = "MYAUTHQMGR";
    private static final String QUEUE_NAME = "DEV.QUEUE.2";
    private static final String CHANNEL_NAME = "DEV.APP.SVRCONN";
    private static final String APP_PASSWORD = "MySuperSecretPassword";


    @ClassRule
    public static GenericContainer<?> MQ_CONTAINER = new GenericContainer<>("icr.io/ibm-messaging/mq:latest")
        .withEnv("LICENSE", "accept")
        .withEnv("MQ_QMGR_NAME", QMGR_NAME)
        .withEnv("MQ_ENABLE_EMBEDDED_WEB_SERVER", "false")
        .withEnv("MQ_APP_PASSWORD", APP_PASSWORD)
        .withExposedPorts(1414);


    @Test
    public void testAuthenticatedQueueManager() throws Exception {
        waitForQueueManagerStartup();

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("mq.queue.manager", QMGR_NAME);
        connectorProps.put("mq.connection.mode", "client");
        connectorProps.put("mq.connection.name.list", "localhost(" + MQ_CONTAINER.getMappedPort(1414).toString() + ")");
        connectorProps.put("mq.channel.name", CHANNEL_NAME);
        connectorProps.put("mq.queue", QUEUE_NAME);
        connectorProps.put("mq.user.authentication.mqcsp", "true");
        connectorProps.put("mq.user.name", "app");
        connectorProps.put("mq.password", APP_PASSWORD);
        connectorProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder");

        MQSinkTask newConnectTask = new MQSinkTask();
        newConnectTask.start(connectorProps);

        List<SinkRecord> records = new ArrayList<>();
        SinkRecord record = new SinkRecord("KAFKA.TOPIC", 0,
                null, null,
                null, "message payload",
                0);
        records.add(record);

        newConnectTask.put(records);

        newConnectTask.stop();

        List<Message> messages = getAllMessagesFromQueue();
        assertEquals(1, messages.size());
        assertEquals("message payload", messages.get(0).getBody(String.class));
    }


    private void waitForQueueManagerStartup() throws TimeoutException {
        WaitingConsumer logConsumer = new WaitingConsumer();
        MQ_CONTAINER.followOutput(logConsumer);
        logConsumer.waitUntil(logline -> logline.getUtf8String().contains("AMQ5806I: Queued Publish/Subscribe Daemon started for queue manager"));
    }

    private List<Message> getAllMessagesFromQueue() throws JMSException {
        Connection connection = null;
        Session session = null;
        Destination destination = null;
        MessageConsumer consumer = null;

        JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);

        JmsConnectionFactory cf = ff.createConnectionFactory();
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, "localhost");
        cf.setIntProperty(WMQConstants.WMQ_PORT, MQ_CONTAINER.getMappedPort(1414));
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, CHANNEL_NAME);
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, QMGR_NAME);
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        cf.setStringProperty(WMQConstants.USERID, "app");
        cf.setStringProperty(WMQConstants.PASSWORD, APP_PASSWORD);

        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        destination = session.createQueue(QUEUE_NAME);
        consumer = session.createConsumer(destination);

        connection.start();

        List<Message> messages = new ArrayList<>();
        Message message;
        do {
            message = consumer.receiveNoWait();
            if (message != null) {
                messages.add(message);
            }
        }
        while (message != null);

        connection.close();

        return messages;
    }
}
