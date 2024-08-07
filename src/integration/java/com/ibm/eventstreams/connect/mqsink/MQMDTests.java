/**
 * Copyright 2024 IBM Corporation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.util.MessageDescriptorBuilder;
import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

public class MQMDTests extends MQSinkTaskAuthIT {

    private Map<String, String> createDefaultConnectorProperties() {
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
        connectorProps.put("mq.message.mqmd.write", "true");
        connectorProps.put("mq.message.mqmd.context", "ALL");
        return connectorProps;
    }

    private MQQueueManager getQmgr() throws MQException {
        Hashtable<Object, Object> props = new Hashtable<>();
        props.put(MQConstants.HOST_NAME_PROPERTY, "localhost");
        props.put(MQConstants.PORT_PROPERTY, MQ_CONTAINER.getMappedPort(AbstractJMSContextIT.TCP_MQ_EXPOSED_PORT));
        props.put(MQConstants.CHANNEL_PROPERTY, AbstractJMSContextIT.CHANNEL_NAME);
        props.put(MQConstants.USER_ID_PROPERTY, AbstractJMSContextIT.APP_USERNAME);
        props.put(MQConstants.PASSWORD_PROPERTY, AbstractJMSContextIT.APP_PASSWORD);

        return new MQQueueManager(AbstractJMSContextIT.QMGR_NAME, props);
    }

    private MQMessage[] mqGet(String queue) throws MQException, IOException {
        MQQueue q = getQmgr().accessQueue(queue, MQConstants.MQOO_INPUT_SHARED | MQConstants.MQOO_INQUIRE);

        List<MQMessage> messages = new ArrayList<>();
        while (q.getCurrentDepth() > 0) {
            MQMessage msg = new MQMessage();
            q.get(msg);
            messages.add(msg);
        }
        q.close();

        return messages.toArray(new MQMessage[messages.size()]);
    }

    @Test
    public void verifyAuthExceptionIfNoAuthContextPermission()
            throws JMSException, MQException, IOException, InterruptedException {
        MQ_CONTAINER.execInContainer("setmqaut",
                "-m", AbstractJMSContextIT.QMGR_NAME,
                "-n", AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME,
                "-p", AbstractJMSContextIT.APP_USERNAME,
                "-t", "queue",
                "-setall", "+get", "+browse", "+put", "+inq"); // The setall grant is removed if present

        MQ_CONTAINER.execInContainer("setmqaut",
                "-m", AbstractJMSContextIT.QMGR_NAME,
                "-p", AbstractJMSContextIT.APP_USERNAME,
                "-t", "qmgr",
                "-setall"); // The setall grant is removed if present

        final MQSinkTask newConnectTask = new MQSinkTask();
        newConnectTask.initialize(mock(SinkTaskContext.class));

        // configure a sink task for string messages
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                AbstractJMSContextIT.DEFAULT_MESSAGE_BUILDER);
        connectorConfigProps.put("mq.message.body.jms", "true");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create a test message
        final List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(AbstractJMSContextIT.TOPIC, AbstractJMSContextIT.PARTITION,
                Schema.STRING_SCHEMA, "key0",
                Schema.STRING_SCHEMA, "value0",
                0L));

        // An MQException is thrown with code MQRC_NOT_AUTHORIZED (reason code 2035) and
        // compcode 2. This exception occurs when the MQ authorization for the queue and
        // queue manager lacks the necessary permissions. Since MQRC_NOT_AUTHORIZED is
        // considered a retriable exception, the system retries it, leading to
        // RetriableException
        assertThrows(RetriableException.class, () -> {
            newConnectTask.put(records);
        });
    }

    @Test
    public void verifyMQMDWriteDisabled()
            throws JMSException, MQException, IOException, InterruptedException {
        final MQSinkTask newConnectTask = new MQSinkTask();

        // configure a sink task for string messages
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                MessageDescriptorBuilder.class.getCanonicalName());

        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.message.mqmd.write", "false");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test message
        final List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(AbstractJMSContextIT.TOPIC, AbstractJMSContextIT.PARTITION,
                Schema.STRING_SCHEMA, "key0",
                Schema.STRING_SCHEMA, "value0",
                0L));
        newConnectTask.put(records);

        // flush the message
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final TopicPartition topic = new TopicPartition(AbstractJMSContextIT.TOPIC, AbstractJMSContextIT.PARTITION);
        final OffsetAndMetadata offset = new OffsetAndMetadata(0L);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the message was submitted to MQ without descriptors
        final MQMessage[] messagesInMQ = mqGet(AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME);
        assertEquals(1, messagesInMQ.length);
        assertEquals("value0", messagesInMQ[0].readLine());
        assertNotEquals("ThisIsMyId", new String(messagesInMQ[0].messageId).trim());
        assertNotEquals("ThisIsMyApplicationData", messagesInMQ[0].applicationIdData.trim());
        assertNotEquals("ThisIsMyPutApplicationName", messagesInMQ[0].putApplicationName.trim());
        assertEquals("MYQMGR", messagesInMQ[0].replyToQueueManagerName.trim());
    }

    @Test
    public void verifyMQMDWriteEnabled()
            throws JMSException, MQException, IOException, InterruptedException {

        // The following code block sets authorization permissions for a specified user
        // (AbstractJMSContextIT.APP_USERNAME) on a particular queue
        // (AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME) within an IBM MQ environment,
        // granting permissions to set all properties (`+setall`) about messages on the
        // queue.
        MQ_CONTAINER.execInContainer("setmqaut",
                "-m", AbstractJMSContextIT.QMGR_NAME,
                "-n", AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME,
                "-p", AbstractJMSContextIT.APP_USERNAME,
                "-t", "queue",
                "+setall", "+get", "+browse", "+put", "+inq");

        // This code line grants authorization permissions for a specified user
        // (`AbstractJMSContextIT.APP_USERNAME`) on a specific queue manager
        // (`AbstractJMSContextIT.QMGR_NAME`) within an IBM MQ environment, allowing the
        // user to set all properties (`+setall`) of the queue manager.
        MQ_CONTAINER.execInContainer("setmqaut",
                "-m", AbstractJMSContextIT.QMGR_NAME,
                "-p", AbstractJMSContextIT.APP_USERNAME,
                "-t", "qmgr",
                "+setall");

        // How to debug mq to list the authorization:
        // For queue: dspmqaut -m MYQMGR -t queue -n DEV.QUEUE.1 -p app
        // For queue Manager: dspmqaut -m MYQMGR -t qmgr -p app

        final MQSinkTask newConnectTask = new MQSinkTask();

        // configure a sink task for string messages
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                MessageDescriptorBuilder.class.getCanonicalName());
        connectorConfigProps.put("mq.message.body.jms", "true");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create a test message
        final List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(AbstractJMSContextIT.TOPIC, AbstractJMSContextIT.PARTITION,
                Schema.STRING_SCHEMA, "key0",
                Schema.STRING_SCHEMA, "value0",
                0L));
        newConnectTask.put(records);

        // flush the message
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final TopicPartition topic = new TopicPartition(AbstractJMSContextIT.TOPIC, AbstractJMSContextIT.PARTITION);
        final OffsetAndMetadata offset = new OffsetAndMetadata(0L);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the message was submitted to MQ with the correct descriptors
        final MQMessage[] messagesInMQ = mqGet(AbstractJMSContextIT.DEFAULT_SINK_QUEUE_NAME);
        assertEquals(1, messagesInMQ.length);
        assertEquals("value0", messagesInMQ[0].readLine());
        assertEquals("ThisIsMyId", new String(messagesInMQ[0].messageId).trim());
        assertEquals("ThisIsMyApplicationData", messagesInMQ[0].applicationIdData.trim());
        assertEquals("ThisIsMyPutApplicationName", messagesInMQ[0].putApplicationName.trim());
        assertEquals("MYQMGR", messagesInMQ[0].replyToQueueManagerName.trim());
    }
}
