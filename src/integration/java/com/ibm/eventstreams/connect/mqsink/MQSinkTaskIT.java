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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.builders.MessageBuilderException;

public class MQSinkTaskIT extends AbstractJMSContextIT {

    private static final String TOPIC = "SINK.TOPIC.NAME";
    private static final int PARTITION = 3;
    private long commonOffset = 0;

    private SinkRecord generateSinkRecord(final Schema valueSchema, final Object value) {
        return new SinkRecord(TOPIC, PARTITION,
                null, null,
                valueSchema, value,
                commonOffset++);
    }

    private Map<String, String> createDefaultConnectorProperties() {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", getQmgrName());
        props.put("mq.connection.mode", CONNECTION_MODE);
        props.put("mq.connection.name.list", getConnectionName());
        props.put("mq.channel.name", getChannelName());
        props.put("mq.queue", DEFAULT_SINK_QUEUE_NAME);
        props.put("mq.user.authentication.mqcsp", String.valueOf(USER_AUTHENTICATION_MQCSP));
        return props;
    }

    @Test
    public void verifyUnsupportedReplyQueueName() {
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                DEFAULT_MESSAGE_BUILDER);
        connectorConfigProps.put("mq.reply.queue", "queue://QM2/Q2?persistence=2&priority=5");

        final MQSinkTask newConnectTask = new MQSinkTask();
        final MessageBuilderException exc = assertThrows(MessageBuilderException.class, () -> {
            newConnectTask.start(connectorConfigProps);
        });

        assertEquals("Reply-to queue URI must not contain properties", exc.getMessage());
    }

    @Test
    public void verifyUnsupportedKeyHeader() {
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                DEFAULT_MESSAGE_BUILDER);
        connectorConfigProps.put("mq.message.builder.key.header", "hello");

        final MQSinkTask newConnectTask = new MQSinkTask();
        final ConfigException exc = assertThrows(ConfigException.class, () -> {
            newConnectTask.start(connectorConfigProps);
        });

        assertEquals("Invalid value hello for configuration mq.message.builder.key.header: String must be one of: null, JMSCorrelationID", exc.getMessage());
    }

    @Test
    public void verifyStringMessages() throws JMSException {
        final MQSinkTask newConnectTask = new MQSinkTask();

        // configure a sink task for string messages
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                DEFAULT_MESSAGE_BUILDER);

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test messages
        final List<SinkRecord> records = new ArrayList<>();
        records.add(generateSinkRecord(null, "hello"));
        records.add(generateSinkRecord(null, "world"));
        newConnectTask.put(records);

        // flush the messages
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        final OffsetAndMetadata offset = new OffsetAndMetadata(commonOffset);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages were successfully submitted to MQ
        final List<Message> messagesInMQ = getAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME);
        assertEquals(2, messagesInMQ.size());
        assertEquals("hello", messagesInMQ.get(0).getBody(String.class));
        assertEquals("world", messagesInMQ.get(1).getBody(String.class));
    }

    @Test
    public void verifyStringJmsMessages() throws JMSException {
        final MQSinkTask newConnectTask = new MQSinkTask();

        final String topicProperty = "PutTopicNameHere";
        final String partitionProperty = "PutTopicPartitionHere";
        final String offsetProperty = "PutOffsetHere";

        // configure a sink task for string messages
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                DEFAULT_MESSAGE_BUILDER);
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.message.builder.topic.property", topicProperty);
        connectorConfigProps.put("mq.message.builder.partition.property", partitionProperty);
        connectorConfigProps.put("mq.message.builder.offset.property", offsetProperty);
        connectorConfigProps.put("mq.message.builder.key.header", "JMSCorrelationID");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test messages
        final List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(TOPIC, PARTITION,
                Schema.STRING_SCHEMA, "key0",
                null, "hello",
                commonOffset++));
        records.add(new SinkRecord(TOPIC, PARTITION,
                Schema.STRING_SCHEMA, "key1",
                null, "world",
                commonOffset++));
        newConnectTask.put(records);

        // flush the messages
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        final OffsetAndMetadata offset = new OffsetAndMetadata(commonOffset);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages were successfully submitted to MQ
        final List<Message> messagesInMQ = getAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME);
        assertEquals(2, messagesInMQ.size());
        assertEquals("hello", messagesInMQ.get(0).getBody(String.class));
        assertEquals("world", messagesInMQ.get(1).getBody(String.class));

        // verify that the message origin details were added to message properties
        for (int i = 0; i < 2; i++) {
            assertEquals(TOPIC, messagesInMQ.get(i).getStringProperty(topicProperty));
            assertEquals(PARTITION, messagesInMQ.get(i).getIntProperty(partitionProperty));
            assertEquals("key" + i, messagesInMQ.get(i).getJMSCorrelationID());
        }
        assertEquals(commonOffset - 2, messagesInMQ.get(0).getLongProperty(offsetProperty));
        assertEquals(commonOffset - 1, messagesInMQ.get(1).getLongProperty(offsetProperty));
    }

    @Test
    public void verifyJsonMessages() throws JMSException {
        final MQSinkTask newConnectTask = new MQSinkTask();

        // configure a sink task for JSON messages
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder",
                "com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.JsonConverter");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test messages
        final Schema fruitSchema = SchemaBuilder.struct()
                .name("com.ibm.eventstreams.tests.Fruit")
                .field("fruit", Schema.STRING_SCHEMA)
                .build();
        final Struct apple = new Struct(fruitSchema).put("fruit", "apple");
        final Struct banana = new Struct(fruitSchema).put("fruit", "banana");
        final Struct pear = new Struct(fruitSchema).put("fruit", "pear");

        // give the test messages to the sink task
        final List<SinkRecord> records = new ArrayList<>();
        records.add(generateSinkRecord(fruitSchema, apple));
        records.add(generateSinkRecord(fruitSchema, banana));
        records.add(generateSinkRecord(fruitSchema, pear));
        newConnectTask.put(records);

        // flush the messages
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        final OffsetAndMetadata offset = new OffsetAndMetadata(commonOffset);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages were successfully submitted to MQ
        final List<Message> messagesInMQ = getAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME);
        assertEquals(3, messagesInMQ.size());
        assertEquals("{\"fruit\":\"apple\"}", messagesInMQ.get(0).getBody(String.class));
        assertEquals("{\"fruit\":\"banana\"}", messagesInMQ.get(1).getBody(String.class));
        assertEquals("{\"fruit\":\"pear\"}", messagesInMQ.get(2).getBody(String.class));
    }

    @Test
    public void verifyStringWithDefaultBuilder() throws JMSException {
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfigProps.put("mq.message.builder",
                DEFAULT_MESSAGE_BUILDER);

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "ABC", "ABC");
    }

    @Test
    public void verifyStringWithJsonBuilder() throws JMSException {
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfigProps.put("mq.message.builder",
                "com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "ABC", "\"ABC\"");
    }

    @Test
    public void verifyJsonWithDefaultBuilder() throws JMSException {
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfigProps.put("mq.message.builder",
                DEFAULT_MESSAGE_BUILDER);

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "ABC", "ABC");
    }

    @Test
    public void verifyJsonWithJsonBuilder() throws JMSException {
        final Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfigProps.put("mq.message.builder",
                "com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "ABC", "\"ABC\"");
    }

    private void verifyMessageConversion(final Map<String, String> connectorProps, final Schema inputSchema,
            final Object input, final String expectedOutput) throws JMSException {
        final MQSinkTask newConnectTask = new MQSinkTask();

        // start the task so that it connects to MQ
        newConnectTask.start(connectorProps);

        // send test message
        final List<SinkRecord> records = new ArrayList<>();
        records.add(generateSinkRecord(inputSchema, input));
        newConnectTask.put(records);

        // flush the message
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        final OffsetAndMetadata offset = new OffsetAndMetadata(commonOffset);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages was successfully submitted to MQ
        final List<Message> messagesInMQ = getAllMessagesFromQueue(DEFAULT_SINK_QUEUE_NAME);
        assertEquals(1, messagesInMQ.size());
        final TextMessage txtMessage = (TextMessage) messagesInMQ.get(0);
        final String output = txtMessage.getText();
        assertEquals(expectedOutput, output);
    }
}
