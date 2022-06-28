package com.ibm.eventstreams.connect.mqsink;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class SimpleMQSinkTaskIT extends AbstractJMSContextIT {

    private static final String TOPIC = "SINK.TOPIC.NAME";
    private static final int PARTITION = 3;
    private long OFFSET = 0;


    private SinkRecord generateSinkRecord(Schema valueSchema, Object value) {
        return new SinkRecord(TOPIC, PARTITION,
                null, null,
                valueSchema, value,
                OFFSET++);
    }


    private Map<String, String> createDefaultConnectorProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", getQmgrName());
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", getConnectionName());
        props.put("mq.channel.name", getChannelName());
        props.put("mq.queue", "DEV.QUEUE.1");
        props.put("mq.user.authentication.mqcsp", "false");
        return props;
    }


    @Test
    public void verifyStringMessages() throws JMSException {
        MQSinkTask newConnectTask = new MQSinkTask();

        // configure a sink task for string messages
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test messages
        List<SinkRecord> records = new ArrayList<>();
        records.add(generateSinkRecord(null, "hello"));
        records.add(generateSinkRecord(null, "world"));
        newConnectTask.put(records);

        // flush the messages
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        OffsetAndMetadata offset = new OffsetAndMetadata(OFFSET);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages were successfully submitted to MQ
        List<Message> messagesInMQ = getAllMessagesFromQueue("DEV.QUEUE.1");
        assertEquals(2, messagesInMQ.size());
        assertEquals("hello", messagesInMQ.get(0).getBody(String.class));
        assertEquals("world", messagesInMQ.get(1).getBody(String.class));
    }

    @Test
    public void verifyStringJmsMessages() throws JMSException {
        MQSinkTask newConnectTask = new MQSinkTask();

        String topicProperty = "PutTopicNameHere";
        String partitionProperty = "PutTopicPartitionHere";
        String offsetProperty = "PutOffsetHere";

        // configure a sink task for string messages
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder");
        connectorConfigProps.put("mq.message.body.jms", "true");
        connectorConfigProps.put("mq.message.builder.topic.property", topicProperty);
        connectorConfigProps.put("mq.message.builder.partition.property", partitionProperty);
        connectorConfigProps.put("mq.message.builder.offset.property", offsetProperty);
        connectorConfigProps.put("mq.message.builder.key.header", "JMSCorrelationID");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test messages
        List<SinkRecord> records = new ArrayList<>();
        records.add(new SinkRecord(TOPIC, PARTITION,
                                   Schema.STRING_SCHEMA, "key0",
                                   null, "hello",
                                   OFFSET++));
        records.add(new SinkRecord(TOPIC, PARTITION,
                                   Schema.STRING_SCHEMA, "key1",
                                   null, "world",
                                   OFFSET++));
        newConnectTask.put(records);

        // flush the messages
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        OffsetAndMetadata offset = new OffsetAndMetadata(OFFSET);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages were successfully submitted to MQ
        List<Message> messagesInMQ = getAllMessagesFromQueue("DEV.QUEUE.1");
        assertEquals(2, messagesInMQ.size());
        assertEquals("hello", messagesInMQ.get(0).getBody(String.class));
        assertEquals("world", messagesInMQ.get(1).getBody(String.class));

        // verify that the message origin details were added to message properties
        for (int i = 0; i < 2; i++) {
            assertEquals(TOPIC, messagesInMQ.get(i).getStringProperty(topicProperty));
            assertEquals(PARTITION, messagesInMQ.get(i).getIntProperty(partitionProperty));
            assertEquals("key" + i, messagesInMQ.get(i).getJMSCorrelationID());
        }
        assertEquals(OFFSET - 2, messagesInMQ.get(0).getLongProperty(offsetProperty));
        assertEquals(OFFSET - 1, messagesInMQ.get(1).getLongProperty(offsetProperty));
    }


    @Test
    public void verifyJsonMessages() throws JMSException {
        MQSinkTask newConnectTask = new MQSinkTask();

        // configure a sink task for JSON messages
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.JsonConverter");

        // start the task so that it connects to MQ
        newConnectTask.start(connectorConfigProps);

        // create some test messages
        Schema fruitSchema = SchemaBuilder.struct()
                .name("com.ibm.eventstreams.tests.Fruit")
                .field("fruit", Schema.STRING_SCHEMA)
                .build();
        Struct apple = new Struct(fruitSchema).put("fruit", "apple");
        Struct banana = new Struct(fruitSchema).put("fruit", "banana");
        Struct pear = new Struct(fruitSchema).put("fruit", "pear");

        // give the test messages to the sink task
        List<SinkRecord> records = new ArrayList<>();
        records.add(generateSinkRecord(fruitSchema, apple));
        records.add(generateSinkRecord(fruitSchema, banana));
        records.add(generateSinkRecord(fruitSchema, pear));
        newConnectTask.put(records);

        // flush the messages
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        OffsetAndMetadata offset = new OffsetAndMetadata(OFFSET);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages were successfully submitted to MQ
        List<Message> messagesInMQ = getAllMessagesFromQueue("DEV.QUEUE.1");
        assertEquals(3, messagesInMQ.size());
        assertEquals("{\"fruit\":\"apple\"}", messagesInMQ.get(0).getBody(String.class));
        assertEquals("{\"fruit\":\"banana\"}", messagesInMQ.get(1).getBody(String.class));
        assertEquals("{\"fruit\":\"pear\"}", messagesInMQ.get(2).getBody(String.class));
    }

    @Test
    public void verifyStringWithDefaultBuilder() throws JMSException {
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.StringConverter");
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder");

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "ABC", "ABC");
    }
    @Test
    public void verifyStringWithJsonBuilder() throws JMSException {
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.StringConverter");
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "ABC", "\"ABC\"");
    }
    @Test
    public void verifyJsonWithDefaultBuilder() throws JMSException {
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder");

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "\"ABC\"", "\"ABC\"");
    }
    @Test
    public void verifyJsonWithJsonBuilder() throws JMSException {
        Map<String, String> connectorConfigProps = createDefaultConnectorProperties();
        connectorConfigProps.put("mq.message.builder.value.converter", "org.apache.kafka.connect.json.JsonConverter");
        connectorConfigProps.put("mq.message.builder", "com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");

        verifyMessageConversion(connectorConfigProps, Schema.STRING_SCHEMA, "\"ABC\"", "\"\\\"ABC\\\"\"");
    }



    private void verifyMessageConversion(Map<String, String> connectorProps, Schema inputSchema, Object input, String expectedOutput) throws JMSException {
        MQSinkTask newConnectTask = new MQSinkTask();

        // start the task so that it connects to MQ
        newConnectTask.start(connectorProps);

        // send test message
        List<SinkRecord> records = new ArrayList<>();
        records.add(generateSinkRecord(inputSchema, input));
        newConnectTask.put(records);

        // flush the message
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        TopicPartition topic = new TopicPartition(TOPIC, PARTITION);
        OffsetAndMetadata offset = new OffsetAndMetadata(OFFSET);
        offsets.put(topic, offset);
        newConnectTask.flush(offsets);

        // stop the task
        newConnectTask.stop();

        // verify that the messages was successfully submitted to MQ
        List<Message> messagesInMQ = getAllMessagesFromQueue("DEV.QUEUE.1");
        assertEquals(1, messagesInMQ.size());
        TextMessage txtMessage = (TextMessage) messagesInMQ.get(0);
        String output = txtMessage.getText();
        assertEquals(expectedOutput, output);
    }
}
