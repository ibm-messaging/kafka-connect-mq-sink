package com.ibm.eventstreams.connect.mqsink.builders;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.mq.jms.MQQueue;

public class DestinationBuilderIT extends AbstractJMSContextIT {


    @Test
    public void verifyReplyQueueProperty() throws Exception {
        String replyQueue = "queue://QM1/REPLY.Q";

        Map<String, String> props = new HashMap<>();
        props.put("mq.reply.queue", replyQueue);

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, "msg", 0);

        Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("msg", message.getBody(String.class));

        MQQueue destination = (MQQueue) message.getJMSReplyTo();
        assertEquals(replyQueue, destination.getQueueName());
    }


    @Test
    public void verifyTopicNameProperty() throws Exception {
        String topicProperty = "PutTopicNameHere";
        String TOPIC = "MY.TOPIC";

        Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.topic.property", topicProperty);

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, "message", 0);

        Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("message", message.getBody(String.class));
        assertEquals(TOPIC, message.getStringProperty(topicProperty));
    }


    @Test
    public void verifyTopicPartitionProperty() throws Exception {
        String topicProperty = "PutTopicPartitionHere";
        int PARTITION = 4;

        Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.partition.property", topicProperty);

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        SinkRecord record = new SinkRecord("topic", PARTITION, null, null, null, "message", 0);

        Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("message", message.getBody(String.class));
        assertEquals(PARTITION, message.getIntProperty(topicProperty));
    }


    @Test
    public void verifyMessageOffsetProperty() throws Exception {
        String topicProperty = "PutOffsetHere";
        long OFFSET = 91;

        Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.offset.property", topicProperty);

        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        SinkRecord record = new SinkRecord("topic", 0, null, null, null, "message", OFFSET);

        Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("message", message.getBody(String.class));
        assertEquals(OFFSET, message.getLongProperty(topicProperty));
    }
}
