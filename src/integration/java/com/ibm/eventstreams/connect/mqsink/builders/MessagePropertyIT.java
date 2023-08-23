/**
 * Copyright 2022, 2023 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsink.builders;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.mq.jms.MQQueue;

public class MessagePropertyIT extends AbstractJMSContextIT {

    @Test
    public void verifyReplyQueueProperty() throws Exception {
        final String replyQueue = "queue://QM1/REPLY.Q";

        final Map<String, String> props = new HashMap<>();
        props.put("mq.reply.queue", replyQueue);

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        final SinkRecord record = new SinkRecord("topic", 0, null, null, null, "msg", 0);

        final Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("msg", message.getBody(String.class));

        final MQQueue destination = (MQQueue) message.getJMSReplyTo();
        assertEquals(replyQueue, destination.getQueueName());
    }

    @Test
    public void verifyTopicNameProperty() throws Exception {
        final String topicProperty = "PutTopicNameHere";
        final String topic = "MY.TOPIC";

        final Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.topic.property", topicProperty);

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        final SinkRecord record = new SinkRecord(topic, 0, null, null, null, "message", 0);

        final Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("message", message.getBody(String.class));
        assertEquals(topic, message.getStringProperty(topicProperty));
    }

    @Test
    public void verifyTopicPartitionProperty() throws Exception {
        final String topicProperty = "PutTopicPartitionHere";
        final int partition = 4;

        final Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.partition.property", topicProperty);

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        final SinkRecord record = new SinkRecord("topic", partition, null, null, null, "message", 0);

        final Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("message", message.getBody(String.class));
        assertEquals(partition, message.getIntProperty(topicProperty));
    }

    @Test
    public void verifyMessageOffsetProperty() throws Exception {
        final String topicProperty = "PutOffsetHere";
        final long offset = 91;

        final Map<String, String> props = new HashMap<>();
        props.put("mq.message.builder.offset.property", topicProperty);

        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.configure(props);

        final SinkRecord record = new SinkRecord("topic", 0, null, null, null, "message", offset);

        final Message message = builder.fromSinkRecord(getJmsContext(), record);
        assertEquals("message", message.getBody(String.class));
        assertEquals(offset, message.getLongProperty(topicProperty));
    }
}
