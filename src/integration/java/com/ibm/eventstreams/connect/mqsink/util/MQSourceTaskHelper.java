/**
 * Copyright 2026 IBM Corporation
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
package com.ibm.eventstreams.connect.mqsink.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import com.ibm.eventstreams.connect.mqsource.MQSourceTask;
import com.ibm.eventstreams.connect.mqsource.builders.DefaultRecordBuilder;

/**
 * Starts an {@link MQSourceTask} against the integration-test queue manager.
 */
public final class MQSourceTaskHelper {

    private MQSourceTaskHelper() {
    }

    public static MQSourceTask startSourceTask(final String connectionName, final String sourceQueue,
            final String kafkaTopic, final boolean mqmdRead) {
        final MQSourceTask sourceTask = new MQSourceTask();
        sourceTask.initialize(emptyOffsetContext());
        sourceTask.start(sourceProperties(connectionName, sourceQueue, kafkaTopic, mqmdRead));
        return sourceTask;
    }

    public static void putMessage(final JMSContext context, final String queueName, final Message message)
            throws JMSException {
        JmsMqPutHelper.putMessage(context, queueName, message);
    }

    public static SourceRecord pollSingleRecord(final MQSourceTask sourceTask) throws InterruptedException {
        final List<SourceRecord> records = sourceTask.poll();
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("MQ source task returned no records");
        }
        return records.get(0);
    }

    public static void commit(final MQSourceTask sourceTask, final SourceRecord record) throws InterruptedException {
        sourceTask.commitRecord(record, null);
    }

    private static Map<String, String> sourceProperties(final String connectionName, final String sourceQueue,
            final String kafkaTopic, final boolean mqmdRead) {
        final Map<String, String> props = new HashMap<>();
        props.put("mq.queue.manager", "MYQMGR");
        props.put("mq.connection.mode", "client");
        props.put("mq.connection.name.list", connectionName);
        props.put("mq.channel.name", "DEV.APP.SVRCONN");
        props.put("mq.queue", sourceQueue);
        props.put("mq.user.authentication.mqcsp", "false");
        props.put("mq.message.body.jms", "true");
        props.put("mq.jms.properties.copy.to.kafka.headers", "true");
        props.put("mq.record.builder", DefaultRecordBuilder.class.getCanonicalName());
        props.put("topic", kafkaTopic);
        props.put("mq.message.receive.timeout", "5000");
        props.put("mq.receive.subsequent.timeout.ms", "2000");
        props.put("mq.reconnect.delay.min.ms", "100");
        props.put("mq.reconnect.delay.max.ms", "10000");
        if (mqmdRead) {
            props.put("mq.message.mqmd.read", "true");
        }
        return props;
    }

    private static SourceTaskContext emptyOffsetContext() {
        final SourceTaskContext context = mock(SourceTaskContext.class);
        final OffsetStorageReader offsetReader = mock(OffsetStorageReader.class);
        when(offsetReader.offset(any())).thenReturn(Collections.emptyMap());
        when(context.offsetStorageReader()).thenReturn(offsetReader);
        return context;
    }
}
