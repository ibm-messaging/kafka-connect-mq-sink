/**
 * Copyright 2017, 2018, 2019 IBM Corporation
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

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MQSinkTask.class);

    private JMSWriter writer;

    public MQSinkTask() {
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return MQSinkConnector.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override public void start(Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        for (final Entry<String, String> entry: props.entrySet()) {
            String value;
            if (entry.getKey().toLowerCase().contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Task props entry {} : {}", entry.getKey(), value);
        }

        // Construct a writer to interface with MQ
        writer = new JMSWriter();
        writer.configure(props);

        // Make a connection as an initial test of the configuration
        writer.connect();

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override public void put(Collection<SinkRecord> records) {
        log.trace("[{}] Entry {}.put", Thread.currentThread().getId(), this.getClass().getName());

        for (SinkRecord r: records) {
            log.debug("Putting record for topic {}, partition {} and offset {}", r.topic(), r.kafkaPartition(), r.kafkaOffset());
            writer.send(r);
        }

        context.requestCommit();
        log.trace("[{}]  Exit {}.put", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link SinkRecord}s
     *                       passed to {@link #put}.
     */
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.trace("[{}] Entry {}.flush", Thread.currentThread().getId(), this.getClass().getName());

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry: currentOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndMetadata om = entry.getValue();
            log.debug("Flushing up to topic {}, partition {} and offset {}", tp.topic(), tp.partition(), om.offset());
        }

        writer.commit();
        log.trace("[{}]  Exit {}.flush", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());

        if (writer != null) {
            writer.close();
        }

        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }
}