/**
 * Copyright 2017, 2020, 2023 IBM Corporation
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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class MQSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MQSinkTask.class);

    protected JMSWorker worker;

    protected long retryBackoffMs = 60000;
    private boolean isExactlyOnceMode = false;

    // private ErrantRecordReporter reporter;
    private Map<String, String> connectorConfigProps;

    private HashMap<String, String> lastCommittedOffsetMap;

    public MQSinkTask() {
    }

    // visible for testing.
    MQSinkTask(final MQSinkConfig connectorConfig, final SinkTaskContext context) throws Exception {
        this.context = context;
    }

    /**
     * Get the version of this task. Usually this should be the same as the
     * corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return MQSinkConnector.version;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time
     * setup of the task.
     *
     * @param props
     *              initial configuration
     */
    @Override
    public void start(final Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        try {
            this.isExactlyOnceMode = MQSinkConnector.configSupportsExactlyOnce(props);
            if (this.isExactlyOnceMode) {
                log.info("Exactly-once mode enabled");
            }
            connectorConfigProps = props;
            logConfiguration(props);
            setRetryBackoff(props);
            // Construct a worker to interface with MQ
            worker = newJMSWorker();
            worker.configure(connectorConfigProps);
            // Make a connection as an initial test of the configuration
            worker.connect();
        } catch (JMSRuntimeException | JMSWorkerConnectionException e) {
            log.error("MQ Connection Exception: ", e);
            stop();
            throw new ConnectException(e);
        } catch (final ConnectException e) {
            log.error("Unexpected connect exception: ", e);
            stop();
            throw e;
        } catch (final RuntimeException e) {
            log.error("Unexpected runtime exception: ", e);
            stop();
            throw e;
        }

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink
     * asynchronously and immediately return.
     *
     * If this operation fails, the SinkTask may throw a
     * {@link org.apache.kafka.connect.errors.RetriableException} to indicate that
     * the framework should attempt to retry the same call again. Other exceptions
     * will cause the task to be stopped immediately.
     * {@link SinkTaskContext#timeout(long)} can be used to set the maximum time
     * before the batch will be retried.
     *
     * @param records
     *                the set of records to send
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        log.trace("[{}] Entry {}.put, records.size={}", Thread.currentThread().getId(), this.getClass().getName(),
                records.size());
        try {
            try {
                if (isExactlyOnceMode) {
                    putExactlyOnce(records);
                } else {
                    putAtLeastOnce(records);
                }
            } catch (final JsonProcessingException jpe) {
                maybeCloseAllWorkers(jpe);
                throw new ConnectException(jpe);
            } catch (final JMSRuntimeException | JMSException e) {
                log.error("JMS Exception: ", e);
                maybeCloseAllWorkers(e);
                throw ExceptionProcessor.handleException(e);
            } catch (final ConnectException e) {
                log.error("Unexpected connect exception: ", e);
                maybeCloseAllWorkers(e);
                throw e;
            } catch (final RuntimeException e) {
                log.error("Unexpected runtime exception: ", e);
                maybeCloseAllWorkers(e);
                throw e;
            }
        } catch (final RetriableException rte) {
            context.timeout(retryBackoffMs);
            throw rte;
        }
        log.trace("[{}]  Exit {}.put", Thread.currentThread().getId(), this.getClass().getName());
    }

    private void putExactlyOnce(final Collection<SinkRecord> records) throws JsonProcessingException, JMSException {
        lastCommittedOffsetMap = worker.readFromStateQueue().orElse(new HashMap<>());
        for (final SinkRecord record : records) {
            final TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
            if (isRecordAlreadyCommitted(record, topicPartition)) {
                log.debug(
                        "Skipping record for topic {}, partition {} and offset {} as it has already been committed",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset());
                continue;
            }
            log.debug("Putting record for topic {}, partition {} and offset {}", record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset());
            worker.send(record);
            lastCommittedOffsetMap.put(topicPartition.toString(), String.valueOf(record.kafkaOffset()));
        }

        worker.writeLastRecordOffsetToStateQueue(lastCommittedOffsetMap);
        worker.commit();
    }

    private boolean isRecordAlreadyCommitted(final SinkRecord record, final TopicPartition topicPartition) {
        final long lastCommittedOffset = Long.parseLong(lastCommittedOffsetMap
                .getOrDefault(topicPartition.toString(), "-1"));
        if (record.kafkaOffset() <= lastCommittedOffset) {
            return true;
        }
        return false;
    }

    private void putAtLeastOnce(final Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            log.debug("Putting record for topic {}, partition {} and offset {}", record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset());
            worker.send(record);
        }
        worker.commit();
    }

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified
     * topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to
     *                       {@link #put(Collection)}}, provided for convenience but
     *                       could also be determined by tracking all offsets
     *                       included in the {@link SinkRecord}s passed to
     *                       {@link #put}.
     */
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        log.trace("[{}] Entry {}.flush", Thread.currentThread().getId(), this.getClass().getName());

        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
            final TopicPartition tp = entry.getKey();
            final OffsetAndMetadata om = entry.getValue();
            log.debug("Flushing up to topic {}, partition {} and offset {}", tp.topic(), tp.partition(), om.offset());
        }

        log.trace("[{}]  Exit {}.flush", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked
     * only once outstanding calls to other methods have completed (e.g.,
     * {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and
     * offset commit has completed. Implementations of this method should only need
     * to perform final cleanup operations, such as closing network connections to
     * the sink system.
     */
    @Override
    public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());

        if (worker != null) {
            worker.close();
        }

        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    /** Create a new JMSWorker. */
    protected JMSWorker newJMSWorker() {
        // Construct a worker to interface with MQ
        final JMSWorker worker = new JMSWorker();
        return worker;
    }

    /**
     * Set the retry backoff time.
     *
     * @param props
     *              the configuration properties
     */
    private void setRetryBackoff(final Map<String, String> props) {
        // check if a custom retry time is provided
        final String retryBackoffMsStr = props.get(MQSinkConfig.CONFIG_NAME_MQ_RETRY_BACKOFF_MS);
        if (retryBackoffMsStr != null) {
            retryBackoffMs = Long.parseLong(retryBackoffMsStr);
        }
        log.debug("Setting retry backoff {}", retryBackoffMs);
    }

    /**
     * Log the configuration properties.
     *
     * @param props
     *              the configuration properties
     */
    private void logConfiguration(final Map<String, String> props) {
        for (final Entry<String, String> entry : props.entrySet()) {
            final String value;
            if (entry.getKey().toLowerCase(Locale.ENGLISH).contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Task props entry {} : {}", entry.getKey(), value);
        }
    }

    private void maybeCloseAllWorkers(final Throwable exc) {
        log.debug(" Checking to see if the failed connection should be closed.");
        if (ExceptionProcessor.isClosable(exc)) {
            stop();
        }
    }

    protected SinkTaskContext getContext() {
        return this.context;
    }
}
