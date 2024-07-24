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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(MQSinkConnector.class);

    public static String version = "2.1.0";

    private Map<String, String> configProps;

    /** Get the version of this connector.
     *
     * @return the version, formatted as a String */
    @Override
    public String version() {
        return version;
    }

    /** Start this Connector. This method will only be called on a clean Connector, i.e. it has either just been
     * instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props
     *            configuration settings */
    @Override
    public void start(final Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        configProps = props;
        for (final Entry<String, String> entry : props.entrySet()) {
            final String value;
            if (entry.getKey().toLowerCase(Locale.ENGLISH).contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Connector props entry {} : {}", entry.getKey(), value);
        }

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    /** Returns the Task implementation for this Connector. */
    @Override
    public Class<? extends Task> taskClass() {
        return MQSinkTask.class;
    }

    /** Returns a set of configurations for Tasks based on the current configuration, producing at most count
     * configurations.
     *
     * @param maxTasks
     *            maximum number of configurations to generate
     * @return configurations for Tasks */
    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        log.trace("[{}] Entry {}.taskConfigs, maxTasks={}", Thread.currentThread().getId(), this.getClass().getName(),
                maxTasks);

        final String exactlyOnceStateQueue = configProps.get(MQSinkConfig.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE);
        if (exactlyOnceStateQueue != null && !exactlyOnceStateQueue.isEmpty() && maxTasks > 1) {
            throw new ConnectException(
                    String.format("%s must be empty or not set when maxTasks > 1",
                            MQSinkConfig.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE));
        }
        final List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(configProps);
        }

        log.trace("[{}]  Exit {}.taskConfigs, retval={}", Thread.currentThread().getId(), this.getClass().getName(),
                taskConfigs);
        return taskConfigs;
    }

    /** Stop this connector. */
    @Override
    public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());
        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    /** Define the configuration for the connector.
     *
     * @return The ConfigDef for this connector. */
    @Override
    public ConfigDef config() {
        return MQSinkConfig.config();
    }

    /**
     * Returns true if the supplied connector configuration supports exactly-once semantics.
     * Checks that 'mq.exactly.once.state.queue' property is supplied and is not empty.
     *
     * @param connectorConfig the connector config
     * @return true if 'mq.exactly.once.state.queue' property is supplied and is not empty.
     */
    public static final boolean configSupportsExactlyOnce(final AbstractConfig connectorConfig) {
        // If there is a state queue configured, we can do exactly-once semantics
        final String exactlyOnceStateQueue = connectorConfig.getString(MQSinkConfig.CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE);
        return exactlyOnceStateQueue != null && !exactlyOnceStateQueue.isEmpty();
    }
}
