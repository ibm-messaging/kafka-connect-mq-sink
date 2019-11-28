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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(MQSinkConnector.class);

    public static final String CONFIG_GROUP_MQ = "mq";

    public static final String CONFIG_NAME_MQ_QUEUE_MANAGER = "mq.queue.manager";
    public static final String CONFIG_DOCUMENTATION_MQ_QUEUE_MANAGER = "The name of the MQ queue manager.";
    public static final String CONFIG_DISPLAY_MQ_QUEUE_MANAGER = "Queue manager";

    public static final String CONFIG_NAME_MQ_CONNECTION_MODE = "mq.connection.mode";
    public static final String CONFIG_DOCUMENTATION_MQ_CONNECTION_MODE = "The connection mode - bindings or client.";
    public static final String CONFIG_DISPLAY_MQ_CONNECTION_MODE = "Connection mode";
    public static final String CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT = "client";
    public static final String CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS = "bindings";

    public static final String CONFIG_NAME_MQ_CONNECTION_NAME_LIST = "mq.connection.name.list";
    public static final String CONFIG_DOCUMENTATION_MQ_CONNNECTION_NAME_LIST = "A list of one or more host(port) entries for connecting to the queue manager. Entries are separated with a comma.";
    public static final String CONFIG_DISPLAY_MQ_CONNECTION_NAME_LIST = "List of connection names for queue manager";

    public static final String CONFIG_NAME_MQ_CHANNEL_NAME = "mq.channel.name";
    public static final String CONFIG_DOCUMENTATION_MQ_CHANNEL_NAME = "The name of the server-connection channel.";
    public static final String CONFIG_DISPLAY_MQ_CHANNEL_NAME = "Channel name";

    public static final String CONFIG_NAME_MQ_QUEUE = "mq.queue";
    public static final String CONFIG_DOCUMENTATION_MQ_QUEUE = "The name of the target MQ queue.";
    public static final String CONFIG_DISPLAY_MQ_QUEUE = "Target queue";

    public static final String CONFIG_NAME_MQ_USER_NAME = "mq.user.name";
    public static final String CONFIG_DOCUMENTATION_MQ_USER_NAME = "The user name for authenticating with the queue manager.";
    public static final String CONFIG_DISPLAY_MQ_USER_NAME = "User name";

    public static final String CONFIG_NAME_MQ_PASSWORD = "mq.password"; 
    public static final String CONFIG_DOCUMENTATION_MQ_PASSWORD = "The password for authenticating with the queue manager.";
    public static final String CONFIG_DISPLAY_MQ_PASSWORD = "Password";

    public static final String CONFIG_NAME_MQ_CCDT_URL = "mq.ccdt.url"; 
    public static final String CONFIG_DOCUMENTATION_MQ_CCDT_URL = "The CCDT URL to use to establish a connection to the queue manager.";
    public static final String CONFIG_DISPLAY_MQ_CCDT_URL = "CCDT URL";

    public static final String CONFIG_NAME_MQ_MESSAGE_BUILDER = "mq.message.builder";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER = "The class used to build the MQ messages.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BUILDER = "Message builder";

    public static final String CONFIG_NAME_MQ_MESSAGE_BODY_JMS = "mq.message.body.jms"; 
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BODY_JMS = "Whether to generate the message body as a JMS message type.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BODY_JMS = "Message body as JMS";

    public static final String CONFIG_NAME_MQ_TIME_TO_LIVE = "mq.time.to.live"; 
    public static final String CONFIG_DOCUMENTATION_MQ_TIME_TO_LIVE = "Time-to-live in milliseconds for messages sent to MQ.";
    public static final String CONFIG_DISPLAY_MQ_TIME_TO_LIVE = "Message time-to-live (ms)";

    public static final String CONFIG_NAME_MQ_PERSISTENT = "mq.persistent"; 
    public static final String CONFIG_DOCUMENTATION_MQ_PERSISTENT = "Send persistent or non-persistent messages to MQ.";
    public static final String CONFIG_DISPLAY_MQ_PERSISTENT = "Send persistent messages";

    public static final String CONFIG_NAME_MQ_SSL_CIPHER_SUITE = "mq.ssl.cipher.suite"; 
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE = "The name of the cipher suite for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE = "SSL cipher suite";

    public static final String CONFIG_NAME_MQ_SSL_PEER_NAME = "mq.ssl.peer.name"; 
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME = "The distinguished name pattern of the TLS (SSL) peer.";
    public static final String CONFIG_DISPLAY_MQ_SSL_PEER_NAME = "SSL peer name";

    public static final String CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION = "mq.ssl.keystore.location";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_LOCATION = "The path to the JKS keystore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_KEYSTORE_LOCATION = "SSL keystore location";

    public static final String CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD = "mq.ssl.keystore.password";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_PASSWORD = "The password of the JKS keystore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_KEYSTORE_PASSWORD = "SSL keystore password";

    public static final String CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION = "mq.ssl.truststore.location";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_LOCATION = "The path to the JKS truststore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_LOCATION = "SSL truststore location";

    public static final String CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD = "mq.ssl.truststore.password";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_PASSWORD = "The password of the JKS truststore to use for the TLS (SSL) connection.";
    public static final String CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_PASSWORD = "SSL truststore password";

    public static final String CONFIG_NAME_MQ_MESSAGE_BUILDER_KEY_HEADER = "mq.message.builder.key.header";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_KEY_HEADER = "The JMS message header to set from the Kafka record key.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_KEY_HEADER = "Record builder key header";
    public static final String CONFIG_VALUE_MQ_MESSAGE_BUILDER_KEY_HEADER_JMSCORRELATIONID = "JMSCorrelationID";

    public static final String CONFIG_NAME_MQ_MESSAGE_BUILDER_VALUE_CONVERTER = "mq.message.builder.value.converter";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_VALUE_CONVERTER = "Prefix for configuring message builder's value converter.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_VALUE_CONVERTER = "Message builder's value converter";

    public static final String CONFIG_NAME_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY = "mq.message.builder.topic.property";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY = "The JMS message property to set from the Kafka topic.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY = "Kafka topic message property";

    public static final String CONFIG_NAME_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY = "mq.message.builder.partition.property";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY = "The JMS message property to set from the Kafka partition.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY = "Kafka partition message property";

    public static final String CONFIG_NAME_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY = "mq.message.builder.offset.property";
    public static final String CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY = "The JMS message property to set from the Kafka offset.";
    public static final String CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY = "Kafka offset message property";

    public static final String CONFIG_NAME_MQ_REPLY_QUEUE = "mq.reply.queue";
    public static final String CONFIG_DOCUMENTATION_MQ_REPLY_QUEUE = "The name of the reply-to queue, as a queue name or URI.";
    public static final String CONFIG_DISPLAY_MQ_REPLY_QUEUE = "Reply-to queue";

    public static final String CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP = "mq.user.authentication.mqcsp";
    public static final String CONFIG_DOCUMENTATION_MQ_USER_AUTHENTICATION_MQCSP = "Whether to use MQ connection security parameters (MQCSP).";
    public static final String CONFIG_DISPLAY_MQ_USER_AUTHENTICATION_MQCSP = "User authentication using MQCSP";

    public static String VERSION = "1.2.0-SNAPSHOT";

    private Map<String, String> configProps;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return VERSION;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override public void start(Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        configProps = props;
        for (final Entry<String, String> entry: props.entrySet()) {
            String value;
            if (entry.getKey().toLowerCase().contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Connector props entry {} : {}", entry.getKey(), value);
        }

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override public Class<? extends Task> taskClass() {
        return MQSinkTask.class;
    }   

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.trace("[{}] Entry {}.taskConfigs, maxTasks={}", Thread.currentThread().getId(), this.getClass().getName(), maxTasks);

        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++)
        {
            taskConfigs.add(configProps);
        }

        log.trace("[{}]  Exit {}.taskConfigs, retval={}", Thread.currentThread().getId(), this.getClass().getName(), taskConfigs);
        return taskConfigs;
    }

    /**
     * Stop this connector.
     */
    @Override public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());
        log.trace("[{}]  Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector.
     */
    @Override public ConfigDef config() {
        ConfigDef config = new ConfigDef();

        config.define(CONFIG_NAME_MQ_QUEUE_MANAGER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                      CONFIG_DOCUMENTATION_MQ_QUEUE_MANAGER, CONFIG_GROUP_MQ, 1, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_QUEUE_MANAGER);

        config.define(CONFIG_NAME_MQ_CONNECTION_MODE, Type.STRING, CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT, 
                      ConfigDef.ValidString.in(CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT,
                                               CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS),
                      Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_CONNECTION_MODE, CONFIG_GROUP_MQ, 2, Width.SHORT,
                      CONFIG_DISPLAY_MQ_CONNECTION_MODE);

        config.define(CONFIG_NAME_MQ_CONNECTION_NAME_LIST, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_CONNNECTION_NAME_LIST, CONFIG_GROUP_MQ, 3, Width.LONG,
                      CONFIG_DISPLAY_MQ_CONNECTION_NAME_LIST);

        config.define(CONFIG_NAME_MQ_CHANNEL_NAME, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_CHANNEL_NAME, CONFIG_GROUP_MQ, 4, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_CHANNEL_NAME);

        config.define(CONFIG_NAME_MQ_CCDT_URL, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_CCDT_URL, CONFIG_GROUP_MQ, 5, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_CCDT_URL);

        config.define(CONFIG_NAME_MQ_QUEUE, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                      CONFIG_DOCUMENTATION_MQ_QUEUE, CONFIG_GROUP_MQ, 6, Width.LONG,
                      CONFIG_DISPLAY_MQ_QUEUE);

        config.define(CONFIG_NAME_MQ_USER_NAME, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_USER_NAME, CONFIG_GROUP_MQ, 7, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_USER_NAME);

        config.define(CONFIG_NAME_MQ_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_PASSWORD, CONFIG_GROUP_MQ, 8, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_PASSWORD);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER, CONFIG_GROUP_MQ, 9, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_MESSAGE_BUILDER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BODY_JMS, Type.BOOLEAN, Boolean.FALSE, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BODY_JMS, CONFIG_GROUP_MQ, 10, Width.SHORT,
                      CONFIG_DISPLAY_MQ_MESSAGE_BODY_JMS);

        config.define(CONFIG_NAME_MQ_TIME_TO_LIVE, Type.LONG, 0, Range.between(0L, 99999999900L), Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_TIME_TO_LIVE, CONFIG_GROUP_MQ, 11, Width.SHORT,
                      CONFIG_DISPLAY_MQ_TIME_TO_LIVE);

        config.define(CONFIG_NAME_MQ_PERSISTENT, Type.BOOLEAN, "true", Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_PERSISTENT, CONFIG_GROUP_MQ, 12, Width.SHORT,
                      CONFIG_DISPLAY_MQ_PERSISTENT);

        config.define(CONFIG_NAME_MQ_SSL_CIPHER_SUITE, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE, CONFIG_GROUP_MQ, 13, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE);

        config.define(CONFIG_NAME_MQ_SSL_PEER_NAME, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME, CONFIG_GROUP_MQ, 14, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_SSL_PEER_NAME);

        config.define(CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_LOCATION, CONFIG_GROUP_MQ, 15, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_SSL_KEYSTORE_LOCATION);

        config.define(CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_PASSWORD, CONFIG_GROUP_MQ, 16, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_SSL_KEYSTORE_PASSWORD);

        config.define(CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_LOCATION, CONFIG_GROUP_MQ, 17, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_LOCATION);

        config.define(CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_PASSWORD, CONFIG_GROUP_MQ, 18, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_PASSWORD);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_KEY_HEADER, Type.STRING, null, Importance.MEDIUM,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_KEY_HEADER, CONFIG_GROUP_MQ, 19, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_KEY_HEADER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_VALUE_CONVERTER, Type.STRING, null, Importance.LOW,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_VALUE_CONVERTER, CONFIG_GROUP_MQ, 20, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_VALUE_CONVERTER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY, Type.STRING, null, Importance.LOW,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY, CONFIG_GROUP_MQ, 21, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY, Type.STRING, null, Importance.LOW,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY, CONFIG_GROUP_MQ, 22, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY, Type.STRING, null, Importance.LOW,
                      CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY, CONFIG_GROUP_MQ, 23, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY);

        config.define(CONFIG_NAME_MQ_REPLY_QUEUE, Type.STRING, null, Importance.LOW,
                      CONFIG_DOCUMENTATION_MQ_REPLY_QUEUE, CONFIG_GROUP_MQ, 24, Width.MEDIUM,
                      CONFIG_DISPLAY_MQ_REPLY_QUEUE);

        config.define(CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP, Type.BOOLEAN, Boolean.TRUE, Importance.LOW,
                      CONFIG_DOCUMENTATION_MQ_USER_AUTHENTICATION_MQCSP, CONFIG_GROUP_MQ, 25, Width.SHORT,
                      CONFIG_DISPLAY_MQ_USER_AUTHENTICATION_MQCSP);

        return config;
    }
}