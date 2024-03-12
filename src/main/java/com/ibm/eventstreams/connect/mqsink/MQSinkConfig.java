/**
 * Copyright 2023 IBM Corporation
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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import com.ibm.eventstreams.connect.mqsink.builders.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;

public class MQSinkConfig {

    public static final Logger log = LoggerFactory.getLogger(MQSinkConfig.class);

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

    public static final String CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE = "mq.exactly.once.state.queue";
    public static final String CONFIG_DOCUMENTATION_MQ_EXACTLY_ONCE_STATE_QUEUE = "The name of the MQ queue used to store the state of the connector when exactly-once delivery is enabled.";
    public static final String CONFIG_DISPLAY_MQ_EXACTLY_ONCE_STATE_QUEUE = "Exactly-once state queue";

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

    public static final String CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS = "mq.ssl.use.ibm.cipher.mappings";
    public static final String CONFIG_DOCUMENTATION_MQ_SSL_USE_IBM_CIPHER_MAPPINGS = "Whether to set system property to control use of IBM cipher mappings.";
    public static final String CONFIG_DISPLAY_MQ_SSL_USE_IBM_CIPHER_MAPPINGS = "Use IBM cipher mappings";

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

    public static final String CONFIG_NAME_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES = "mq.kafka.headers.copy.to.jms.properties";
    public static final String CONFIG_DOCUMENTATION_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES = "Whether to copy Kafka headers to JMS message properties.";
    public static final String CONFIG_DISPLAY_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES = "Copy Kafka headers to JMS message properties";

    public static final String CONFIG_NAME_MQ_RETRY_BACKOFF_MS = "mq.retry.backoff.ms";
    public static final String CONFIG_DOCUMENTATION_MQ_RETRY_BACKOFF_MS = "Time to wait, in milliseconds, before retrying after retriable exceptions";
    public static final String CONFIG_DISPLAY_MQ_RETRY_BACKOFF_MS = "Retry backoff (ms)";

    // https://www.ibm.com/docs/en/ibm-mq/9.3?topic=amffmcja-reading-writing-message-descriptor-from-mq-classes-jms-application
    public static final String CONFIG_NAME_MQ_MQMD_WRITE_ENABLED = "mq.message.mqmd.write";
    public static final String CONFIG_DISPLAY_MQ_MQMD_WRITE_ENABLED = "Enable MQMD Message Writing";
    public static final String CONFIG_DOCUMENTATION_MQ_MQMD_WRITE_ENABLED = "This configuration option determines whether the MQMD structure will be written along with the message data. Enabling this option allows control information to accompany the application data during message transmission between sending and receiving applications. Disabling this option will exclude the MQMD structure from the message payload.";

    // https://www.ibm.com/docs/en/ibm-mq/9.3?topic=application-jms-message-object-properties
    public static final String CONFIG_NAME_MQ_MQMD_MESSAGE_CONTEXT = "mq.message.mqmd.context";
    public static final String CONFIG_DISPLAY_MQ_MQMD_MESSAGE_CONTEXT = "MQMD Message Context";
    public static final String CONFIG_DOCUMENTATION_MQ_MQMD_MESSAGE_CONTEXT = "This configuration option specifies the context in which MQMD properties are applied. Certain properties require this context to be set appropriately for them to take effect. Valid options for WMQ_MQMD_MESSAGE_CONTEXT are IDENTITY for WMQ_MDCTX_SET_IDENTITY_CONTEXT or ALL for WMQ_MDCTX_SET_ALL_CONTEXT.";

    private static final Validator ANY_VALUE_VALID = null;

    public static ConfigDef config() {
        final ConfigDef config = new ConfigDef();

        config.define(CONFIG_NAME_MQ_QUEUE_MANAGER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyStringWithoutControlChars(), Importance.HIGH, CONFIG_DOCUMENTATION_MQ_QUEUE_MANAGER, CONFIG_GROUP_MQ, 1, Width.MEDIUM, CONFIG_DISPLAY_MQ_QUEUE_MANAGER);

        config.define(CONFIG_NAME_MQ_CONNECTION_MODE, Type.STRING, CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT, ConfigDef.ValidString.in(CONFIG_VALUE_MQ_CONNECTION_MODE_CLIENT, CONFIG_VALUE_MQ_CONNECTION_MODE_BINDINGS), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_CONNECTION_MODE, CONFIG_GROUP_MQ, 2, Width.SHORT, CONFIG_DISPLAY_MQ_CONNECTION_MODE);

        config.define(CONFIG_NAME_MQ_CONNECTION_NAME_LIST, Type.STRING, null, ANY_VALUE_VALID, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_CONNNECTION_NAME_LIST, CONFIG_GROUP_MQ, 3, Width.LONG, CONFIG_DISPLAY_MQ_CONNECTION_NAME_LIST);

        config.define(CONFIG_NAME_MQ_CHANNEL_NAME, Type.STRING, null, ANY_VALUE_VALID, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_CHANNEL_NAME, CONFIG_GROUP_MQ, 4, Width.MEDIUM, CONFIG_DISPLAY_MQ_CHANNEL_NAME);

        config.define(CONFIG_NAME_MQ_CCDT_URL, Type.STRING, null, new ValidURL(), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_CCDT_URL, CONFIG_GROUP_MQ, 5, Width.MEDIUM, CONFIG_DISPLAY_MQ_CCDT_URL);

        config.define(CONFIG_NAME_MQ_QUEUE, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyStringWithoutControlChars(), Importance.HIGH, CONFIG_DOCUMENTATION_MQ_QUEUE, CONFIG_GROUP_MQ, 6, Width.LONG, CONFIG_DISPLAY_MQ_QUEUE);

        config.define(CONFIG_NAME_MQ_USER_NAME, Type.STRING, null, ANY_VALUE_VALID, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_USER_NAME, CONFIG_GROUP_MQ, 7, Width.MEDIUM, CONFIG_DISPLAY_MQ_USER_NAME);

        config.define(CONFIG_NAME_MQ_PASSWORD, Type.PASSWORD, null, ANY_VALUE_VALID, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_PASSWORD, CONFIG_GROUP_MQ, 8, Width.MEDIUM, CONFIG_DISPLAY_MQ_PASSWORD);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ValidClass(), Importance.HIGH, CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER, CONFIG_GROUP_MQ, 9, Width.MEDIUM, CONFIG_DISPLAY_MQ_MESSAGE_BUILDER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BODY_JMS, Type.BOOLEAN, Boolean.FALSE, new ConfigDef.NonNullValidator(), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_MESSAGE_BODY_JMS, CONFIG_GROUP_MQ, 10, Width.SHORT, CONFIG_DISPLAY_MQ_MESSAGE_BODY_JMS);

        config.define(CONFIG_NAME_MQ_TIME_TO_LIVE, Type.LONG, 0, Range.between(0L, 99999999900L), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_TIME_TO_LIVE, CONFIG_GROUP_MQ, 11, Width.SHORT, CONFIG_DISPLAY_MQ_TIME_TO_LIVE);

        config.define(CONFIG_NAME_MQ_PERSISTENT, Type.BOOLEAN, Boolean.TRUE, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_PERSISTENT, CONFIG_GROUP_MQ, 12, Width.SHORT, CONFIG_DISPLAY_MQ_PERSISTENT);

        config.define(CONFIG_NAME_MQ_SSL_CIPHER_SUITE, Type.STRING, null, ANY_VALUE_VALID, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_SSL_CIPHER_SUITE, CONFIG_GROUP_MQ, 13, Width.MEDIUM, CONFIG_DISPLAY_MQ_SSL_CIPHER_SUITE);

        config.define(CONFIG_NAME_MQ_SSL_PEER_NAME, Type.STRING, null, ANY_VALUE_VALID, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_SSL_PEER_NAME, CONFIG_GROUP_MQ, 14, Width.MEDIUM, CONFIG_DISPLAY_MQ_SSL_PEER_NAME);

        config.define(CONFIG_NAME_MQ_SSL_KEYSTORE_LOCATION, Type.STRING, null, new ValidFileLocation(), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_LOCATION, CONFIG_GROUP_MQ, 15, Width.MEDIUM, CONFIG_DISPLAY_MQ_SSL_KEYSTORE_LOCATION);

        config.define(CONFIG_NAME_MQ_SSL_KEYSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_SSL_KEYSTORE_PASSWORD, CONFIG_GROUP_MQ, 16, Width.MEDIUM, CONFIG_DISPLAY_MQ_SSL_KEYSTORE_PASSWORD);

        config.define(CONFIG_NAME_MQ_SSL_TRUSTSTORE_LOCATION, Type.STRING, null, new ValidFileLocation(), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_LOCATION, CONFIG_GROUP_MQ, 17, Width.MEDIUM, CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_LOCATION);

        config.define(CONFIG_NAME_MQ_SSL_TRUSTSTORE_PASSWORD, Type.PASSWORD, null, Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_SSL_TRUSTSTORE_PASSWORD, CONFIG_GROUP_MQ, 18, Width.MEDIUM, CONFIG_DISPLAY_MQ_SSL_TRUSTSTORE_PASSWORD);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_KEY_HEADER, Type.STRING, null, ConfigDef.ValidString.in(null, CONFIG_VALUE_MQ_MESSAGE_BUILDER_KEY_HEADER_JMSCORRELATIONID), Importance.MEDIUM, CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_KEY_HEADER, CONFIG_GROUP_MQ, 19, Width.MEDIUM, CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_KEY_HEADER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_VALUE_CONVERTER, Type.STRING, null, new ValidClass(), Importance.LOW, CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_VALUE_CONVERTER, CONFIG_GROUP_MQ, 20, Width.MEDIUM, CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_VALUE_CONVERTER);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY, Type.STRING, null, ANY_VALUE_VALID, Importance.LOW, CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY, CONFIG_GROUP_MQ, 21, Width.MEDIUM, CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY, Type.STRING, null, ANY_VALUE_VALID, Importance.LOW, CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY, CONFIG_GROUP_MQ, 22, Width.MEDIUM, CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY);

        config.define(CONFIG_NAME_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY, Type.STRING, null, ANY_VALUE_VALID, Importance.LOW, CONFIG_DOCUMENTATION_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY, CONFIG_GROUP_MQ, 23, Width.MEDIUM, CONFIG_DISPLAY_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY);

        config.define(CONFIG_NAME_MQ_REPLY_QUEUE, Type.STRING, null, ANY_VALUE_VALID, Importance.LOW, CONFIG_DOCUMENTATION_MQ_REPLY_QUEUE, CONFIG_GROUP_MQ, 24, Width.MEDIUM, CONFIG_DISPLAY_MQ_REPLY_QUEUE);

        config.define(CONFIG_NAME_MQ_USER_AUTHENTICATION_MQCSP, Type.BOOLEAN, Boolean.TRUE, Importance.LOW, CONFIG_DOCUMENTATION_MQ_USER_AUTHENTICATION_MQCSP, CONFIG_GROUP_MQ, 25, Width.SHORT, CONFIG_DISPLAY_MQ_USER_AUTHENTICATION_MQCSP);

        config.define(CONFIG_NAME_MQ_SSL_USE_IBM_CIPHER_MAPPINGS, Type.BOOLEAN, null, Importance.LOW, CONFIG_DOCUMENTATION_MQ_SSL_USE_IBM_CIPHER_MAPPINGS, CONFIG_GROUP_MQ, 26, Width.SHORT, CONFIG_DISPLAY_MQ_SSL_USE_IBM_CIPHER_MAPPINGS);

        config.define(CONFIG_NAME_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES, Type.BOOLEAN, Boolean.FALSE, Importance.LOW, CONFIG_DOCUMENTATION_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES, CONFIG_GROUP_MQ, 27, Width.SHORT, CONFIG_DISPLAY_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES);

        config.define(CONFIG_NAME_MQ_RETRY_BACKOFF_MS, Type.LONG, 60000, Range.between(0L, 99999999900L), Importance.LOW, CONFIG_DOCUMENTATION_MQ_RETRY_BACKOFF_MS, CONFIG_GROUP_MQ, 28, Width.SHORT, CONFIG_DISPLAY_MQ_RETRY_BACKOFF_MS);

        config.define(CONFIG_NAME_MQ_EXACTLY_ONCE_STATE_QUEUE, Type.STRING, null, ANY_VALUE_VALID, Importance.LOW, CONFIG_DOCUMENTATION_MQ_EXACTLY_ONCE_STATE_QUEUE, CONFIG_GROUP_MQ, 29, Width.LONG, CONFIG_DISPLAY_MQ_EXACTLY_ONCE_STATE_QUEUE);

        config.define(CONFIG_NAME_MQ_MQMD_WRITE_ENABLED, Type.BOOLEAN, false, Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_MQMD_WRITE_ENABLED, CONFIG_GROUP_MQ, 30, Width.LONG,
                CONFIG_DISPLAY_MQ_MQMD_WRITE_ENABLED);

        config.define(CONFIG_NAME_MQ_MQMD_MESSAGE_CONTEXT, Type.STRING, null,
                ConfigDef.ValidString.in(null, "identity", "IDENTITY", "all", "ALL"),
                Importance.LOW,
                CONFIG_DOCUMENTATION_MQ_MQMD_MESSAGE_CONTEXT, CONFIG_GROUP_MQ, 31, Width.LONG,
                CONFIG_DISPLAY_MQ_MQMD_MESSAGE_CONTEXT);
        return config;
    }


    private static class ValidURL implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String stringValue = (String) value;
            if (stringValue == null || stringValue.isEmpty()) {
                // URLs are optional values
                return;
            }

            try {
                new URL(stringValue);

            } catch (final MalformedURLException exception) {
                throw new ConfigException(name, value, "Value must be a URL for a CCDT file");
            }
        }
    }

    private static class ValidClass implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            Class requiredClass = null;
            final String stringValue = (String) value;
            if (name.endsWith("builder")) {
                requiredClass = MessageBuilder.class;
            } else { // converter
                requiredClass = Converter.class;

            }
            if (stringValue == null || stringValue.isEmpty()) {
                return;
            }
            try {
                Class.forName(stringValue).asSubclass(requiredClass).newInstance();
            } catch (final ClassNotFoundException exc) {
                log.error("Failed to validate class {}", stringValue);
                throw new ConfigException(name, value, "Class must be accessible on the classpath for Kafka Connect");
            } catch (final ClassCastException | IllegalAccessException exc) {
                log.error("Failed to validate class {}", stringValue);
                throw new ConfigException(name, value, "Class must be an implementation of " + requiredClass.getCanonicalName());
            } catch (final InstantiationException exc) {
                log.error("Failed to validate class {}", stringValue);
                throw new ConfigException(name, value, "Unable to create an instance of the class");
            } catch (final NullPointerException exc) {
                throw new ConfigException(name, value, "Value must not be null");
            }
        }
    }

    private static class ValidFileLocation implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String stringValue = (String) value;
            if (stringValue == null || stringValue.isEmpty()) {
                // URLs are optional values
                return;
            }
            File f = null;
            try {
                f = new File(stringValue);
            } catch (final Exception exception) {
                throw new ConfigException(name, value, "Value must be a File Location");
            }
            if (!f.isFile()) {
                throw new ConfigException(name, value, "Value must be a File location");
            }
            if (!f.canRead()) {
                throw new ConfigException(name, value, "Value must be a readable file");
            }
        }
    }
}
