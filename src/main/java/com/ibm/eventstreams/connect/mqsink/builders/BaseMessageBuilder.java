/**
 * Copyright 2018, 2019, 2023, 2024 IBM Corporation
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

import com.ibm.eventstreams.connect.mqsink.MQSinkConfig;

import com.ibm.mq.jms.MQQueue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds JMS messages from Kafka Connect SinkRecords.
 */
public abstract class BaseMessageBuilder implements MessageBuilder {
    private static final Logger log = LoggerFactory.getLogger(BaseMessageBuilder.class);

    public enum KeyHeader { NONE, CORRELATION_ID };
    protected KeyHeader keyheader = KeyHeader.NONE;
    public Destination replyToQueue;
    public String topicPropertyName;
    public String partitionPropertyName;
    public String offsetPropertyName;
    public boolean copyJmsProperties;

    /**
     * MQMD properties that require Integer type according to IBM MQ JMS specification.
     * Includes both JMS_IBM_MQMD_* and JMS_IBM_* variants as MQ Source Connector may use either format.
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     */
    private static final Set<String> INTEGER_MQMD_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_MQMD_Report",
            "JMS_IBM_MQMD_MsgType",
            "JMS_IBM_MQMD_Expiry",
            "JMS_IBM_MQMD_Feedback",
            "JMS_IBM_MQMD_Encoding",
            "JMS_IBM_MQMD_CodedCharSetId",
            "JMS_IBM_MQMD_Priority",
            "JMS_IBM_MQMD_Persistence",
            "JMS_IBM_MQMD_BackoutCount",
            "JMS_IBM_MQMD_PutApplType",
            "JMS_IBM_MQMD_MsgSeqNumber",
            "JMS_IBM_MQMD_Offset",
            "JMS_IBM_MQMD_MsgFlags",
            "JMS_IBM_MQMD_OriginalLength",
            // Non-MQMD prefixed variants (also used by MQ Source Connector)
            "JMS_IBM_Encoding",
            "JMS_IBM_MsgType",
            "JMS_IBM_Priority",
            "JMS_IBM_PutApplType"
    ));

    /**
     * MQMD properties that require String type according to IBM MQ JMS specification.
     */
    private static final Set<String> STRING_MQMD_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_MQMD_Format",
            "JMS_IBM_MQMD_ReplyToQ",
            "JMS_IBM_MQMD_ReplyToQMgr",
            "JMS_IBM_MQMD_UserIdentifier",
            "JMS_IBM_MQMD_ApplIdentityData",
            "JMS_IBM_MQMD_PutApplName",
            "JMS_IBM_MQMD_PutDate",
            "JMS_IBM_MQMD_PutTime",
            "JMS_IBM_MQMD_ApplOriginData"
    ));

    /**
     * MQMD properties that require byte[] (Object) type according to IBM MQ JMS specification.
     */
    private static final Set<String> BYTE_ARRAY_MQMD_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_MQMD_MsgId",
            "JMS_IBM_MQMD_CorrelId",
            "JMS_IBM_MQMD_AccountingToken",
            "JMS_IBM_MQMD_GroupId"
    ));

    /**
     * All MQMD properties that require type-aware handling.
     */
    private static final Set<String> ALL_MQMD_PROPERTIES = new HashSet<>();

    static {
        ALL_MQMD_PROPERTIES.addAll(INTEGER_MQMD_PROPERTIES);
        ALL_MQMD_PROPERTIES.addAll(STRING_MQMD_PROPERTIES);
        ALL_MQMD_PROPERTIES.addAll(BYTE_ARRAY_MQMD_PROPERTIES);
    }


    /**
     * Configure this class.
     *
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    @Override public void configure(final Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        final String kh = props.get(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BUILDER_KEY_HEADER);
        if (kh != null) {
            if (kh.equals(MQSinkConfig.CONFIG_VALUE_MQ_MESSAGE_BUILDER_KEY_HEADER_JMSCORRELATIONID)) {
                keyheader = KeyHeader.CORRELATION_ID;
                log.debug("Setting JMSCorrelationID header field from Kafka record key");
            } else {
                log.debug("Unsupported MQ message builder key header value {}", kh);
                throw new MessageBuilderException("Unsupported MQ message builder key header value");
            }
        }

        final String rtq = props.get(MQSinkConfig.CONFIG_NAME_MQ_REPLY_QUEUE);
        if (rtq != null) {
            try {
                // The queue URI format supports properties, but we only accept "queue://qmgr/queue"
                if (rtq.contains("?")) {
                    throw new MessageBuilderException("Reply-to queue URI must not contain properties");
                } else {
                    replyToQueue = new MQQueue(rtq);
                }
            } catch (final JMSException jmse) {
                throw new MessageBuilderException("Failed to build reply-to queue", jmse);
            }
        }

        final String tpn = props.get(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BUILDER_TOPIC_PROPERTY);
        if (tpn != null) {
            topicPropertyName = tpn;
        }

        final String ppn = props.get(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BUILDER_PARTITION_PROPERTY);
        if (ppn != null) {
            partitionPropertyName = ppn;
        }

        final String opn = props.get(MQSinkConfig.CONFIG_NAME_MQ_MESSAGE_BUILDER_OFFSET_PROPERTY);
        if (opn != null) {
            offsetPropertyName = opn;
        }

        final String copyhdr = props.get(MQSinkConfig.CONFIG_NAME_KAFKA_HEADERS_COPY_TO_JMS_PROPERTIES);
        if (copyhdr != null) {
            copyJmsProperties = Boolean.valueOf(copyhdr);
        }

        log.trace("[{}]  Exit {}.configure", Thread.currentThread().getId(), this.getClass().getName());
    }

    /**
     * Gets the JMS message for the Kafka Connect SinkRecord.
     *
     * @param context            the JMS context to use for building messages
     * @param record             the Kafka Connect SinkRecord
     *
     * @return the JMS message
     */
    public abstract Message getJMSMessage(JMSContext jmsCtxt, SinkRecord record);

    /**
     * Convert a Kafka Connect SinkRecord into a JMS message.
     *
     * @param context            the JMS context to use for building messages
     * @param record             the Kafka Connect SinkRecord
     *
     * @return the JMS message
     */
    @Override public Message fromSinkRecord(final JMSContext jmsCtxt, final SinkRecord record) {
        final Message m = this.getJMSMessage(jmsCtxt, record);

        if (keyheader != KeyHeader.NONE) {
            final Schema s = record.keySchema();
            final Object k = record.key();

            if (k != null) {
                if (s == null) {
                    log.debug("No schema info {}", k);
                    if (k instanceof byte[]) {
                        try {
                            m.setJMSCorrelationIDAsBytes((byte[]) k);
                        } catch (final JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    } else if (k instanceof ByteBuffer) {
                        try {
                            m.setJMSCorrelationIDAsBytes(((ByteBuffer) k).array());
                        } catch (final JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    } else {
                        try {
                            m.setJMSCorrelationID(k.toString());
                        } catch (final JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                } else if (s.type() == Type.BYTES) {
                    if (k instanceof byte[]) {
                        try {
                            m.setJMSCorrelationIDAsBytes((byte[]) k);
                        } catch (final JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    } else if (k instanceof ByteBuffer) {
                        try {
                            m.setJMSCorrelationIDAsBytes(((ByteBuffer) k).array());
                        } catch (final JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                } else if (s.type() == Type.STRING) {
                    try {
                        m.setJMSCorrelationID((String) k);
                    } catch (final JMSException jmse) {
                        throw new ConnectException("Failed to write string", jmse);
                    }
                }
            }
        }

        if (replyToQueue != null) {
            try {
                m.setJMSReplyTo(replyToQueue);
            } catch (final JMSException jmse) {
                throw new ConnectException("Failed to set reply-to queue", jmse);
            }
        }

        if (topicPropertyName != null) {
            try {
                m.setStringProperty(topicPropertyName, record.topic());
            } catch (final JMSException jmse) {
                throw new ConnectException("Failed to set topic property", jmse);
            }
        }

        if (partitionPropertyName != null) {
            try {
                m.setIntProperty(partitionPropertyName, record.kafkaPartition());
            } catch (final JMSException jmse) {
                throw new ConnectException("Failed to set partition property", jmse);
            }
        }

        if (offsetPropertyName != null) {
            try {
                m.setLongProperty(offsetPropertyName, record.kafkaOffset());
            } catch (final JMSException jmse) {
                throw new ConnectException("Failed to set offset property", jmse);
            }
        }

        if (copyJmsProperties) {
            for (Iterator<Header> iterator = record.headers().iterator(); iterator.hasNext();) {
                final Header header = iterator.next();
                try {
                    setJmsProperty(m, header.key(), header.value());
                } catch (final JMSException jmse) {
                    throw new ConnectException("Failed to set header", jmse);
                } catch (final NumberFormatException nfe) {
                    throw new ConnectException("Failed to parse numeric MQMD property: " + header.key(), nfe);
                }
            }
        }

        return m;
    }

      /**
     * Sets a JMS message property with proper type casting for MQMD properties.
     *
     * MQMD properties require specific types according to IBM MQ JMS specification:
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     *
     * This method preserves type information for MQMD properties while converting
     * all other properties to strings for backward compatibility with existing deployments.
     *
     * @param message            the JMS message to set the property on
     * @param key                the property name
     * @param value              the property value (may be null)
     * @throws JMSException      if the property cannot be set
     * @throws ConnectException  if the value type is not compatible with the expected MQMD type
     */
    private void setJmsProperty(final Message message, final String key, final Object value) throws JMSException {
        if (value == null) {
            // Skip null values as JMS properties cannot be null
            log.debug("Skipping null value for property '{}'", key);
            return;
        }

        // Check if this is an MQMD property that requires type-aware handling
        if (INTEGER_MQMD_PROPERTIES.contains(key)) {
            // Handle Integer MQMD properties
            if (value instanceof Integer) {
                message.setIntProperty(key, (Integer) value);
            } else if (value instanceof Number) {
                // Allow conversion from other numeric types (Long, Double, etc.) to Integer
                message.setIntProperty(key, ((Number) value).intValue());
            } else if (value instanceof String) {
                try {
                    message.setIntProperty(key, Integer.parseInt((String) value));
                } catch (final NumberFormatException e) {
                    log.warn("Cannot convert string value '{}' to integer for MQMD property '{}'", value, key);
                    throw new ConnectException("Failed to set MQMD property '" + key +
                            "': expected integer but got '" + value + "'", e);
                }
            } else {
                log.warn("Cannot convert type {} to integer for MQMD property '{}'",
                        value.getClass().getName(), key);
                throw new ConnectException("Failed to set MQMD property '" + key +
                        "': unsupported type " + value.getClass().getName());
            }
        } else if (STRING_MQMD_PROPERTIES.contains(key)) {
            // Handle String MQMD properties
            message.setStringProperty(key, value.toString());
        } else if (BYTE_ARRAY_MQMD_PROPERTIES.contains(key)) {
            // Handle byte[] MQMD properties
            if (value instanceof byte[]) {
                message.setObjectProperty(key, value);
            } else if (value instanceof String) {
                // MQ Source Connector may output byte arrays as Java toString() representations (e.g., "[B@42969b24")
                // These cannot be converted back to actual bytes, so we set an empty byte array
                log.debug("Setting empty byte array for MQMD property '{}' (cannot convert from string: {})", key, value);
                message.setObjectProperty(key, new byte[0]);
            } else {
                log.warn("Cannot convert type {} to byte array for MQMD property '{}'",
                        value.getClass().getName(), key);
                throw new ConnectException("Failed to set MQMD property '" + key +
                        "': expected byte array but got " + value.getClass().getName());
            }
        } else {
            // For non-MQMD properties, convert everything to string for backward compatibility
            message.setStringProperty(key, value.toString());
            log.debug("Set property '{}' as string (non-MQMD): {}", key, value);
        }
    }

}
