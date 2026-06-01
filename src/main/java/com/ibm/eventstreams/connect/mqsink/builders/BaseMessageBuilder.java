/**
 * Copyright 2018, 2019, 2023, 2024, 2026 IBM Corporation
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
     *
     * IMPORTANT NOTES about JMS Specification Compliance:
     * - JMS_IBM_MQMD_Priority: Values outside 0-9 range violate JMS specification
     * - These properties are IBM MQ extensions and may not be fully JMS-compliant
     * - The connector passes values through; IBM MQ will validate/reject invalid values
     *
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
     */
    private static final Set<String> MQMD_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
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
            "JMS_IBM_MQMD_OriginalLength"
    ));

    /**
     * MQMD properties that require String type according to IBM MQ JMS specification.
     */
    private static final Set<String> MQMD_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
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
     * MQMD byte array properties that should be skipped when setting JMS properties.
     *
     * NOTE: According to IBM MQ documentation, "The use of byte array properties on a message
     * violates the JMS specification." These properties should be set through the MQMD API
     * (MessageDescriptor) rather than as JMS properties.
     *
     * These properties are explicitly skipped to avoid JMS spec violations. Use MessageDescriptorBuilder
     * with mq.message.mqmd.write=true and mq.message.mqmd.context=ALL to set these fields properly.
     *
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     */
    private static final Set<String> MQMD_BYTES_PROPERTIES_TO_SKIP = new HashSet<>(Arrays.asList(
            "JMS_IBM_MQMD_MsgId",
            "JMS_IBM_MQMD_CorrelId",
            "JMS_IBM_MQMD_AccountingToken",
            "JMS_IBM_MQMD_GroupId"
    ));

    /**
     * JMS_IBM properties (non-MQMD) that require Integer type.
     * These are standard JMS properties that map to MQMD fields and can be set by applications.
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
     *
     * Note: Many JMS_IBM properties are read-only (set by IBM MQ) and cannot be set by applications.
     * Only settable properties are included here.
     * Note: JMS_IBM_MsgId and JMS_IBM_CorrelId are read-only and cannot be set directly.
     * JMS_IBM_ConnectionId is also read-only.
     */
    private static final Set<String> JMS_IBM_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_Report_Exception",
            "JMS_IBM_Report_Expiration",
            "JMS_IBM_Report_COA",
            "JMS_IBM_Report_COD",
            "JMS_IBM_Report_PAN",
            "JMS_IBM_Report_NAN",
            "JMS_IBM_Report_Pass_Msg_ID",
            "JMS_IBM_Report_Pass_Correl_ID",
            "JMS_IBM_Report_Discard_Msg",
            "JMS_IBM_MsgType",
            "JMS_IBM_Feedback",
            "JMS_IBM_Encoding",
            "JMS_IBM_PutApplType"
    ));

    /**
     * JMS_IBM properties (non-MQMD) that require String type.
     * Note: JMS_IBM_PutAppl, JMS_IBM_PutDate, JMS_IBM_PutTime are read-only and cannot be set.
     */
    private static final Set<String> JMS_IBM_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_Format",
            "JMS_IBM_Character_Set"
    ));

    /**
     * JMS_IBM properties (non-MQMD) that require Boolean type.
     */
    private static final Set<String> JMS_IBM_BOOLEAN_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_Last_Msg_In_Group"
    ));

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
     * Sets a JMS message property with proper type casting for IBM MQ properties.
     *
     * IBM MQ properties require specific types according to IBM MQ JMS specification:
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
     *
     * This method preserves type information for IBM MQ properties while converting
     * all other properties to strings for backward compatibility with existing deployments.
     *
     * @param message            the JMS message to set the property on
     * @param key                the property name
     * @param value              the property value (may be null)
     * @throws JMSException      if the property cannot be set
     * @throws ConnectException  if the value type is not compatible with the expected property type
     */
    private void setJmsProperty(final Message message, final String key, final Object value) throws JMSException {
        if (value == null) {
            // Skip null values as JMS properties cannot be null
            log.debug("Skipping null value for property '{}'", key);
            return;
        }

        // Check if this is an IBM MQ property that requires type-aware handling
        if (MQMD_INTEGER_PROPERTIES.contains(key) || JMS_IBM_INTEGER_PROPERTIES.contains(key)) {
            // Handle Integer properties (both MQMD and JMS_IBM)
            if (value instanceof Integer) {
                message.setIntProperty(key, (Integer) value);
            } else if (value instanceof Number) {
                // Allow conversion from other numeric types (Long, Double, etc.) to Integer
                message.setIntProperty(key, ((Number) value).intValue());
            } else if (value instanceof String) {
                try {
                    message.setIntProperty(key, Integer.parseInt((String) value));
                } catch (final NumberFormatException e) {
                    log.warn("Cannot convert string value '{}' to integer for property '{}'", value, key);
                    throw new ConnectException("Failed to set property '" + key +
                            "': expected integer but got '" + value + "'", e);
                }
            } else {
                log.warn("Cannot convert type {} to integer for property '{}'",
                        value.getClass().getName(), key);
                throw new ConnectException("Failed to set property '" + key +
                        "': unsupported type " + value.getClass().getName());
            }
        } else if (MQMD_STRING_PROPERTIES.contains(key) || JMS_IBM_STRING_PROPERTIES.contains(key)) {
            // Handle String properties (both MQMD and JMS_IBM)
            message.setStringProperty(key, value.toString());
        } else if (MQMD_BYTES_PROPERTIES_TO_SKIP.contains(key)) {
            // Skip byte[] MQMD properties - these violate JMS specification
            // See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
            // "The use of byte array properties on a message violates the JMS specification"
            // Use MQMD API (MessageDescriptor) to set these fields instead
            log.debug("Skipping byte array MQMD property '{}' - use MQMD API instead", key);
            return;
        } else if (JMS_IBM_BOOLEAN_PROPERTIES.contains(key)) {
            // Handle Boolean properties (JMS_IBM only)
            if (value instanceof Boolean) {
                message.setBooleanProperty(key, (Boolean) value);
            } else if (value instanceof Integer) {
                // Allow conversion from Integer (0/1) to Boolean
                message.setBooleanProperty(key, ((Integer) value) != 0);
            } else if (value instanceof String) {
                message.setBooleanProperty(key, Boolean.parseBoolean((String) value));
            } else {
                log.warn("Cannot convert type {} to boolean for property '{}'",
                        value.getClass().getName(), key);
                throw new ConnectException("Failed to set property '" + key +
                        "': unsupported type " + value.getClass().getName());
            }
        } else {
            // For non-IBM MQ properties, convert everything to string for backward compatibility
            message.setStringProperty(key, value.toString());
            log.debug("Set property '{}' as string (non-IBM MQ): {}", key, value);
        }
    }

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
                    throw new ConnectException("Failed to set header '" + header.key() + "'", jmse);
                }
            }
        }

        return m;
    }
}
