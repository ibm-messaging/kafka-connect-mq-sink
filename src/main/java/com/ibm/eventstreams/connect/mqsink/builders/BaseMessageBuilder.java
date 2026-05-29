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
    // MQMD properties that require integer type
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
            "JMS_IBM_MQMD_OriginalLength"
    ));

    // MQMD properties that require byte array type
    private static final Set<String> BYTE_ARRAY_MQMD_PROPERTIES = new HashSet<>(Arrays.asList(
            "JMS_IBM_MQMD_MsgId",
            "JMS_IBM_MQMD_CorrelId",
            "JMS_IBM_MQMD_AccountingToken",
            "JMS_IBM_MQMD_GroupId"
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
                    setProperty(m, header.key(), header.value().toString());
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
     * Sets a JMS property with the correct type based on the property name.
     * MQMD properties require specific types according to IBM MQ JMS API.
     *
     * @param message      the JMS message
     * @param propertyName the property name
     * @param value        the property value as a string
     * @throws JMSException if setting the property fails
     * @throws NumberFormatException if parsing a numeric property fails
     */
    private void setProperty(final Message message, final String propertyName, final String value) throws JMSException {
        // Handle MQMD properties with correct types
        if (propertyName.startsWith("JMS_IBM_MQMD_")) {
            // Integer MQMD properties
            if (isIntegerMQMDProperty(propertyName)) { 
                message.setIntProperty(propertyName, Integer.parseInt(value));
                return;
            }
            
            // Byte array MQMD properties (stored as hex strings by Source Connector)
            if (isByteArrayMQMDProperty(propertyName)) {
                message.setObjectProperty(propertyName, hexStringToByteArray(value));
                return;
            }
            // String MQMD properties (default)
        }
        // All other properties are set as strings
        message.setStringProperty(propertyName, value);
    }

    /**
     * Checks if an MQMD property requires integer type.
     * Based on IBM MQ JMS API documentation.
     *
     * @param propertyName the property name
     * @return true if the property requires integer type
     */
    private boolean isIntegerMQMDProperty(final String propertyName) {
        return INTEGER_MQMD_PROPERTIES.contains(propertyName);
    }

    /**
     * Checks if an MQMD property requires byte array type.
     * Based on IBM MQ JMS API documentation.
     * Note: Using byte array properties violates the JMS specification.
     *
     * @param propertyName the property name
     * @return true if the property requires byte array type
     */
    private boolean isByteArrayMQMDProperty(final String propertyName) {
        return BYTE_ARRAY_MQMD_PROPERTIES.contains(propertyName);
    }

    /**
     * Converts a hex string to a byte array.
     * The MQ Source Connector stores byte arrays as hex strings.
     *
     * @param hexString the hex string
     * @return the byte array
     */
    private byte[] hexStringToByteArray(final String hexString) {
        final int len = hexString.length();
        final byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                    + Character.digit(hexString.charAt(i + 1), 16));
        }
        return data;
    }
}
