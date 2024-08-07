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
import java.util.Iterator;
import java.util.Map;

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
                    m.setStringProperty(header.key(), header.value().toString());
                } catch (final JMSException jmse) {
                    throw new ConnectException("Failed to set header", jmse);
                }
            }
        }

        return m;
    }
}
