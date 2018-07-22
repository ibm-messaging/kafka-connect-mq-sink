/**
 * Copyright 2018 IBM Corporation
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
package com.ibm.mq.kafkaconnect.builders;

import com.ibm.mq.kafkaconnect.MQSinkConnector;

import java.util.Map;

import java.nio.ByteBuffer;

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds JMS messages from Kafka Connect SinkRecords.
 */
public abstract class BaseMessageBuilder implements MessageBuilder {
    private static final Logger log = LoggerFactory.getLogger(BaseMessageBuilder.class);

    public enum KeyHeader {NONE, CORRELATION_ID};
    protected KeyHeader keyheader = KeyHeader.NONE;

    /**
     * Configure this class.
     * 
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    @Override public void configure(Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        String kh = props.get(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BUILDER_KEY_HEADER);
        if (kh != null) {
            if (kh.equals(MQSinkConnector.CONFIG_VALUE_MQ_MESSAGE_BUILDER_KEY_HEADER_JMSCORRELATIONID)) {
                keyheader = KeyHeader.CORRELATION_ID;
                log.debug("Setting JMSCorrelationID header field from Kafka record key");
            }
            else
            {
                log.debug("Unsupported MQ message builder key header value {}", kh);
                throw new ConnectException("Unsupported MQ message builder key header value");
            }
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
    abstract Message getJMSMessage(JMSContext jmsCtxt, SinkRecord record);

   /**
     * Convert a Kafka Connect SinkRecord into a JMS message.
     * 
     * @param context            the JMS context to use for building messages
     * @param record             the Kafka Connect SinkRecord
     * 
     * @return the JMS message
     */
    @Override public Message fromSinkRecord(JMSContext jmsCtxt, SinkRecord record) {
        Message m = this.getJMSMessage(jmsCtxt, record);

        if (keyheader != KeyHeader.NONE) {
            Schema s = record.keySchema();
            Object k = record.key();

            if (k != null) {
                if (s == null) {
                    log.debug("No schema info {}", k);
                    if (k instanceof byte[]) {
                        try {
                            m.setJMSCorrelationIDAsBytes((byte[])k);
                        }
                        catch (JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                    else if (k instanceof ByteBuffer) {
                        try {
                            m.setJMSCorrelationIDAsBytes(((ByteBuffer)k).array());
                        }
                        catch (JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                    else {
                        try {
                            m.setJMSCorrelationID(k.toString());
                        }
                        catch (JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                }
                else if (s.type() == Type.BYTES) {
                    if (k instanceof byte[]) {
                        try {
                            m.setJMSCorrelationIDAsBytes((byte[])k);
                        }
                        catch (JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                    else if (k instanceof ByteBuffer) {
                        try {
                            m.setJMSCorrelationIDAsBytes(((ByteBuffer)k).array());
                        }
                        catch (JMSException jmse) {
                            throw new ConnectException("Failed to write bytes", jmse);
                        }
                    }
                }
                else if (s.type() == Type.STRING) {
                    try {
                        m.setJMSCorrelationID((String)k);
                    }
                    catch (JMSException jmse) {
                        throw new ConnectException("Failed to write string", jmse);
                    }
                }
            }
        }

        return m;
    }
}