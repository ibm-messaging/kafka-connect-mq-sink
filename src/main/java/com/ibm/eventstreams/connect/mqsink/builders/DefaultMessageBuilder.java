/**
 * Copyright 2017, 2018 IBM Corporation
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
 * Builds JMS messages from Kafka Connect SinkRecords. This is the default implementation.
 * <ul>
 * <li>If the SinkRecord has no value, it creates an untyped JMS Message.
 * <li>If the SinkRecord has no schema and a byte[] or ByteBuffer value or a BYTES schema, it creates a JMS BytesMessage.
 * <li>Otherwise, it creates a JMS TextMessage using java.lang.Object.toString() for the value.
 * </ul>
 */
public class DefaultMessageBuilder extends BaseMessageBuilder {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessageBuilder.class);

    public DefaultMessageBuilder() {
        log.info("Building messages using com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder");
    }

    /**
     * Gets the JMS message for the Kafka Connect SinkRecord.
     * 
     * @param context            the JMS context to use for building messages
     * @param record             the Kafka Connect SinkRecord
     * 
     * @return the JMS message
     */
    @Override public Message getJMSMessage(JMSContext jmsCtxt, SinkRecord record) {
        Schema s = record.valueSchema();
        Object v = record.value();

        log.debug("Value schema {}", s);
        if (v == null) {
            return jmsCtxt.createMessage();
        }
        else if (s == null) {
            log.debug("No schema info {}", v);
            if (v instanceof byte[]) {
                try {
                    BytesMessage bm = jmsCtxt.createBytesMessage();
                    bm.writeBytes((byte[])v);
                    return bm;
                }
                catch (JMSException jmse) {
                    throw new ConnectException("Failed to write bytes", jmse);
                }
            }
            else if (v instanceof ByteBuffer) {
                try {
                    BytesMessage bm = jmsCtxt.createBytesMessage();
                    bm.writeBytes(((ByteBuffer)v).array());
                    return bm;
                }
                catch (JMSException jmse) {
                    throw new ConnectException("Failed to write bytes", jmse);
                }
            }
        }
        else if (s.type() == Type.BYTES) {
            if (v instanceof byte[]) {
                try {
                    BytesMessage bm = jmsCtxt.createBytesMessage();
                    bm.writeBytes((byte[])v);
                    return bm;
                }
                catch (JMSException jmse) {
                    throw new ConnectException("Failed to write bytes", jmse);
                }
            }
            else if (v instanceof ByteBuffer) {
                try {
                    BytesMessage bm = jmsCtxt.createBytesMessage();
                    bm.writeBytes(((ByteBuffer)v).array());
                    return bm;
                }
                catch (JMSException jmse) {
                    throw new ConnectException("Failed to write bytes", jmse);
                }
            }
        }

        return jmsCtxt.createTextMessage(v.toString());     
    }
}