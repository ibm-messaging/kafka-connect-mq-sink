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
package com.ibm.eventstreams.connect.mqsink.builders;

import com.ibm.eventstreams.connect.mqsink.MQSinkConnector;

import java.util.Map;

import javax.jms.JMSContext;
import javax.jms.Message;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.*;

/**
 * Builds messages from Kafka Connect SinkRecords. It creates a JMS TextMessage containing
 * a string representation of the payload created by a Converter.
 */
public class ConverterMessageBuilder extends BaseMessageBuilder {
    private static final Logger log = LoggerFactory.getLogger(ConverterMessageBuilder.class);

    private Converter converter;

    public ConverterMessageBuilder() {
        log.info("Building messages using com.ibm.eventstreams.connect.mqsink.builders.ConverterMessageBuilder");
    }

    /**
     * Configure this class.
     * 
     * @param props initial configuration
     *
     * @throws ConnectException   Operation failed and connector should stop.
     */
    public void configure(Map<String, String> props) {
        log.trace("[{}] Entry {}.configure, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        super.configure(props);

        String converterClass = props.get(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BUILDER_VALUE_CONVERTER);

        try {
            Class<? extends Converter> c = Class.forName(converterClass).asSubclass(Converter.class);
            converter = c.newInstance();

            // Make a copy of the configuration to filter out only those that begin "mq.message.builder.value.converter."
            // since those are used to configure the converter itself
            AbstractConfig ac = new AbstractConfig(new ConfigDef(), props, false);

            // Configure the Converter to convert the value, not the key (isKey == false)
            converter.configure(ac.originalsWithPrefix(MQSinkConnector.CONFIG_NAME_MQ_MESSAGE_BUILDER_VALUE_CONVERTER + "."), false);
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NullPointerException exc) {
            log.error("Could not instantiate converter for message builder {}", converterClass);
            throw new ConnectException("Could not instantiate converter for message builder", exc);
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
    @Override public Message getJMSMessage(JMSContext jmsCtxt, SinkRecord record) {
        byte[] payload = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        return jmsCtxt.createTextMessage(new String(payload, UTF_8));
    }
}