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

import java.util.HashMap;

import javax.jms.JMSContext;
import javax.jms.Message;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.*;

/**
 * Builds messages from Kafka Connect SinkRecords. It creates a JMS TextMessage containing
 * a JSON string representation of the value.
 */
public class JsonMessageBuilder extends BaseMessageBuilder {
    private static final Logger log = LoggerFactory.getLogger(JsonMessageBuilder.class);

    private JsonConverter converter;

    public JsonMessageBuilder() {
        log.info("Building messages using com.ibm.eventstreams.connect.mqsink.builders.JsonMessageBuilder");
        converter = new JsonConverter();
        
        // We just want the payload, not the schema in the output message
        HashMap<String, String> m = new HashMap<>();
        m.put("schemas.enable", "false");

        // Convert the value, not the key (isKey == false)
        converter.configure(m, false);
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