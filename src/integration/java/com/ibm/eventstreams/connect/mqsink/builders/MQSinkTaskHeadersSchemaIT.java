/**
 * Copyright 2026 IBM Corporation
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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;

import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.msg.client.jms.JmsConstants;

/**
 * Integration tests for copying Kafka headers to JMS message properties
 *  using the JSON Connect header converter.
 *
 * JsonConverter is schema-sensitive so this is used to catch schema type 
 *  bugs that using the default SimpleHeaderConverter cannot reveal.
 */
public class MQSinkTaskHeadersSchemaIT extends AbstractJMSContextIT {

    private static final String TOPIC = "mytopic";

    private MessageBuilder builder;
    private HeaderConverter converter;

    @Before
    public void before() {
        builder = new DefaultMessageBuilder();
        final HashMap<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        builder.configure(props);

        converter = new JsonConverter();
        final HashMap<String, String> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", "true");
        converterConfig.put("converter.type", "header");
        converter.configure(converterConfig);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a SinkRecord whose headers contain a single header reconstructed
     * from wire bytes via JsonConverter, recreating how Kafka Connect
     * would deliver headers to a sink task.
     */
    private SinkRecord sinkRecordWithHeader(final String headerName, final byte[] wireBytes) {
        final SchemaAndValue schemaAndValue = converter.toConnectHeader(TOPIC, headerName, wireBytes);
        final ConnectHeaders headers = new ConnectHeaders();
        headers.add(headerName, schemaAndValue);
        return new SinkRecord(TOPIC, 0,
                Schema.STRING_SCHEMA, "key",
                Schema.STRING_SCHEMA, "value",
                0L,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);
    }

    private SinkRecord sinkRecordWithJsonHeader(final String headerName, final String json) {
        return sinkRecordWithHeader(headerName, json.getBytes(StandardCharsets.UTF_8));
    }


    // =========================================================================
    // User-defined message properties
    // =========================================================================

    @Test
    public void userDefinedStringProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("strProp",
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"hello\"}"));

        assertThat(message.getStringProperty("strProp")).isEqualTo("hello");
    }

    @Test
    public void userDefinedByteProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("byteProp", 
                "{\"schema\":{\"type\":\"int8\"},\"payload\": 64 }"));

        assertThat(message.getByteProperty("byteProp")).isEqualTo((byte) 64);
    }

    @Test
    public void userDefinedShortProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("intProp", 
                "{\"schema\":{\"type\":\"int16\"},\"payload\": 1000 }"));

        assertThat(message.getShortProperty("intProp")).isEqualTo((short) 1000);
    }

    @Test
    public void userDefinedIntProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("intProp", 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 12345 }"));

        assertThat(message.getLongProperty("intProp")).isEqualTo(12345);
    }

    @Test
    public void userDefinedLongProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("longProp", 
                "{\"schema\":{\"type\":\"int64\"},\"payload\": 123456789012345 }"));

        assertThat(message.getLongProperty("longProp")).isEqualTo(123456789012345L);
    }

    @Test
    public void userDefinedFloatProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("floatProp", 
                "{\"schema\":{\"type\":\"float\"},\"payload\": 3.14 }"));

        assertThat(message.getFloatProperty("floatProp")).isEqualTo(3.14f);
    }

    @Test
    public void userDefinedDoubleProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("doubleProp", 
                "{\"schema\":{\"type\":\"double\"},\"payload\": 3.141592653589793 }"));

        assertThat(message.getDoubleProperty("doubleProp")).isEqualTo((double) 3.141592653589793);
    }

    @Test
    public void userDefinedBooleanProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("boolProp", 
                "{\"schema\":{\"type\":\"boolean\"},\"payload\": true }"));

        assertThat(message.getBooleanProperty("boolProp")).isTrue();
    }

    // =========================================================================
    // JMSx properties (standard JMS)
    // =========================================================================

    @Test
    public void jmsxUserId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMSX_USERID,
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"app\"}"));

        assertThat(message.getStringProperty(JmsConstants.JMSX_USERID)).isEqualTo("app");        
    }

    @Test
    public void jmsxGroupId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMSX_GROUPID,
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"MY_GROUP\"}"));

        assertThat(message.getStringProperty(JmsConstants.JMSX_GROUPID)).isEqualTo("MY_GROUP");
    }

    @Test
    public void jmsxGroupSeq() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMSX_GROUPSEQ,
                "{\"schema\":{\"type\":\"int8\"},\"payload\": 3 }"));

        assertThat(message.getLongProperty(JmsConstants.JMSX_GROUPSEQ)).isEqualTo(3);
    }


    // =========================================================================
    // JMS_IBM_* extension properties
    // =========================================================================

    @Test
    public void jmsIbmFormat() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_FORMAT,
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"MQSTR\"}"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_FORMAT).trim()).isEqualTo("MQSTR");
    }

    @Test
    public void jmsIbmMsgType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MSGTYPE, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 8 }"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo(8);
    }

    @Test
    public void jmsIbmCharacterSet() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_CHARACTER_SET, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\":1208}"));
                    
        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo(1208);
    }

    @Test
    public void jmsIbmEncoding() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_ENCODING, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 273 }"));
                    
        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_ENCODING)).isEqualTo(273);
    }

    @Test
    public void jmsIbmPutDate() throws Exception {
        final String today = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_PUTDATE, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"" + today + "\"}"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmPutTime() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_PUTTIME, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"12345678\"}"));
                    
        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTTIME)).matches("\\d+");
    }    

    @Test
    public void jmsIbmPutApplType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_PUTAPPLTYPE, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 28 }"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE)).isEqualTo(28);
    }


    // =========================================================================
    // JMS_IBM_MQMD_* properties
    // =========================================================================

    @Test
    public void jmsIbmMqmdPutApplName() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"myapp\"}"));
                    
        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).isNotEmpty();
    }

    @Test
    public void jmsIbmMqmdPutDate() throws Exception {
        final String today = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PUTDATE, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"" + today + "\"}"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmMqmdPutTime() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PUTTIME, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"12345678\"}"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTTIME)).matches("\\d+");
    }

    @Test
    public void jmsIbmMqmdPriority() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PRIORITY, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 4 }"));
                    
        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY)).isEqualTo(4);
    }

    @Test
    public void jmsIbmMqmdPersistence() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PERSISTENCE, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 1 }"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo(1);
    }

    @Test
    public void jmsIbmMqmdMsgType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGTYPE, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 8 }"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo(8);
    }

    @Test
    public void jmsIbmMqmdEncoding() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_ENCODING, 
                "{\"schema\":{\"type\":\"int32\"},\"payload\": 273 }"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo(273);
    }

    @Test
    public void jmsIbmMqmdMsgIdBytes() throws Exception {
        final byte[] msgId = { 0x41, 0x4D, 0x51, 0x20, 0x70, 0x61, 0x75, 0x6C, 
                               0x74, 0x56, 0x39, 0x34, 0x4C, 0x54, 0x53, 0x20, 
                               0x22, 0x23, 0x2F, 0x6A, 0x01, 0x2D, 0x01, 0x40 }; 
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGID, 
                "{\"schema\":{\"type\":\"bytes\"},\"payload\":\"" + 
                    Base64.getEncoder().encodeToString(msgId) + 
                    "\" }"));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes).isEqualTo(msgId);
    }

    @Test
    public void jmsIbmMqmdMsgIdString() throws Exception {
        final byte[] msgId = { 0x41, 0x4D, 0x51, 0x20, 0x70, 0x61, 0x75, 0x6C, 
                               0x74, 0x56, 0x39, 0x34, 0x4C, 0x54, 0x53, 0x20, 
                               0x22, 0x23, 0x2F, 0x6A, 0x01, 0x2D, 0x01, 0x40 }; 
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGID, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"414D51207061756C745639344C54532022232F6A012D0140\" }"));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes).isEqualTo(msgId);
    }

    @Test
    public void jmsIbmMqmdMsgIdPrefixed() throws Exception {
        final byte[] msgId = { 0x41, 0x4D, 0x51, 0x20, 0x70, 0x61, 0x75, 0x6C, 
                               0x74, 0x56, 0x39, 0x34, 0x4C, 0x54, 0x53, 0x20, 
                               0x22, 0x23, 0x2F, 0x6A, 0x01, 0x2D, 0x01, 0x40 }; 
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGID, 
                "{\"schema\":{\"type\":\"string\"},\"payload\":\"ID:414D51207061756C745639344C54532022232F6A012D0140\" }"));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes).isEqualTo(msgId);
    }

    @Test
    public void jmsIbmMqmdCorrelId() throws Exception {
        final byte[] correlId = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                                  0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 
                                  0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18 };
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_CORRELID, 
                "{\"schema\":{\"type\":\"bytes\"},\"payload\":\"" + 
                    Base64.getEncoder().encodeToString(correlId) + 
                    "\" }"));
        
        assertThat(message.getJMSCorrelationIDAsBytes()).isEqualTo(correlId);
    }

    @Test
    public void jmsIbmMqmdAccountingToken() throws Exception {
        final byte[] accountingToken = new byte[32];
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN, 
                "{\"schema\":{\"type\":\"bytes\"},\"payload\":\"" + 
                    Base64.getEncoder().encodeToString(accountingToken) + 
                    "\" }"));

        assertThat((byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN)).hasSize(32);
    }

    @Test
    public void jmsIbmMqmdGroupId() throws Exception {
        final byte[] groupId = new byte[24];
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_GROUPID, 
                "{\"schema\":{\"type\":\"bytes\"},\"payload\":\"" + 
                    Base64.getEncoder().encodeToString(groupId) + 
                    "\" }"));

        assertThat((byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_GROUPID)).hasSize(24);
    }
}
