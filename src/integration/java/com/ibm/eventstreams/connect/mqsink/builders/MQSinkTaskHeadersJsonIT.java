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
public class MQSinkTaskHeadersJsonIT extends AbstractJMSContextIT {

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
        converterConfig.put("schemas.enable", "false");
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

    /**
     * JsonConverter (schemas.enable=false) serialising JSON data as UTF-8 bytes of 
     * the string representation.
     */
    private SinkRecord sinkRecordWithJsonHeader(final String headerName, final String json) {
        return sinkRecordWithHeader(headerName, json.getBytes(StandardCharsets.UTF_8));
    }


    // =========================================================================
    // User-defined message properties
    // =========================================================================

    @Test
    public void userDefinedStringProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("strProp", "\"hello\""));

        assertThat(message.getStringProperty("strProp")).isEqualTo("hello");
    }

    @Test
    public void userDefinedLongProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("longProp", "123456789012345"));

        assertThat(message.getLongProperty("longProp")).isEqualTo(123456789012345L);
    }

    @Test
    public void userDefinedDoubleProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("doubleProp", "3.141592653589793"));

        assertThat(message.getDoubleProperty("doubleProp")).isEqualTo(3.141592653589793);
    }

    @Test
    public void userDefinedBooleanProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader("boolProp", "true"));

        assertThat(message.getBooleanProperty("boolProp")).isTrue();
    }

    // =========================================================================
    // JMSx properties (standard JMS)
    // =========================================================================

    @Test
    public void jmsxUserId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMSX_USERID, "\"app\""));

        assertThat(message.getStringProperty(JmsConstants.JMSX_USERID)).isEqualTo("app");        
    }

    @Test
    public void jmsxGroupId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMSX_GROUPID, "\"MY_GROUP\""));

        assertThat(message.getStringProperty(JmsConstants.JMSX_GROUPID)).isEqualTo("MY_GROUP");
    }

    @Test
    public void jmsxGroupSeq() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMSX_GROUPSEQ, "3"));

        assertThat(message.getLongProperty(JmsConstants.JMSX_GROUPSEQ)).isEqualTo(3);
    }


    // =========================================================================
    // JMS_IBM_* extension properties
    // =========================================================================

    @Test
    public void jmsIbmFormat() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_FORMAT, "\"MQSTR\""));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_FORMAT).trim()).isEqualTo("MQSTR");
    }

    @Test
    public void jmsIbmMsgType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MSGTYPE, "8"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo(8);
    }

    @Test
    public void jmsIbmCharacterSet() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_CHARACTER_SET, "1208"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo(1208);
    }

    @Test
    public void jmsIbmEncoding() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_ENCODING, "273"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_ENCODING)).isEqualTo(273);
    }

    @Test
    public void jmsIbmPutDate() throws Exception {
        final String today = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_PUTDATE, "\"" + today + "\""));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmPutTime() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_PUTTIME, "\"12345678\""));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTTIME)).matches("\\d+");
    }    

    @Test
    public void jmsIbmPutApplType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_PUTAPPLTYPE, "28"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE)).isEqualTo(28);
    }


    // =========================================================================
    // JMS_IBM_MQMD_* properties
    // =========================================================================

    @Test
    public void jmsIbmMqmdPutApplName() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME, "\"myapp\""));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).isNotEmpty();
    }

    @Test
    public void jmsIbmMqmdPutDate() throws Exception {
        final String today = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PUTDATE, "\"" + today + "\""));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmMqmdPutTime() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PUTTIME, "\"12345678\""));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTTIME)).matches("\\d+");
    }

    @Test
    public void jmsIbmMqmdPriority() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PRIORITY, "4"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY)).isEqualTo(4);
    }

    @Test
    public void jmsIbmMqmdPersistence() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_PERSISTENCE, "1"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo(1);
    }

    @Test
    public void jmsIbmMqmdMsgType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGTYPE, "8"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo(8);
    }

    @Test
    public void jmsIbmMqmdEncoding() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_ENCODING, "273"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo(273);
    }

    @Test
    public void jmsIbmMqmdMsgId() throws Exception {
        final String msgId = "414D51207061756C745639344C545320EBC32F6A01FD0140";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGID, "\"" + msgId + "\""));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes[0]).isEqualTo((byte)65);
        assertThat(msgIdBytes[1]).isEqualTo((byte)77);
        assertThat(msgIdBytes[22]).isEqualTo((byte)1);
        assertThat(msgIdBytes[23]).isEqualTo((byte)64);
    }

    @Test
    public void jmsIbmMqmdMsgIdPrefixed() throws Exception {
        final String msgId = "414D51207061756C745639344C545320EBC32F6A01FD0140";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_MSGID, "\"ID:" + msgId + "\""));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes[0]).isEqualTo((byte)65);
        assertThat(msgIdBytes[1]).isEqualTo((byte)77);
        assertThat(msgIdBytes[22]).isEqualTo((byte)1);
        assertThat(msgIdBytes[23]).isEqualTo((byte)64);
    }

    @Test
    public void jmsIbmMqmdCorrelId() throws Exception {
        final String correlId = "0102030405060708090A0B0C0D0E0F101112131415161718";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_CORRELID, "\"" + correlId + "\""));
        
        assertThat(message.getJMSCorrelationID()).isEqualTo(correlId);
    }

    @Test
    public void jmsIbmMqmdAccountingToken() throws Exception {
        final byte[] accountingToken = new byte[32];
        final String base64Json = "\"" + Base64.getEncoder().encodeToString(accountingToken) + "\"";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN, base64Json));

        assertThat((byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN)).hasSize(32);
    }

    @Test
    public void jmsIbmMqmdGroupId() throws Exception {
        final byte[] groupId = new byte[24];
        final String base64Json = "\"" + Base64.getEncoder().encodeToString(groupId) + "\"";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithJsonHeader(JmsConstants.JMS_IBM_MQMD_GROUPID, base64Json));

        assertThat((byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_GROUPID)).hasSize(24);
    }
}
