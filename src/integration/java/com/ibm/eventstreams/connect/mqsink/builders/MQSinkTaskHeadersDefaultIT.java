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
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;

import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.msg.client.jms.JmsConstants;

/**
 * Integration tests for copying Kafka headers to JMS message properties
 *  using the default Connect header converter.
 */
public class MQSinkTaskHeadersDefaultIT extends AbstractJMSContextIT {

    private static final String TOPIC = "mytopic";

    private static final byte[] TEST_CORREL_ID = new byte[] {
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18
    };

    private MessageBuilder builder;
    private HeaderConverter converter;

    @Before
    public void before() {
        builder = new DefaultMessageBuilder();
        final HashMap<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        builder.configure(props);

        converter = new SimpleHeaderConverter();
        final HashMap<String, String> converterConfig = new HashMap<>();
        converter.configure(converterConfig);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Builds a SinkRecord whose headers contain a single header reconstructed
     * from wire bytes via SimpleHeaderConverter, recreating how Kafka Connect
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
     * SimpleHeaderConverter writes scalar values as UTF-8 bytes of
     * their string representation.
     */
    private SinkRecord sinkRecordWithStringHeader(final String headerName, final String headerString) {
        return sinkRecordWithHeader(headerName, headerString.getBytes(StandardCharsets.UTF_8));
    }


    // =========================================================================
    // User-defined message properties
    // =========================================================================

    @Test
    public void userDefinedStringProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("strProp", "hello"));

        assertThat(message.getStringProperty("strProp")).isEqualTo("hello");
    }

    @Test
    public void userDefinedIntProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("intProp", "42"));

        assertThat(message.getIntProperty("intProp")).isEqualTo(42);
    }

    @Test
    public void userDefinedLongProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("longProp", "123456789012345"));

        assertThat(message.getLongProperty("longProp")).isEqualTo(123456789012345L);
    }

    @Test
    public void userDefinedFloatProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("floatProp", "1.5"));

        assertThat(message.getFloatProperty("floatProp")).isEqualTo(1.5f);
    }

    @Test
    public void userDefinedDoubleProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("doubleProp", "3.141592653589793"));

        assertThat(message.getFloatProperty("doubleProp")).isEqualTo((float) 3.141592653589793);
    }

    @Test
    public void userDefinedBooleanProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("boolProp", "true"));

        assertThat(message.getBooleanProperty("boolProp")).isTrue();
    }

    @Test
    public void userDefinedByteProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("byteProp", "64"));

        assertThat(message.getByteProperty("byteProp")).isEqualTo((byte) 64);
    }

    @Test
    public void userDefinedShortProperty() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader("shortProp", "1000"));

        assertThat(message.getShortProperty("shortProp")).isEqualTo((short) 1000);
    }


    // =========================================================================
    // JMSx properties (standard JMS)
    // =========================================================================

    @Test
    public void jmsxUserId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMSX_USERID, "app"));

        assertThat(message.getStringProperty(JmsConstants.JMSX_USERID).trim()).isEqualTo("app");
    }

    @Test
    public void jmsxGroupId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMSX_GROUPID, "MY_GROUP"));

        assertThat(message.getStringProperty(JmsConstants.JMSX_GROUPID)).isEqualTo("MY_GROUP");
    }

    @Test
    public void jmsxGroupSeq() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMSX_GROUPSEQ, "3"));

        assertThat(message.getIntProperty(JmsConstants.JMSX_GROUPSEQ)).isEqualTo(3);
    }


    // =========================================================================
    // JMS_IBM_* extension properties
    // =========================================================================

    @Test
    public void jmsIbmFormat() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_FORMAT, "MQSTR"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_FORMAT).trim()).isEqualTo("MQSTR");
    }

    @Test
    public void jmsIbmMsgType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MSGTYPE, "8"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MSGTYPE)).isEqualTo(8);
    }

    @Test
    public void jmsIbmCharacterSet() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_CHARACTER_SET, "1208"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_CHARACTER_SET)).isEqualTo(1208);
    }

    @Test
    public void jmsIbmEncoding() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_ENCODING, "273"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_ENCODING)).isEqualTo(273);
    }

    @Test
    public void jmsIbmPutDate() throws Exception {
        final String today = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_PUTDATE, today));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmPutTime() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_PUTTIME, "12345678"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTTIME)).matches("\\d+");
    }

    @Test
    public void jmsIbmPutApplType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_PUTAPPLTYPE, "28"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE)).matches("\\d+");
    }


    // =========================================================================
    // JMS_IBM_MQMD_* properties
    // =========================================================================

    @Test
    public void jmsIbmMqmdPutApplName() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME, "myapp"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME)).isNotEmpty();
    }

    @Test
    public void jmsIbmMqmdPutDate() throws Exception {
        final String today = java.time.LocalDate.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_PUTDATE, today));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTDATE)).isEqualTo(today);
    }

    @Test
    public void jmsIbmMqmdPutTime() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_PUTTIME, "12345678"));

        assertThat(message.getStringProperty(JmsConstants.JMS_IBM_MQMD_PUTTIME)).matches("\\d+");
    }

    @Test
    public void jmsIbmMqmdPriority() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_PRIORITY, "4"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY)).isEqualTo(4);
    }

    @Test
    public void jmsIbmMqmdPersistence() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_PERSISTENCE, "1"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_PERSISTENCE)).isEqualTo(1);
    }

    @Test
    public void jmsIbmMqmdMsgType() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_MSGTYPE, "8"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_MSGTYPE)).isEqualTo(8);
    }

    @Test
    public void jmsIbmMqmdEncoding() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_ENCODING, "273"));

        assertThat(message.getIntProperty(JmsConstants.JMS_IBM_MQMD_ENCODING)).isEqualTo(273);
    }

    @Test
    public void jmsIbmMqmdMsgId() throws Exception {
        final String msgId = "414D51207061756C745639344C545320EBC32F6A01FD0140";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_MSGID, msgId));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes[0]).isEqualTo((byte)65);
        assertThat(msgIdBytes[1]).isEqualTo((byte)77);
        assertThat(msgIdBytes[22]).isEqualTo((byte)1);
        assertThat(msgIdBytes[23]).isEqualTo((byte)64);
    }

    @Test
    public void jmsIbmMqmdMsgIdPrefixed() throws Exception {
        final String msgId = "ID:414D51207061756C745639344C545320EBC32F6A01FD0140";
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithStringHeader(JmsConstants.JMS_IBM_MQMD_MSGID, msgId));

        final byte[] msgIdBytes = (byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID);
        assertThat(msgIdBytes).hasSize(24);
        assertThat(msgIdBytes[0]).isEqualTo((byte)65);
        assertThat(msgIdBytes[1]).isEqualTo((byte)77);
        assertThat(msgIdBytes[22]).isEqualTo((byte)1);
        assertThat(msgIdBytes[23]).isEqualTo((byte)64);
    }    

    @Test
    public void jmsIbmMqmdCorrelId() throws Exception {
        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithHeader(JmsConstants.JMS_IBM_MQMD_CORRELID, TEST_CORREL_ID));

        assertThat(message.getJMSCorrelationIDAsBytes()).isEqualTo(TEST_CORREL_ID);
    }

    @Test
    public void jmsIbmMqmdAccountingToken() throws Exception {
        final byte[] accountingToken = new byte[32];
        Arrays.fill(accountingToken, (byte) 9);

        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithHeader(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN, Base64.getEncoder().encode(accountingToken)));

        assertThat((byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN)).isEqualTo(accountingToken);
    }

    @Test
    public void jmsIbmMqmdGroupId() throws Exception {
        final byte[] groupId = new byte[24];
        Arrays.fill(groupId, (byte) 0);

        final Message message = builder.fromSinkRecord(getJmsContext(),
                sinkRecordWithHeader(JmsConstants.JMS_IBM_MQMD_GROUPID, Base64.getEncoder().encode(groupId)));

        assertThat((byte[]) message.getObjectProperty(JmsConstants.JMS_IBM_MQMD_GROUPID)).isEqualTo(groupId);
    }
}
