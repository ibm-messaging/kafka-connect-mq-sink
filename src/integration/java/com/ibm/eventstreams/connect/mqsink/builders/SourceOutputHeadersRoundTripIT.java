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

import static com.ibm.eventstreams.connect.mqsink.util.SourceHeaderAssertions.assertSinkMatchesSourceHeaders;

import java.util.HashMap;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;
import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;
import com.ibm.eventstreams.connect.mqsink.util.HexUtils;
import com.ibm.msg.client.jms.JmsConstants;

/**
 * Fast round-trip integration tests using the MQ source header converter directly.
 *
 * <p>For a full MQ source task → Kafka → MQ sink task path, see
 * {@link ConnectKafkaHeadersRoundTripIT}.
 *
 * <p>Mirrors {@code mq-jms-test-put} scenarios using the real {@link JmsToKafkaHeaderConverter}
 * from the MQ source connector and {@link DefaultMessageBuilder} from the sink.
 */
public class SourceOutputHeadersRoundTripIT extends AbstractJMSContextIT {

    private static final String BODY = "round-trip test";

    @Test
    public void roundTripCustomPropertiesFromSourceHeaders() throws Exception {
        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setStringProperty("facilityCountryCode", "US");
        input.setIntProperty("volume", 11);
        input.setDoubleProperty("decimalmeaning", 42.0);
        input.setBooleanProperty("enabled", true);
        input.setLongProperty("createdAt", 1_609_459_200_000L);

        final ConnectHeaders sourceHeaders = new JmsToKafkaHeaderConverter()
                .convertJmsPropertiesToKafkaHeaders(input);

        final SinkRecord record = new SinkRecord(
                TOPIC,
                PARTITION,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, BODY,
                0L,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                sourceHeaders);

        final Message sinkMessage = messageBuilder(false).fromSinkRecord(getJmsContext(), record);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, false);
    }

    @Test
    public void roundTripLegacyStringPropertiesFromSourceHeaders() throws Exception {
        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setStringProperty("facilityCountryCode", "US");
        input.setStringProperty("volume", "11");
        input.setStringProperty("decimalmeaning", "42.0");
        input.setStringProperty("enabled", "true");
        input.setStringProperty("createdAt", "1609459200000");
        input.setStringProperty("customBytesHex", "01020304");

        final ConnectHeaders sourceHeaders = new JmsToKafkaHeaderConverter()
                .convertJmsPropertiesToKafkaHeaders(input);

        final SinkRecord record = new SinkRecord(
                TOPIC,
                PARTITION,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, BODY,
                0L,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                sourceHeaders);

        final Message sinkMessage = messageBuilder(false).fromSinkRecord(getJmsContext(), record);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, false);
    }

    @Test
    public void roundTripTypedPropertiesAsSourceStringHeaders() throws Exception {
        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setStringProperty("stringProp", "hello");
        input.setIntProperty("intProp", 42);
        input.setLongProperty("longProp", 9_000_000_000L);
        input.setShortProperty("shortProp", (short) 7);
        input.setByteProperty("byteProp", (byte) 3);
        input.setFloatProperty("floatProp", 3.14f);
        input.setDoubleProperty("doubleProp", 2.718);
        input.setBooleanProperty("booleanProp", true);

        final ConnectHeaders sourceHeaders = new JmsToKafkaHeaderConverter()
                .convertJmsPropertiesToKafkaHeaders(input);

        final SinkRecord record = new SinkRecord(
                TOPIC,
                PARTITION,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, BODY,
                0L,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                sourceHeaders);

        final Message sinkMessage = messageBuilder(false).fromSinkRecord(getJmsContext(), record);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, false);
    }

    @Test
    public void roundTripMqmdPropertiesFromSourceHeaders() throws Exception {
        final TextMessage input = getJmsContext().createTextMessage(BODY);
        input.setJMSPriority(5);
        input.setJMSDeliveryMode(DeliveryMode.PERSISTENT);

        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID, 1208);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_ENCODING, 273);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGFLAGS, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_OFFSET, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_REPORT, 2);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_FEEDBACK, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGTYPE, 8);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH, 1);

        input.setIntProperty(JmsConstants.JMS_IBM_ENCODING, 273);
        input.setIntProperty(JmsConstants.JMS_IBM_MSGTYPE, 8);
        input.setIntProperty(JmsConstants.JMS_IBM_FEEDBACK, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_RETAIN, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT, 1);
        input.setBooleanProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);

        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_EXCEPTION, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_EXPIRATION, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_COA, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_COD, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_PAN, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_NAN, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_REPORT_DISCARD_MSG, 1);
        input.setIntProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE, 1);

        input.setStringProperty(JmsConstants.JMS_IBM_MQMD_REPLYTOQ, "REPLY.Q");
        input.setStringProperty(JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR, "QM1");
        input.setStringProperty(JmsConstants.JMS_IBM_CHARACTER_SET, "UTF-8");

        input.setJMSCorrelationIDAsBytes(
                HexUtils.parseHex("414D51207061756C745639344C545320EBC32F6A01A00740"));
        input.setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID,
                HexUtils.parseHex("414141407061756C745639344C545320EBC32F6A01A00740"));
        input.setStringProperty(JmsConstants.JMSX_GROUPID, "mygroup");
        input.setIntProperty(JmsConstants.JMSX_GROUPSEQ, 1);

        final ConnectHeaders sourceHeaders = new JmsToKafkaHeaderConverter()
                .convertJmsPropertiesToKafkaHeaders(input);

        final SinkRecord record = new SinkRecord(
                TOPIC,
                PARTITION,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, BODY,
                0L,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                sourceHeaders);

        final Message sinkMessage = messageBuilder(true).fromSinkRecord(getJmsContext(), record);
        assertSinkMatchesSourceHeaders(sinkMessage, sourceHeaders, true);
    }

    private DefaultMessageBuilder messageBuilder(final boolean mqmdWrite) {
        final DefaultMessageBuilder builder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        if (mqmdWrite) {
            props.put("mq.message.mqmd.write", "true");
        }
        builder.configure(props);
        return builder;
    }
}
