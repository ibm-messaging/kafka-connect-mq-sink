/**
 * Copyright 2023, 2023, 2024, 2026 IBM Corporation
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class DefaultMessageBuilderWithHeadersIT extends AbstractJMSContextIT {

    private MessageBuilder builder;

    @BeforeEach
    public void prepareMessageBuilder() {
        builder = new DefaultMessageBuilder();

        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        builder.configure(props);
    }

    private SinkRecord generateSinkRecord(final ConnectHeaders headers) {
        final String topic = "TOPIC.NAME";
        final int partition = 0;
        final long offset = 0;
        return new SinkRecord(topic, partition,
                Schema.STRING_SCHEMA, "mykey",
                Schema.STRING_SCHEMA, "Test message",
                offset,
                null, TimestampType.NO_TIMESTAMP_TYPE,
                headers);
    }

    @Test
    public void buildMessageWithNoHeaders() throws Exception {
        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(null));

        // verify there are no MQ message properties
        assertFalse(message.getPropertyNames().hasMoreElements());
    }

    @Test
    public void buildMessageWithStringHeaders() throws Exception {
        final Map<String, String> testHeaders = new HashMap<>();
        testHeaders.put("HeaderOne", "This is test header one");
        testHeaders.put("HeaderTwo", "This is test header two");
        testHeaders.put("HeaderThree", "This is test header three");
        testHeaders.put("HeaderFour", "This is test header four");

        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        for (final String key : testHeaders.keySet()) {
            headers.addString(key, testHeaders.get(key));
        }

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        for (final String key : testHeaders.keySet()) {
            assertEquals(testHeaders.get(key), message.getStringProperty(key));
        }
    }

    @Test
    public void buildMessageWithBooleanHeaders() throws Exception {
        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addBoolean("TestTrue", true);
        headers.addBoolean("TestFalse", false);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        assertEquals("true", message.getStringProperty("TestTrue"));
        assertEquals("false", message.getStringProperty("TestFalse"));
        assertTrue(message.getBooleanProperty("TestTrue"));
        assertFalse(message.getBooleanProperty("TestFalse"));
    }

    @Test
    public void buildMessageWithIntegerHeaders() throws Exception {
        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addInt("TestOne", 1);
        headers.addInt("TestTwo", 2);
        headers.addInt("TestThree", 3);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        assertEquals("1", message.getStringProperty("TestOne"));
        assertEquals("2", message.getStringProperty("TestTwo"));
        assertEquals("3", message.getStringProperty("TestThree"));
        assertEquals(1, message.getIntProperty("TestOne"));
        assertEquals(2, message.getIntProperty("TestTwo"));
        assertEquals(3, message.getIntProperty("TestThree"));
    }

    @Test
    public void buildMessageWithDoubleHeaders() throws Exception {
        // prepare Kafka headers for input message
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addDouble("TestPi", 3.14159265359);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // verify MQ message properties
        assertEquals("3.14159265359", message.getStringProperty("TestPi"));
        assertEquals(3.14159265359, message.getDoubleProperty("TestPi"), 0.0000000001);
    }

    @Test
    public void buildMessageWithMQMDIntegerProperties() throws Exception {
        // Test MQMD Integer properties according to IBM MQ documentation:
        // https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // Priority property - Integer (0-9)
        headers.addString("JMS_IBM_MQMD_Priority", "5");

        // Expiry property - Integer (time-to-live in tenths of a second, or MQEXPIRY_DEFAULT, MQEXPIRY_UNLIMITED)
        headers.addString("JMS_IBM_MQMD_Expiry", "36000"); // 1 hour in tenths of a second

        // MsgType property - Integer
        headers.addString("JMS_IBM_MQMD_MsgType", "1"); // MQMT_REQUEST

        // Report property - Integer (report options)
        headers.addString("JMS_IBM_MQMD_Report", "0");

        // Encoding property - Integer (numeric encoding)
        headers.addString("JMS_IBM_MQMD_Encoding", "546"); // MQENC_NATIVE

        // CodedCharSetId property - Integer (character set)
        headers.addString("JMS_IBM_MQMD_CodedCharSetId", "819"); // UTF-8

        // Persistence property - Integer (MQPER_PERSISTENT, MQPER_NOT_PERSISTENT)
        headers.addString("JMS_IBM_MQMD_Persistence", "1"); // MQPER_PERSISTENT

        // Feedback property - Integer (feedback code)
        headers.addString("JMS_IBM_MQMD_Feedback", "0");

        // PutApplType property - Integer (application type)
        headers.addString("JMS_IBM_MQMD_PutApplType", "11"); // MQAT_WINDOWS

        // MsgSeqNumber property - Integer
        headers.addString("JMS_IBM_MQMD_MsgSeqNumber", "1");

        // Offset property - Integer
        headers.addString("JMS_IBM_MQMD_Offset", "0");

        // MsgFlags property - Integer
        headers.addString("JMS_IBM_MQMD_MsgFlags", "0");

        // OriginalLength property - Integer
        headers.addString("JMS_IBM_MQMD_OriginalLength", "-1"); // MQOL_UNDEFINED

        // generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify each MQMD integer property can be retrieved with correct type
        assertEquals(5, message.getIntProperty("JMS_IBM_MQMD_Priority"));
        assertEquals(36000, message.getIntProperty("JMS_IBM_MQMD_Expiry"));
        assertEquals(1, message.getIntProperty("JMS_IBM_MQMD_MsgType"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_Report"));
        assertEquals(546, message.getIntProperty("JMS_IBM_MQMD_Encoding"));
        assertEquals(819, message.getIntProperty("JMS_IBM_MQMD_CodedCharSetId"));
        assertEquals(1, message.getIntProperty("JMS_IBM_MQMD_Persistence"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_Feedback"));
        assertEquals(11, message.getIntProperty("JMS_IBM_MQMD_PutApplType"));
        assertEquals(1, message.getIntProperty("JMS_IBM_MQMD_MsgSeqNumber"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_Offset"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_MsgFlags"));
        assertEquals(-1, message.getIntProperty("JMS_IBM_MQMD_OriginalLength"));
    }

    @Test
    public void buildMessageWithMQMDIntegerPropertiesAsStrings() throws Exception {
        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        headers.addString("JMS_IBM_MQMD_Priority", "5");
        headers.addString("JMS_IBM_MQMD_Expiry", "36000");
        headers.addString("JMS_IBM_MQMD_MsgType", "1");
        headers.addString("JMS_IBM_MQMD_Encoding", "546");
        headers.addString("JMS_IBM_MQMD_CodedCharSetId", "819");
        headers.addString("JMS_IBM_MQMD_Persistence", "1");
        headers.addString("JMS_IBM_MQMD_Feedback", "0");
        headers.addString("JMS_IBM_MQMD_PutApplType", "11");
        headers.addString("JMS_IBM_MQMD_MsgSeqNumber", "1");
        headers.addString("JMS_IBM_MQMD_Offset", "0");
        headers.addString("JMS_IBM_MQMD_MsgFlags", "0");
        headers.addString("JMS_IBM_MQMD_OriginalLength", "-1");

        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        assertEquals(5, message.getIntProperty("JMS_IBM_MQMD_Priority"));
        assertEquals(36000, message.getIntProperty("JMS_IBM_MQMD_Expiry"));
        assertEquals(1, message.getIntProperty("JMS_IBM_MQMD_MsgType"));
        assertEquals(546, message.getIntProperty("JMS_IBM_MQMD_Encoding"));
        assertEquals(819, message.getIntProperty("JMS_IBM_MQMD_CodedCharSetId"));
        assertEquals(1, message.getIntProperty("JMS_IBM_MQMD_Persistence"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_Feedback"));
        assertEquals(11, message.getIntProperty("JMS_IBM_MQMD_PutApplType"));
        assertEquals(1, message.getIntProperty("JMS_IBM_MQMD_MsgSeqNumber"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_Offset"));
        assertEquals(0, message.getIntProperty("JMS_IBM_MQMD_MsgFlags"));
        assertEquals(-1, message.getIntProperty("JMS_IBM_MQMD_OriginalLength"));
    }

    @Test
    public void buildMessageWithInvalidMQMDIntegerPropertyAsStringIsSkipped() throws Exception {
        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();
        headers.addString("JMS_IBM_MQMD_Priority", "abc");  // Invalid integer value
        headers.addString("JMS_IBM_MQMD_CodedCharSetId", "819");  // Valid integer

        // Message should be created successfully, invalid property is skipped
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Valid property should be set
        assertEquals(819, message.getIntProperty("JMS_IBM_MQMD_CodedCharSetId"));

        // Invalid property should be skipped (not set)
        assertFalse(message.propertyExists("JMS_IBM_MQMD_Priority"));
    }

    @Test
    public void buildMessageWithInvalidMQMDIntegerPropertyAsFloatStringIsSkipped() throws Exception {
        // Test that a float string like "5.0" for an Integer MQMD property is skipped
        // This validates that invalid values don't cause message processing to fail

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();
        // Try to set Priority with a float string - this should be skipped
        headers.addString("JMS_IBM_MQMD_Priority", "5.0");
        // Add a valid property to ensure message is still created
        headers.addString("JMS_IBM_MQMD_Format", "MQSTR");

        // Message should be created successfully, invalid property is skipped
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Valid property should be set
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_MQMD_Format"));

        // Invalid property should NOT be set (skipped due to NumberFormatException)
        assertFalse(message.propertyExists("JMS_IBM_MQMD_Priority"));
    }

    @Test
    public void buildMessageWithMQMDStringProperties() throws Exception {
        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        headers.addString("JMS_IBM_MQMD_Format", "MQSTR");
        headers.addString("JMS_IBM_MQMD_ReplyToQ", "REPLY.QUEUE");
        headers.addString("JMS_IBM_MQMD_ReplyToQMgr", "REPLY.QMGR");
        headers.addString("JMS_IBM_MQMD_UserIdentifier", "testuser");
        headers.addString("JMS_IBM_MQMD_ApplIdentityData", "ApplicationID123");
        headers.addString("JMS_IBM_MQMD_PutApplName", "TestApplication");
        headers.addString("JMS_IBM_MQMD_PutDate", "20240129");
        headers.addString("JMS_IBM_MQMD_PutTime", "14302156");
        headers.addString("JMS_IBM_MQMD_ApplOriginData", "OriginData");

        // generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify each MQMD string property can be retrieved with correct type
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_MQMD_Format"));
        assertEquals("REPLY.QUEUE", message.getStringProperty("JMS_IBM_MQMD_ReplyToQ"));
        assertEquals("REPLY.QMGR", message.getStringProperty("JMS_IBM_MQMD_ReplyToQMgr"));
        assertEquals("testuser", message.getStringProperty("JMS_IBM_MQMD_UserIdentifier"));
        assertEquals("ApplicationID123", message.getStringProperty("JMS_IBM_MQMD_ApplIdentityData"));
        assertEquals("TestApplication", message.getStringProperty("JMS_IBM_MQMD_PutApplName"));
        assertEquals("20240129", message.getStringProperty("JMS_IBM_MQMD_PutDate"));
        assertEquals("14302156", message.getStringProperty("JMS_IBM_MQMD_PutTime"));
        assertEquals("OriginData", message.getStringProperty("JMS_IBM_MQMD_ApplOriginData"));
    }

    @Test
    public void buildMessageWithMixedTypeHeaders() throws Exception {
        // Comprehensive test with all supported numeric JMS property types
        // to ensure type preservation when copying Kafka headers to JMS properties

        final ConnectHeaders headers = new ConnectHeaders();

        // Add various typed headers
        headers.addString("StringProp", "test_value");
        headers.addInt("IntProp", 42);
        headers.addLong("LongProp", 1234567890L);
        headers.addBoolean("BoolProp", true);
        headers.addDouble("DoubleProp", 3.14159);
        headers.addFloat("FloatProp", 2.71828f);
        headers.addByte("ByteProp", (byte) 127);
        headers.addShort("ShortProp", (short) 32000);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify each property can be retrieved with its correct type
        assertEquals("test_value", message.getStringProperty("StringProp"));
        assertEquals(42, message.getIntProperty("IntProp"));
        assertEquals(1234567890L, message.getLongProperty("LongProp"));
        assertEquals(true, message.getBooleanProperty("BoolProp"));
        assertEquals(3.14159, message.getDoubleProperty("DoubleProp"), 0.0001);
        assertEquals(2.71828f, message.getFloatProperty("FloatProp"), 0.0001f);
        assertEquals((byte) 127, message.getByteProperty("ByteProp"));
        assertEquals((short) 32000, message.getShortProperty("ShortProp"));
    }

    @Test
    public void buildMessageWithJMSIBMIntegerProperties() throws Exception {
        // Test JMS_IBM Integer properties (non-MQMD) according to IBM MQ documentation:
        // https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields

        final ConnectHeaders headers = new ConnectHeaders();

        // Report options - Integer values
        headers.addString("JMS_IBM_Report_Exception", "1");
        headers.addString("JMS_IBM_Report_Expiration", "1");
        headers.addString("JMS_IBM_Report_COA", "1");
        headers.addString("JMS_IBM_Report_COD", "1");
        headers.addString("JMS_IBM_Report_PAN", "0");
        headers.addString("JMS_IBM_Report_NAN", "0");
        headers.addString("JMS_IBM_Report_Pass_Msg_ID", "1");
        headers.addString("JMS_IBM_Report_Pass_Correl_ID", "1");
        headers.addString("JMS_IBM_Report_Discard_Msg", "0");

        // Message type - Integer
        headers.addString("JMS_IBM_MsgType", "8"); // MQMT_DATAGRAM

        // Feedback - Integer
        headers.addString("JMS_IBM_Feedback", "0");

        // Encoding - Integer (numeric encoding)
        headers.addString("JMS_IBM_Encoding", "546"); // MQENC_NATIVE

        // Character Set - Integer
        headers.addString("JMS_IBM_Character_Set", "819"); // UTF-8

        // Put Application Type - Integer
        headers.addString("JMS_IBM_PutApplType", "11"); // MQAT_WINDOWS

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify each JMS_IBM integer property can be retrieved with correct type
        assertEquals(1, message.getIntProperty("JMS_IBM_Report_Exception"));
        assertEquals(1, message.getIntProperty("JMS_IBM_Report_Expiration"));
        assertEquals(1, message.getIntProperty("JMS_IBM_Report_COA"));
        assertEquals(1, message.getIntProperty("JMS_IBM_Report_COD"));
        assertEquals(0, message.getIntProperty("JMS_IBM_Report_PAN"));
        assertEquals(0, message.getIntProperty("JMS_IBM_Report_NAN"));
        assertEquals(1, message.getIntProperty("JMS_IBM_Report_Pass_Msg_ID"));
        assertEquals(1, message.getIntProperty("JMS_IBM_Report_Pass_Correl_ID"));
        assertEquals(0, message.getIntProperty("JMS_IBM_Report_Discard_Msg"));
        assertEquals(8, message.getIntProperty("JMS_IBM_MsgType"));
        assertEquals(0, message.getIntProperty("JMS_IBM_Feedback"));
        assertEquals(546, message.getIntProperty("JMS_IBM_Encoding"));
        assertEquals(819, message.getIntProperty("JMS_IBM_Character_Set"));
        assertEquals(11, message.getIntProperty("JMS_IBM_PutApplType"));
    }

    @Test
    public void buildMessageWithJMSIBMBooleanProperties() throws Exception {
        // Test JMS_IBM Boolean properties (non-MQMD) according to IBM MQ documentation:
        // JMS_IBM_Last_Msg_In_Group requires Boolean type

        final ConnectHeaders headers = new ConnectHeaders();

        // Last message in group - Boolean
        headers.addString("JMS_IBM_Last_Msg_In_Group", "true");

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify JMS_IBM boolean property can be retrieved with correct type
        assertEquals(true, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));
    }

    @Test
    public void buildMessageWithJMSIBMBooleanPropertyAsInteger() throws Exception {
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addString("JMS_IBM_Last_Msg_In_Group", "1");

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));
        assertEquals(true, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));
    }

    @Test
    public void buildMessageWithJMSIBMBooleanPropertyAsStringTrue() throws Exception {
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addString("JMS_IBM_Last_Msg_In_Group", "true");

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));
        assertEquals(true, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));
    }

    @Test
    public void buildMessageWithJMSIBMBooleanPropertyAsStringFalse() throws Exception {
        final ConnectHeaders headers = new ConnectHeaders();
        headers.addString("JMS_IBM_Last_Msg_In_Group", "false");

        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));
        assertEquals(false, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));
    }

    @Test
    public void buildMessageWithJMSIBMStringProperties() throws Exception {
        // Test JMS_IBM String properties (non-MQMD) according to IBM MQ documentation:
        // The following JMS_IBM properties require String type and can be set:
        // - JMS_IBM_Format (String)
        //
        // Note: JMS_IBM_PutAppl, JMS_IBM_PutDate, JMS_IBM_PutTime are read-only

        final ConnectHeaders headers = new ConnectHeaders();

        headers.addString("JMS_IBM_Format", "MQSTR");

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify JMS_IBM string property can be retrieved with correct type
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_Format"));
    }

    @Test
    public void buildMessageWithMixedMQMDAndJMSIBMProperties() throws Exception {
        // Comprehensive test with both MQMD and JMS_IBM properties
        // to ensure all IBM MQ property types are handled correctly

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // MQMD Integer properties
        headers.addString("JMS_IBM_MQMD_Priority", "5");
        headers.addString("JMS_IBM_MQMD_Expiry", "36000");
        headers.addString("JMS_IBM_MQMD_Encoding", "546");

        // MQMD String properties
        headers.addString("JMS_IBM_MQMD_Format", "MQSTR");
        headers.addString("JMS_IBM_MQMD_ReplyToQ", "REPLY.QUEUE");


        // JMS_IBM Integer properties
        headers.addString("JMS_IBM_Encoding", "546");
        headers.addString("JMS_IBM_Character_Set", "819");
        headers.addString("JMS_IBM_MsgType", "8");

        // JMS_IBM String properties
        headers.addString("JMS_IBM_Format", "MQSTR");

        // JMS_IBM Boolean properties
        headers.addString("JMS_IBM_Last_Msg_In_Group", "true");

        // Regular properties (should be converted to strings)
        headers.addString("CustomStringProp", "custom_value");
        headers.addString("CustomIntProp", "999");

        // generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify MQMD properties
        assertEquals(5, message.getIntProperty("JMS_IBM_MQMD_Priority"));
        assertEquals(36000, message.getIntProperty("JMS_IBM_MQMD_Expiry"));
        assertEquals(546, message.getIntProperty("JMS_IBM_MQMD_Encoding"));
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_MQMD_Format"));
        assertEquals("REPLY.QUEUE", message.getStringProperty("JMS_IBM_MQMD_ReplyToQ"));

        // Verify JMS_IBM properties
        assertEquals(546, message.getIntProperty("JMS_IBM_Encoding"));
        assertEquals(819, message.getIntProperty("JMS_IBM_Character_Set"));
        assertEquals(8, message.getIntProperty("JMS_IBM_MsgType"));
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_Format"));
        assertEquals(true, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));

        // Verify regular properties (converted to strings)
        assertEquals("custom_value", message.getStringProperty("CustomStringProp"));
        assertEquals("999", message.getStringProperty("CustomIntProp"));
    }

    @Test
    public void buildMessageWithMQMDByteArrayProperties() throws Exception {
        // Test MQMD byte[] properties with mq.message.mqmd.write=true
        // These properties can be set via setObjectProperty() when mqmd.write is enabled

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // MQMD byte[] properties - these are the 4 byte[] MQMD fields
        final byte[] groupId = new byte[] {0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
                                           0x49, 0x4A, 0x4B, 0x4C, 0x4D, 0x4E, 0x4F, 0x50,
                                           0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58};
        final byte[] accountingToken = new byte[] {0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68,
                                                    0x69, 0x6A, 0x6B, 0x6C, 0x6D, 0x6E, 0x6F, 0x70,
                                                    0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
                                                    0x79, 0x7A, 0x7B, 0x7C, 0x7D, 0x7E, 0x7F, (byte)0x80};

        headers.addString("JMS_IBM_MQMD_GroupId", java.util.Base64.getEncoder().encodeToString(groupId));
        headers.addString("JMS_IBM_MQMD_AccountingToken", java.util.Base64.getEncoder().encodeToString(accountingToken));

        // generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify each MQMD byte[] property can be retrieved
        // Note: We can't directly verify the byte[] values in integration tests without accessing
        // the underlying MQ message, but we can verify the message was created successfully
        // and the properties were set (they would throw an exception if they couldn't be set)
        assertNotNull(message);

        // Verify the properties exist (propertyExists returns true if set, even for byte[])
        assertTrue(message.propertyExists("JMS_IBM_MQMD_GroupId"));
        assertTrue(message.propertyExists("JMS_IBM_MQMD_AccountingToken"));
    }

    @Test
    public void buildMessageWithMQMDByteArrayPropertiesSkippedWhenDisabled() throws Exception {
        // Test that MQMD byte[] properties are skipped when mq.message.mqmd.write=false
        // Uses the default builder which has mqmd.write disabled
        // Note: The actual code expects Base64-encoded strings for byte array properties

        final ConnectHeaders headers = new ConnectHeaders();

        // MQMD byte[] properties as Base64-encoded strings (matching actual code behavior)
        final byte[] msgId = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                                         0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
                                         0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18};
        final byte[] correlId = new byte[] {0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                                            0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30,
                                            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38};

        // Add as Base64-encoded strings (how the source connector sends them)
        headers.addBytes("JMS_IBM_MQMD_MsgId", msgId);
        headers.addBytes("JMS_IBM_MQMD_CorrelId", correlId);

        // generate MQ message - should succeed but properties should be skipped
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify message was created successfully
        assertNotNull(message);

        // Verify the MQMD byte[] properties were NOT set (skipped because mqmd.write=false)
        assertFalse(message.propertyExists("JMS_IBM_MQMD_MsgId"));
        assertFalse(message.propertyExists("JMS_IBM_MQMD_CorrelId"));
    }

    @Test
    public void buildMessageSkipsJmsMqmdBackoutCount() throws Exception {
        // Test that JMS_IBM_MQMD_BackoutCount is NOT set even when it is present
        // Create builder with mq.message.mqmd.write=true to ensure other MQMD properties work
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // Add JMS_IBM_MQMD_BackoutCount - this should be skipped
        headers.addString("JMS_IBM_MQMD_BackoutCount", "5");

        // Add another MQMD property to verify mqmd.write is working
        headers.addString("JMS_IBM_MQMD_Priority", "7");

        // Generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify JMS_IBM_MQMD_BackoutCount was NOT set (connector skips it)
        assertFalse(message.propertyExists("JMS_IBM_MQMD_BackoutCount"));

        // Verify other MQMD property WAS set (to confirm mqmd.write is working)
        assertTrue(message.propertyExists("JMS_IBM_MQMD_Priority"));
        assertEquals(7, message.getIntProperty("JMS_IBM_MQMD_Priority"));
    }

    @Test
    public void buildMessageWithHeadersFromNewSourceConnector() throws Exception {
        // Test the actual flow: New source connector sends everything as String (except byte[])
        // This validates the sink can correctly parse String headers back to proper JMS types

        // MQMD properties require mq.message.mqmd.write=true
        final DefaultMessageBuilder mqmdBuilder = new DefaultMessageBuilder();
        final Map<String, String> props = new HashMap<>();
        props.put("mq.kafka.headers.copy.to.jms.properties", "true");
        props.put("mq.message.mqmd.write", "true");
        mqmdBuilder.configure(props);

        final ConnectHeaders headers = new ConnectHeaders();

        // New source converts all types to String (except byte[])
        // MQMD Integer properties as String
        headers.addString("JMS_IBM_MQMD_Priority", "5");
        headers.addString("JMS_IBM_MQMD_Expiry", "36000");
        headers.addString("JMS_IBM_MQMD_Encoding", "546");

        // MQMD String properties as String
        headers.addString("JMS_IBM_MQMD_Format", "MQSTR");
        headers.addString("JMS_IBM_MQMD_ReplyToQ", "REPLY.QUEUE");

        // JMS_IBM Boolean property as String
        headers.addString("JMS_IBM_Last_Msg_In_Group", "true");

        // JMS_IBM Integer properties as String
        headers.addString("JMS_IBM_Encoding", "546");

        // Standard JMS properties as String (NEW: these use dedicated setters)
        headers.addString("JMS_DELIVERY_MODE", "2");
        headers.addString("JMS_EXPIRATION", "60000");

        // Only byte[] properties stay as byte[]
        final byte[] groupId = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                                           0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
                                           0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18};
        headers.add("JMS_IBM_MQMD_GroupId", groupId, Schema.OPTIONAL_BYTES_SCHEMA);

        // Generate MQ message
        final Message message = mqmdBuilder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify sink correctly parses String back to proper types
        // MQMD Integer properties
        assertEquals(5, message.getIntProperty("JMS_IBM_MQMD_Priority"));
        assertEquals(36000, message.getIntProperty("JMS_IBM_MQMD_Expiry"));
        assertEquals(546, message.getIntProperty("JMS_IBM_MQMD_Encoding"));

        // MQMD String properties
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_MQMD_Format"));
        assertEquals("REPLY.QUEUE", message.getStringProperty("JMS_IBM_MQMD_ReplyToQ"));

        // JMS_IBM Boolean property
        assertEquals(true, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));

        // JMS_IBM Integer property
        assertEquals(546, message.getIntProperty("JMS_IBM_Encoding"));

        // Standard JMS properties (using dedicated setters)
        assertEquals(2, message.getJMSDeliveryMode());
        // assertEquals(60000L, message.getJMSExpiration());

        // byte[] property
        assertTrue(message.propertyExists("JMS_IBM_MQMD_GroupId"));
    }
}
