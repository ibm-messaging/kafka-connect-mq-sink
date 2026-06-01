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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Message;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT;

public class DefaultMessageBuilderWithHeadersIT extends AbstractJMSContextIT {

    private MessageBuilder builder;

    @Before
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
        //
        // The following MQMD properties require Integer type:
        // - JMS_IBM_MQMD_Report (Integer)
        // - JMS_IBM_MQMD_MsgType (Integer)
        // - JMS_IBM_MQMD_Expiry (Integer)
        // - JMS_IBM_MQMD_Feedback (Integer)
        // - JMS_IBM_MQMD_Encoding (Integer)
        // - JMS_IBM_MQMD_CodedCharSetId (Integer)
        // - JMS_IBM_MQMD_Priority (Integer)
        // - JMS_IBM_MQMD_Persistence (Integer)
        // - JMS_IBM_MQMD_PutApplType (Integer)
        // - JMS_IBM_MQMD_MsgSeqNumber (Integer)
        // - JMS_IBM_MQMD_Offset (Integer)
        // - JMS_IBM_MQMD_MsgFlags (Integer)
        // - JMS_IBM_MQMD_OriginalLength (Integer)

        final ConnectHeaders headers = new ConnectHeaders();

        // Priority property - Integer (0-9)
        headers.addInt("JMS_IBM_MQMD_Priority", 5);

        // Expiry property - Integer (time-to-live in tenths of a second, or MQEXPIRY_DEFAULT, MQEXPIRY_UNLIMITED)
        headers.addInt("JMS_IBM_MQMD_Expiry", 36000); // 1 hour in tenths of a second

        // MsgType property - Integer
        headers.addInt("JMS_IBM_MQMD_MsgType", 1); // MQMT_REQUEST

        // Report property - Integer (report options)
        headers.addInt("JMS_IBM_MQMD_Report", 0);

        // Encoding property - Integer (numeric encoding)
        headers.addInt("JMS_IBM_MQMD_Encoding", 546); // MQENC_NATIVE

        // CodedCharSetId property - Integer (character set)
        headers.addInt("JMS_IBM_MQMD_CodedCharSetId", 819); // UTF-8

        // Persistence property - Integer (MQPER_PERSISTENT, MQPER_NOT_PERSISTENT)
        headers.addInt("JMS_IBM_MQMD_Persistence", 1); // MQPER_PERSISTENT

        // Feedback property - Integer (feedback code)
        headers.addInt("JMS_IBM_MQMD_Feedback", 0);

        // PutApplType property - Integer (application type)
        headers.addInt("JMS_IBM_MQMD_PutApplType", 11); // MQAT_WINDOWS

        // MsgSeqNumber property - Integer
        headers.addInt("JMS_IBM_MQMD_MsgSeqNumber", 1);

        // Offset property - Integer
        headers.addInt("JMS_IBM_MQMD_Offset", 0);

        // MsgFlags property - Integer
        headers.addInt("JMS_IBM_MQMD_MsgFlags", 0);

        // OriginalLength property - Integer
        headers.addInt("JMS_IBM_MQMD_OriginalLength", -1); // MQOL_UNDEFINED

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

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
    public void buildMessageWithMQMDStringProperties() throws Exception {
        // Test MQMD String properties according to IBM MQ documentation:
        // The following MQMD properties require String type:
        // - JMS_IBM_MQMD_Format (String)
        // - JMS_IBM_MQMD_ReplyToQ (String)
        // - JMS_IBM_MQMD_ReplyToQMgr (String)
        // - JMS_IBM_MQMD_UserIdentifier (String)
        // - JMS_IBM_MQMD_ApplIdentityData (String)
        // - JMS_IBM_MQMD_PutApplName (String)
        // - JMS_IBM_MQMD_PutDate (String)
        // - JMS_IBM_MQMD_PutTime (String)
        // - JMS_IBM_MQMD_ApplOriginData (String)

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
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

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
    public void buildMessageWithMQMDByteArrayProperties() throws Exception {
        // Test MQMD byte[] properties according to IBM MQ documentation:
        // The following MQMD properties have Object (byte[]) type:
        // - JMS_IBM_MQMD_MsgId (Object byte[])
        // - JMS_IBM_MQMD_CorrelId (Object byte[])
        // - JMS_IBM_MQMD_AccountingToken (Object byte[])
        // - JMS_IBM_MQMD_GroupId (Object byte[])
        //
        // See: MessageDescriptorBuilder.java for example usage of setObjectProperty()
        // https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties

        final ConnectHeaders headers = new ConnectHeaders();

        // These are byte array values - Kafka headers supports byte arrays
        byte[] msgId = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                    0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] correlId = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                      0x00, 0x00, 0x00, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E};
        byte[] accountingToken = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                                              0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
                                              0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                                              0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20};
        byte[] groupId = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                      0x00, 0x00, 0x00, 0x05, 0x06, 0x07, 0x08, 0x09};

        headers.addBytes("JMS_IBM_MQMD_MsgId", msgId);
        headers.addBytes("JMS_IBM_MQMD_CorrelId", correlId);
        headers.addBytes("JMS_IBM_MQMD_AccountingToken", accountingToken);
        headers.addBytes("JMS_IBM_MQMD_GroupId", groupId);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify byte[] properties can be retrieved as Object properties
        // JMS spec: byte[] properties are retrieved as Object type
        Object msgIdObj = message.getObjectProperty("JMS_IBM_MQMD_MsgId");
        assertNotNull("JMS_IBM_MQMD_MsgId property should not be null", msgIdObj);
        assertTrue("JMS_IBM_MQMD_MsgId should be a byte array", msgIdObj instanceof byte[]);
        assertArrayEquals("JMS_IBM_MQMD_MsgId should match the original byte array", msgId, (byte[]) msgIdObj);

        Object correlIdObj = message.getObjectProperty("JMS_IBM_MQMD_CorrelId");
        assertNotNull("JMS_IBM_MQMD_CorrelId property should not be null", correlIdObj);
        assertTrue("JMS_IBM_MQMD_CorrelId should be a byte array", correlIdObj instanceof byte[]);
        assertArrayEquals("JMS_IBM_MQMD_CorrelId should match the original byte array", correlId, (byte[]) correlIdObj);

        Object accountingTokenObj = message.getObjectProperty("JMS_IBM_MQMD_AccountingToken");
        assertNotNull("JMS_IBM_MQMD_AccountingToken property should not be null", accountingTokenObj);
        assertTrue("JMS_IBM_MQMD_AccountingToken should be a byte array", accountingTokenObj instanceof byte[]);
        assertArrayEquals("JMS_IBM_MQMD_AccountingToken should match the original byte array", accountingToken, (byte[]) accountingTokenObj);

        Object groupIdObj = message.getObjectProperty("JMS_IBM_MQMD_GroupId");
        assertNotNull("JMS_IBM_MQMD_GroupId property should not be null", groupIdObj);
        assertTrue("JMS_IBM_MQMD_GroupId should be a byte array", groupIdObj instanceof byte[]);
        assertArrayEquals("JMS_IBM_MQMD_GroupId should match the original byte array", groupId, (byte[]) groupIdObj);
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
        //
        // The following JMS_IBM properties require Integer type and can be set by applications:
        // - JMS_IBM_Report_Exception (Integer)
        // - JMS_IBM_Report_Expiration (Integer)
        // - JMS_IBM_Report_COA (Integer)
        // - JMS_IBM_Report_COD (Integer)
        // - JMS_IBM_Report_PAN (Integer)
        // - JMS_IBM_Report_NAN (Integer)
        // - JMS_IBM_Report_Pass_Msg_ID (Integer)
        // - JMS_IBM_Report_Pass_Correl_ID (Integer)
        // - JMS_IBM_Report_Discard_Msg (Integer)
        // - JMS_IBM_MsgType (Integer)
        // - JMS_IBM_Feedback (Integer)
        // - JMS_IBM_Encoding (Integer)
        // - JMS_IBM_Character_Set (Integer)
        // - JMS_IBM_PutApplType (Integer)

        final ConnectHeaders headers = new ConnectHeaders();

        // Report options - Integer values
        headers.addInt("JMS_IBM_Report_Exception", 1);
        headers.addInt("JMS_IBM_Report_Expiration", 1);
        headers.addInt("JMS_IBM_Report_COA", 1);
        headers.addInt("JMS_IBM_Report_COD", 1);
        headers.addInt("JMS_IBM_Report_PAN", 0);
        headers.addInt("JMS_IBM_Report_NAN", 0);
        headers.addInt("JMS_IBM_Report_Pass_Msg_ID", 1);
        headers.addInt("JMS_IBM_Report_Pass_Correl_ID", 1);
        headers.addInt("JMS_IBM_Report_Discard_Msg", 0);

        // Message type - Integer
        headers.addInt("JMS_IBM_MsgType", 8); // MQMT_DATAGRAM

        // Feedback - Integer
        headers.addInt("JMS_IBM_Feedback", 0);

        // Encoding - Integer (numeric encoding)
        headers.addInt("JMS_IBM_Encoding", 546); // MQENC_NATIVE

        // Character Set - Integer
        headers.addInt("JMS_IBM_Character_Set", 819); // UTF-8

        // Put Application Type - Integer
        headers.addInt("JMS_IBM_PutApplType", 11); // MQAT_WINDOWS

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
        headers.addBoolean("JMS_IBM_Last_Msg_In_Group", true);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify JMS_IBM boolean property can be retrieved with correct type
        assertEquals(true, message.getBooleanProperty("JMS_IBM_Last_Msg_In_Group"));
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

        final ConnectHeaders headers = new ConnectHeaders();

        // MQMD Integer properties
        headers.addInt("JMS_IBM_MQMD_Priority", 5);
        headers.addInt("JMS_IBM_MQMD_Expiry", 36000);
        headers.addInt("JMS_IBM_MQMD_Encoding", 546);

        // MQMD String properties
        headers.addString("JMS_IBM_MQMD_Format", "MQSTR");
        headers.addString("JMS_IBM_MQMD_ReplyToQ", "REPLY.QUEUE");

        // MQMD byte[] properties
        byte[] mqmdMsgId = new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                        0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05};
        headers.addBytes("JMS_IBM_MQMD_MsgId", mqmdMsgId);

        // JMS_IBM Integer properties
        headers.addInt("JMS_IBM_Encoding", 546);
        headers.addInt("JMS_IBM_Character_Set", 819);
        headers.addInt("JMS_IBM_MsgType", 8);

        // JMS_IBM String properties
        headers.addString("JMS_IBM_Format", "MQSTR");

        // JMS_IBM Boolean properties
        headers.addBoolean("JMS_IBM_Last_Msg_In_Group", true);

        // Regular properties (should be converted to strings)
        headers.addString("CustomStringProp", "custom_value");
        headers.addInt("CustomIntProp", 999);

        // generate MQ message
        final Message message = builder.fromSinkRecord(getJmsContext(), generateSinkRecord(headers));

        // Verify MQMD properties
        assertEquals(5, message.getIntProperty("JMS_IBM_MQMD_Priority"));
        assertEquals(36000, message.getIntProperty("JMS_IBM_MQMD_Expiry"));
        assertEquals(546, message.getIntProperty("JMS_IBM_MQMD_Encoding"));
        assertEquals("MQSTR", message.getStringProperty("JMS_IBM_MQMD_Format"));
        assertEquals("REPLY.QUEUE", message.getStringProperty("JMS_IBM_MQMD_ReplyToQ"));

        Object mqmdMsgIdObj = message.getObjectProperty("JMS_IBM_MQMD_MsgId");
        assertNotNull(mqmdMsgIdObj);
        assertTrue(mqmdMsgIdObj instanceof byte[]);
        assertArrayEquals(mqmdMsgId, (byte[]) mqmdMsgIdObj);

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
}
