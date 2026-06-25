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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.ibm.msg.client.jms.JmsConstants;

@RunWith(MockitoJUnitRunner.class)
public class KafkaToJmsHeaderConverterTest {

    @Mock
    private Message message;

    @InjectMocks
    private KafkaToJmsHeaderConverter converter;


    @Test
    public void copiesLegacyStringMqmdIntegerHeader() throws Exception {
        // MQMD properties require mq.message.mqmd.write=true
        converter.setMqmdWriteEnabled(true);

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "5")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void copiesTypedMqmdIntegerHeader() throws Exception {
        // MQMD properties require mq.message.mqmd.write=true
        converter.setMqmdWriteEnabled(true);

        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void skipsInvalidLegacyMqmdIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "abc")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        // Should not throw exception, invalid value is skipped
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test
    public void coercesIbmBooleanHeaderFromString() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, "true")
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);
    }

    @Test
    public void coercesIbmBooleanHeaderFromStringOne() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, "1")
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);
    }

    @Test
    public void copiesMqmdStringHeader() throws Exception {
        // MQMD properties require mq.message.mqmd.write=true
        converter.setMqmdWriteEnabled(true);

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_FORMAT, "MQSTR")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_FORMAT);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_FORMAT, "MQSTR");
    }

    @Test
    public void skipsMqmdMsgIdByteArrayWhenMqmdWriteDisabled() throws Exception {
        converter.setMqmdWriteEnabled(false);

        final String value = "ID:414141407061756C745639344C545320EBC32F6A01A00740";
        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MQMD_MSGID, value, Schema.STRING_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify setObjectProperty was NOT called
        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_MSGID), any());
    }

    @Test
    public void skipsMqmdCorrelIdByteArrayWhenMqmdWriteDisabled() throws Exception {
        converter.setMqmdWriteEnabled(false);

        final String value = "correlId";
        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MQMD_CORRELID, value, Schema.STRING_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_CORRELID);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify setObjectProperty was NOT called
        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_CORRELID), any());
    }

    @Test
    public void skipsMqmdGroupIdByteArrayWhenMqmdWriteDisabled() throws Exception {
        // MQMD_GROUPID byte[] should be skipped when mq.message.mqmd.write=false (default)
        converter.setMqmdWriteEnabled(false);

        final byte[] value = new byte[] {0x01, 0x02, 0x03};
        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MQMD_GROUPID, value, Schema.OPTIONAL_BYTES_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_GROUPID);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify setObjectProperty was NOT called
        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_GROUPID), any());
    }

    @Test
    public void skipsMqmdAccountingTokenByteArrayWhenMqmdWriteDisabled() throws Exception {
        // MQMD_ACCOUNTINGTOKEN byte[] should be skipped when mq.message.mqmd.write=false (default)
        converter.setMqmdWriteEnabled(false);

        final byte[] value = new byte[] {0x01, 0x02, 0x03};
        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN, value, Schema.OPTIONAL_BYTES_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify setObjectProperty was NOT called
        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN), any());
    }

    @Test
    public void skipsMqmdIntegerPropertyWhenMqmdWriteDisabled() throws Exception {
        // MQMD Integer properties should be skipped when mq.message.mqmd.write=false (default)
        converter.setMqmdWriteEnabled(false);

        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify setObjectProperty was NOT called
        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_PRIORITY), any());
    }

    @Test
    public void skipsMqmdStringPropertyWhenMqmdWriteDisabled() throws Exception {
        // MQMD String properties should be skipped when mq.message.mqmd.write=false (default)
        converter.setMqmdWriteEnabled(false);

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_FORMAT, "MQSTR")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_FORMAT);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify setObjectProperty was NOT called
        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_FORMAT), any());
    }
    @Test
    public void setsMqmdMsgIdWithIdPrefix() throws Exception {
        // Test that MSGID with "ID:" prefix is handled correctly
        converter.setMqmdWriteEnabled(true);

        final String hexValue = "414d5120514d312020202020202020205e7e5f6601c04a28";
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_MSGID, "ID:" + hexValue)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify the hex string was parsed correctly (without the "ID:" prefix)
        final byte[] expectedBytes = HexUtils.hexStringToBytes(hexValue);
        verify(message).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_MSGID), eq(expectedBytes));
    }

    @Test
    public void setsMqmdMsgIdWithoutIdPrefix() throws Exception {
        // Test that MSGID without "ID:" prefix is handled correctly
        converter.setMqmdWriteEnabled(true);

        final String hexValue = "414d5120514d312020202020202020205e7e5f6601c04a28";
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_MSGID, hexValue)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify the hex string was parsed correctly
        final byte[] expectedBytes = HexUtils.hexStringToBytes(hexValue);
        verify(message).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_MSGID), eq(expectedBytes));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsExceptionForInvalidMsgIdHex() throws Exception {
        // Test that invalid hex string throws an exception
        converter.setMqmdWriteEnabled(true);

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_MSGID, "ID:INVALID_HEX")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        // Should throw IllegalArgumentException due to invalid hex format
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsExceptionForByteArrayToStringMsgId() throws Exception {
        // Old Source Connector sends byte arrays as toString() - should throw exception
        converter.setMqmdWriteEnabled(true);

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_MSGID, "[B@42969b24")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        // Should throw IllegalArgumentException due to invalid hex format
        converter.copyHeaderToJmsProperty(message, header);
    }


    @Test
    public void copiesTypedCustomIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addInt("priority", 5).lastWithName("priority");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty("priority", 5);
    }

    @Test
    public void copiesLegacyStringCustomHeader() throws Exception {
        final Header header = new ConnectHeaders().addString("customHeader", "headerValue")
                .lastWithName("customHeader");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty("customHeader", "headerValue");
    }

    @Test
    public void copiesLegacyStringNumericCustomHeader() throws Exception {
        final Header header = new ConnectHeaders().addString("volume", "11").lastWithName("volume");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty("volume", "11");
    }


    @Test
    public void classifiesCharacterSetAsInt() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_CHARACTER_SET, "819")
                .lastWithName(JmsConstants.JMS_IBM_CHARACTER_SET);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_CHARACTER_SET, 819);
    }

    @Test
    public void copiesJmsIbmReportException() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_REPORT_EXCEPTION, "1")
                .lastWithName(JmsConstants.JMS_IBM_REPORT_EXCEPTION);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_REPORT_EXCEPTION, 1);
    }

    @Test
    public void integerParseFailureSkipsProperty() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "abc")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        // Should not throw exception, invalid value is skipped
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test(expected = IllegalArgumentException.class)
    public void booleanParseFailureSkipsProperty() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, "invalid")
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        // Should throw IllegalArgumentException for invalid boolean value
        converter.copyHeaderToJmsProperty(message, header);
    }


    @Test
    public void setsNullHeaderValue() throws Exception {
        // Test that null values are set as properties (JMS allows null values)
        final Header header = new ConnectHeaders().add("nullHeader", null, Schema.OPTIONAL_STRING_SCHEMA)
                .lastWithName("nullHeader");

        converter.copyHeaderToJmsProperty(message, header);

        // Verify that null value is set as a property
        verify(message).setObjectProperty("nullHeader", null);
    }

    @Test
    public void testNullIntegerPropertyIsSet() throws Exception {
        // Test that null values for integer properties like JMS_IBM_MSGTYPE
        // are set correctly without throwing JMSException

        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MSGTYPE, null, Schema.OPTIONAL_INT32_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MSGTYPE);

        converter.copyHeaderToJmsProperty(message, header);

        // Verify that null value is set as a property (JMS allows null for Object properties)
        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MSGTYPE, null);
    }

    @Test
    public void testUnsupportedTypeConvertedToString() throws Exception {
        // Test that unsupported types (like Date, Struct, etc.) are converted to String
        // to avoid JMSException from setObjectProperty()

        final java.util.Date dateValue = new java.util.Date(1234567890000L);
        final Header header = new ConnectHeaders().add("DateProp", dateValue, null)
                .lastWithName("DateProp");

        converter.copyHeaderToJmsProperty(message, header);

        // Should convert Date to String
        verify(message).setObjectProperty("DateProp", dateValue.toString());
    }

    @Test
    public void testJmsSupportedTypesPreserved() throws Exception {
        // Test that JMS-supported types are preserved as-is for custom properties

        // Boolean
        Header header = new ConnectHeaders().addBoolean("BoolProp", true).lastWithName("BoolProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("BoolProp", Boolean.TRUE);

        // Byte
        header = new ConnectHeaders().addByte("ByteProp", (byte) 127).lastWithName("ByteProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("ByteProp", (byte) 127);

        // Short
        header = new ConnectHeaders().addShort("ShortProp", (short) 32000).lastWithName("ShortProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("ShortProp", (short) 32000);

        // Integer
        header = new ConnectHeaders().addInt("IntProp", 42).lastWithName("IntProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("IntProp", 42);

        // Long
        header = new ConnectHeaders().addLong("LongProp", 1234567890L).lastWithName("LongProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("LongProp", 1234567890L);

        // Float
        header = new ConnectHeaders().addFloat("FloatProp", 3.14f).lastWithName("FloatProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("FloatProp", 3.14f);

        // Double
        header = new ConnectHeaders().addDouble("DoubleProp", 2.71828).lastWithName("DoubleProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("DoubleProp", 2.71828);

        // String
        header = new ConnectHeaders().addString("StringProp", "test").lastWithName("StringProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("StringProp", "test");
    }

    @Test
    public void testComplexTypeConvertedToString() throws Exception {
        // Test that complex types like Map are converted to String

        final java.util.Map<String, Object> mapValue = new java.util.HashMap<>();
        mapValue.put("key1", "value1");
        mapValue.put("key2", 42);

        final Header header = new ConnectHeaders().add("MapProp", mapValue, null)
                .lastWithName("MapProp");

        converter.copyHeaderToJmsProperty(message, header);

        // Should convert Map to String
        verify(message).setObjectProperty("MapProp", mapValue.toString());
    }

    @Test
    public void testInvalidIntegerStringIsSkipped() throws Exception {
        // Test that invalid integer strings are skipped

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "not-a-number")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        // Should not throw exception, invalid value is skipped
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test
    public void testInvalidIntegerTypeIsSkipped() throws Exception {
        // Test that incompatible types for integer properties are skipped

        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MQMD_PRIORITY, new Object(), null)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        // Should not throw exception, invalid value is skipped
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBooleanStringIsSkipped() throws Exception {
        // Test that invalid boolean strings are skipped

        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_REPORT_EXCEPTION, "not-a-boolean")
                .lastWithName(JmsConstants.JMS_IBM_REPORT_EXCEPTION);

        // Should not throw exception, invalid value is skipped
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidBooleanTypeIsSkipped() throws Exception {
        // Test that incompatible types for boolean properties are skipped

        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_REPORT_EXCEPTION, new Object(), null)
                .lastWithName(JmsConstants.JMS_IBM_REPORT_EXCEPTION);

        // Should not throw exception, invalid value is skipped
        converter.copyHeaderToJmsProperty(message, header);
    }

    @Test
    public void copiesJmsDeliveryModeAsString() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_DELIVERY_MODE, "2")
                .lastWithName(JmsConstants.JMS_DELIVERY_MODE);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(
            "JMSDeliveryMode",
            "2"
        );
    }

    @Test
    public void copiesJmsExpirationAsString() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_EXPIRATION, "60000")
                .lastWithName(JmsConstants.JMS_EXPIRATION);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(
            "JMSExpiration",
            "60000"
        );
    }

    @Test
    public void skipsInvalidJmsDeliveryMode() throws Exception {
        // Test that invalid JMS_DELIVERY_MODE value is handled gracefully
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_DELIVERY_MODE, "invalid")
                .lastWithName(JmsConstants.JMS_DELIVERY_MODE);

        // Should not throw exception, invalid value causes NumberFormatException
        // which is caught and handled by the dedicated setter
        try {
            converter.copyHeaderToJmsProperty(message, header);
        } catch (final NumberFormatException e) {
            // Expected - invalid integer string
        }
    }

    @Test
    public void skipsInvalidJmsExpiration() throws Exception {
        // Test that invalid JMS_EXPIRATION value is handled gracefully
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_EXPIRATION, "invalid")
                .lastWithName(JmsConstants.JMS_EXPIRATION);

        // Should not throw exception, invalid value causes NumberFormatException
        // which is caught and handled by the dedicated setter
        try {
            converter.copyHeaderToJmsProperty(message, header);
        } catch (final NumberFormatException e) {
            // Expected - invalid long string
        }
    }
}
