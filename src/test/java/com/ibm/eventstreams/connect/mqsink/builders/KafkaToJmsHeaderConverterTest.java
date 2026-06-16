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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import javax.jms.Message;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
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
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "5")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void copiesTypedMqmdIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }

    @Test
    public void rejectsInvalidLegacyMqmdIntegerHeader() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "abc")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        assertThrows(ConnectException.class, () -> converter.copyHeaderToJmsProperty(message, header));
    }

    @Test
    public void coercesMqmdIntegerHeaderFromLongSchema() throws Exception {
        final Header header = new ConnectHeaders().addLong(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5L)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, 5);
    }


    @Test
    public void copiesTypedBooleanHeader() throws Exception {
        final Header header = new ConnectHeaders().addBoolean(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true)
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);
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
    public void coercesIbmBooleanHeaderFromStringZero() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, "0")
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, false);
    }

    @Test
    public void coercesIbmBooleanHeaderFromInteger() throws Exception {
        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, 1)
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);
    }


    @Test
    public void copiesMqmdStringHeader() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_FORMAT, "MQSTR")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_FORMAT);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_MQMD_FORMAT, "MQSTR");
    }


    @Test
    public void copiesTypedByteArrayHeader() throws Exception {
        final byte[] value = new byte[] {0x01, 0x02};
        final Header header = new ConnectHeaders().add("customBytes", value, Schema.OPTIONAL_BYTES_SCHEMA)
                .lastWithName("customBytes");

        converter.copyHeaderToJmsProperty(message, header);

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(message).setObjectProperty(eq("customBytes"), captor.capture());
        assertArrayEquals(value, (byte[]) captor.getValue());
    }

    @Test
    public void skipsMqmdByteArrayHeaderWithStringValue() throws Exception {
        // Old Source Connector sends byte arrays as toString()
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_MSGID, "[B@42969b24")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message, never()).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_MSGID), eq("[B@42969b24"));
    }

    @Test
    public void copiesMqmdByteArrayHeaderWithByteArrayValue() throws Exception {
        final byte[] value = new byte[] {0x01, 0x02, 0x03};
        final Header header = new ConnectHeaders().add(JmsConstants.JMS_IBM_MQMD_MSGID, value, Schema.OPTIONAL_BYTES_SCHEMA)
                .lastWithName(JmsConstants.JMS_IBM_MQMD_MSGID);

        converter.copyHeaderToJmsProperty(message, header);

        final ArgumentCaptor<Object> captor = ArgumentCaptor.forClass(Object.class);
        verify(message).setObjectProperty(eq(JmsConstants.JMS_IBM_MQMD_MSGID), captor.capture());
        assertArrayEquals(value, (byte[]) captor.getValue());
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
    public void classifiesCharacterSetAsString() throws Exception {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_CHARACTER_SET, "819")
                .lastWithName(JmsConstants.JMS_IBM_CHARACTER_SET);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_CHARACTER_SET, "819");
    }

    @Test
    public void copiesJmsIbmReportException() throws Exception {
        final Header header = new ConnectHeaders().addInt(JmsConstants.JMS_IBM_REPORT_EXCEPTION, 1)
                .lastWithName(JmsConstants.JMS_IBM_REPORT_EXCEPTION);

        converter.copyHeaderToJmsProperty(message, header);

        verify(message).setObjectProperty(JmsConstants.JMS_IBM_REPORT_EXCEPTION, 1);
    }

    @Test
    public void integerParseFailureIncludesCause() {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_MQMD_PRIORITY, "abc")
                .lastWithName(JmsConstants.JMS_IBM_MQMD_PRIORITY);

        final ConnectException exception = assertThrows(ConnectException.class,
                () -> converter.copyHeaderToJmsProperty(message, header));

        assertNotNull(exception.getCause());
        assertEquals(NumberFormatException.class, exception.getCause().getClass());
    }

    @Test
    public void booleanParseFailureThrowsException() {
        final Header header = new ConnectHeaders().addString(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, "invalid")
                .lastWithName(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP);

        assertThrows(ConnectException.class, () -> converter.copyHeaderToJmsProperty(message, header));
    }


    @Test
    public void skipsNullHeaderValue() throws Exception {
        final Header header = new ConnectHeaders().add("nullHeader", null, Schema.OPTIONAL_STRING_SCHEMA)
                .lastWithName("nullHeader");

        converter.copyHeaderToJmsProperty(message, header);

        verify(message, never()).setObjectProperty(eq("nullHeader"), eq(null));
        verify(message, never()).setStringProperty(eq("nullHeader"), eq(null));
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
        
        // byte[]
        final byte[] bytes = new byte[]{1, 2, 3};
        header = new ConnectHeaders().addBytes("BytesProp", bytes).lastWithName("BytesProp");
        converter.copyHeaderToJmsProperty(message, header);
        verify(message).setObjectProperty("BytesProp", bytes);
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
}
