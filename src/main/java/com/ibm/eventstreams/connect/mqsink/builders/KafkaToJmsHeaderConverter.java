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


import com.ibm.msg.client.jms.JmsConstants;

import org.apache.kafka.connect.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Copies Kafka Connect headers to JMS message properties.
 *
 * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
 * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
 * IBM MQ JMS message object properties
 */
public class KafkaToJmsHeaderConverter {
    private static final Logger log = LoggerFactory.getLogger(KafkaToJmsHeaderConverter.class);

    private boolean mqmdWriteEnabled = false;

    /**
     * Sets whether MQMD write is enabled (mq.message.mqmd.write=true).
     * @param enabled true if MQMD write is enabled
     */
    public void setMqmdWriteEnabled(final boolean enabled) {
        this.mqmdWriteEnabled = enabled;
    }

    /**
     * MQMD properties that require Integer type according to IBM MQ JMS specification.
     *
     * IMPORTANT NOTES about JMS Specification Compliance:
     * - JMS_IBM_MQMD_Priority: Values outside 0-9 range violate JMS specification
     * - JMS_IBM_MQMD_BackoutCount: Value can't be set by the connector. Any value set is ignored
     * - JMS_IBM_MQMD_PutApplType: Requires WMQ_MQMD_MESSAGE_CONTEXT to be set to ALL context
     * - The connector passes values through; IBM MQ will validate/reject invalid values
     */
    private static final Set<String> MQMD_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_REPORT,
            JmsConstants.JMS_IBM_MQMD_MSGTYPE,
            JmsConstants.JMS_IBM_MQMD_EXPIRY,
            JmsConstants.JMS_IBM_MQMD_FEEDBACK,
            JmsConstants.JMS_IBM_MQMD_ENCODING,
            JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID,
            JmsConstants.JMS_IBM_MQMD_PRIORITY,
            JmsConstants.JMS_IBM_MQMD_PERSISTENCE,
            JmsConstants.JMS_IBM_MQMD_PUTAPPLTYPE,
            JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER,
            JmsConstants.JMS_IBM_MQMD_OFFSET,
            JmsConstants.JMS_IBM_MQMD_MSGFLAGS,
            JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH
    ));

    /**
     * MQMD properties that require String type according to IBM MQ JMS specification.
     *
     * IMPORTANT: Some properties require WMQ_MQMD_MESSAGE_CONTEXT to be set on the Destination:
     * - IDENTITY context (mq.message.mqmd.context=identity): UserIdentifier, AccountingToken, ApplIdentityData
     * - ALL context (mq.message.mqmd.context=all): PutApplType, PutApplName, PutDate, PutTime, ApplOriginData
     */
    private static final Set<String> MQMD_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_FORMAT,
            JmsConstants.JMS_IBM_MQMD_REPLYTOQ,
            JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR,
            JmsConstants.JMS_IBM_MQMD_USERIDENTIFIER,
            JmsConstants.JMS_IBM_MQMD_APPLIDENTITYDATA,
            JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME,
            JmsConstants.JMS_IBM_MQMD_PUTDATE,            
            JmsConstants.JMS_IBM_MQMD_PUTTIME,
            JmsConstants.JMS_IBM_MQMD_APPLORIGINDATA
    ));

    /**
     * MQMD properties that require byte array type according to IBM MQ JMS specification.
     *
     * IMPORTANT: AccountingToken requires WMQ_MQMD_MESSAGE_CONTEXT to be set to IDENTITY or ALL context.
     *
     * When these properties are received from the default SimpleHeaderConverter, the source
     * connector serializes the byte[] as Base64 (via Values.convertToString), so the connector 
     * receives a Base64-encoded String which must be decoded back to byte[].
     * When using ByteArrayConverter or JsonConverter, the byte[] arrives as-is.
     */
    private static final Set<String> MQMD_BYTES_PROPERTIES = new HashSet<>(Arrays.asList(
            // JmsConstants.JMS_IBM_MQMD_CORRELID, - this can be set by setJMSCorrelationID 
            JmsConstants.JMS_IBM_MQMD_MSGID,
            JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN,
            JmsConstants.JMS_IBM_MQMD_GROUPID
    ));

    /**
     * All MQMD properties (Integer, String, and byte[]) that can only be set when mq.message.mqmd.write=true.
     * This is a combined set of all MQMD_INTEGER_PROPERTIES, MQMD_STRING_PROPERTIES, and MQMD_BYTES_PROPERTIES.
     */
    private static final Set<String> ALL_MQMD_PROPERTIES;

    static {
        ALL_MQMD_PROPERTIES = new HashSet<>();
        ALL_MQMD_PROPERTIES.addAll(MQMD_INTEGER_PROPERTIES);
        ALL_MQMD_PROPERTIES.addAll(MQMD_STRING_PROPERTIES);
        ALL_MQMD_PROPERTIES.addAll(MQMD_BYTES_PROPERTIES);
    }

    /**
     * JMS_IBM properties that require Integer type.
     * These are standard JMS properties that map to MQMD fields and can be set by applications.
     *
     * Note: Many JMS_IBM properties are read-only (set by IBM MQ) and cannot be set by applications.
     * Only settable properties are included here.
     */
    private static final Set<String> JMS_IBM_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_CHARACTER_SET,
            JmsConstants.JMS_IBM_REPORT_EXCEPTION,
            JmsConstants.JMS_IBM_REPORT_EXPIRATION,
            JmsConstants.JMS_IBM_REPORT_COA,
            JmsConstants.JMS_IBM_REPORT_COD,
            JmsConstants.JMS_IBM_REPORT_PAN,
            JmsConstants.JMS_IBM_REPORT_NAN,
            JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID,
            JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID,
            JmsConstants.JMS_IBM_REPORT_DISCARD_MSG,
            JmsConstants.JMS_IBM_MSGTYPE,
            JmsConstants.JMS_IBM_FEEDBACK,
            JmsConstants.JMS_IBM_ENCODING,
            JmsConstants.JMS_IBM_PUTAPPLTYPE
    ));

    /**
     * JMS_IBM properties (non-MQMD) that require String type.
     */
    private static final Set<String> JMS_IBM_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_FORMAT,
            JmsConstants.JMS_IBM_PUTDATE,
            JmsConstants.JMS_IBM_PUTTIME,
            JmsConstants.JMS_IBM_CHARACTER_SET
    ));

    /**
     * JMS_IBM properties that require Boolean type.
     */
    private static final Set<String> JMS_IBM_BOOLEAN_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP
    ));

    private static final Set<String> MQMD_PROPERTIES_TO_IGNORE = new HashSet<>(Arrays.asList(
            // Value that should not be set by the connector
            JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT
    ));

    /**
     * Java types accepted by {@link javax.jms.Message#setObjectProperty}.
     * Any user-defined property value whose class is not in this set must be
     * converted to String before being passed to setObjectProperty.
     */
    private static final Set<Class<?>> JMS_SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
            Boolean.class, Byte.class, Short.class,
            Integer.class, Long.class, Float.class,
            Double.class, String.class
    ));    



    /**
     * Copies a Kafka Connect header to a JMS message property with type-correct conversion.
     *
     * The IBM MQ JMS specification requires specific Java types for MQMD and JMS_IBM
     * properties. This method ensures values from Kafka headers are coerced into the
     * correct types. 
     * 
     * @param message the target JMS message
     * @param header  the source Kafka Connect header
     * @throws IllegalArgumentException if the property name is illegal or value conversion fails
     */
    public void copyHeaderToJmsProperty(final Message message, final Header header) {
        final String key = header.key();
        final Object value = header.value();

        if (MQMD_PROPERTIES_TO_IGNORE.contains(key)) {
            log.debug("Skipping read-only MQMD property '{}'", key);
            return;
        }

        if (ALL_MQMD_PROPERTIES.contains(key) && !mqmdWriteEnabled) {
            log.debug("Skipping MQMD property '{}': requires mq.message.mqmd.write=true", key);
            return;
        }

        try {
            if (value == null) {
                // Set null value as property - JMS allows null values
                // Source connector copies jms headers with null values to kafka headers.
                message.setObjectProperty(key, null);
                log.debug("Set property '{}' with null value", key);
                return;
            }

            setJmsProperty(message, key, value);
        } catch (final JMSException e) {
            throw new IllegalArgumentException(
                    String.format("Failed to set JMS property '%s' (type: %s, value: %s)",
                            key, value == null ? "null" : value.getClass().getSimpleName(), value), e);
        }
    }

    /**
     * Sets the appropriate JMS property on the message, handling special-cased header keys
     * (CorrelId, MsgId) before falling through to typed property conversion.
     */
    private void setJmsProperty(final Message message, final String key, final Object value)
            throws JMSException {

        if (JmsConstants.JMS_IBM_MQMD_CORRELID.equals(key)) {
            if (value instanceof byte[]) {
                message.setJMSCorrelationIDAsBytes((byte[]) value);
            } else {
                String valueString = value.toString();
                if (valueString.startsWith("ID:")) {
                    valueString = valueString.substring(3);
                }

                message.setJMSCorrelationID(valueString);
            }
            return;
        }

        if (JmsConstants.JMS_IBM_MQMD_MSGID.equals(key)) {
            if (value instanceof byte[]) {
                message.setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID, value);
            } else {
                String hexString = value.toString();
                if (hexString.startsWith("ID:")) {
                    hexString = hexString.substring(3);
                }
                
                final byte[] msgIdBytes = HexUtils.hexStringToBytes(hexString);

                message.setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID, msgIdBytes);
            }
            return;
        }

        final Object converted = convertToJmsType(key, value);
        message.setObjectProperty(key, converted);
        log.debug("Set property '{}' as {}", key, converted);
    }

    /**
     * Converts a Kafka header value to the Java type required by the IBM MQ JMS specification
     * for the given property key.
     *
     * If the value is already the correct type, it is returned as-is. 
     * String values are coerced to the required type.
     *
     * @param key   the JMS property name
     * @param value the raw value from the Kafka header
     * @return the converted value, never {@code null}
     * @throws IllegalArgumentException if conversion fails or the value is of an unexpected type
     */
    Object convertToJmsType(final String key, final Object value) {
        if (MQMD_BYTES_PROPERTIES.contains(key)) {
            return toByteArray(key, value);
        }
        if (MQMD_INTEGER_PROPERTIES.contains(key) || JMS_IBM_INTEGER_PROPERTIES.contains(key)) {
            return toInteger(key, value);
        }
        if (JMS_IBM_BOOLEAN_PROPERTIES.contains(key)) {
            return toBoolean(key, value);
        }
        if (MQMD_STRING_PROPERTIES.contains(key) || JMS_IBM_STRING_PROPERTIES.contains(key)) {
            return value.toString();
        }
        if (JMS_SUPPORTED_TYPES.contains(value.getClass())) {
            return value;
        }
        // user-defined property that is not a supported type 
        //  convert to String for JMS compatibility
        return value.toString();
    }


    private byte[] toByteArray(final String key, final Object value) {
        if (value instanceof byte[]) {
            // if the connector is paired with ByteArrayConverter
            //  or JsonConverter (with schemas.enable=true), we will 
            //  have a byte[] we can use directly
            return (byte[]) value;
        }
        try {
            // We cannot distinguish between a string that came from 
            //  StringConverter or SimpleHeaderConverter, so we are
            //  assuming SimpleHeaderConverter behaviour 
            // (as it is the default converter in Connect)

            // if the connector is paired with StringConverter, we 
            //   will have a string representation of the bytes
            // return value.toString().getBytes();

            // if the connector is paired with SimpleHeaderConverter, we 
            //   will likely have a base64-encoded string representation 
            //   of the byte array
            return Base64.getDecoder().decode(value.toString());
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format("Property '%s': value is not valid Base64: '%s'", key, value), e);
        }
    }

    private Integer toInteger(final String key, final Object value) {
        if (value instanceof Integer) {
            // if the connector is paired with ByteArrayConverter, JsonConverter (with
            //   schemas.enable=true), IntegerConverter, or a typed header added directly,
            //   we will have an Integer we can use directly
            return (Integer) value;
        }
        if (value instanceof Number) {
            // if the connector is paired with a numeric converter (ShortConverter,
            //   LongConverter, FloatConverter, DoubleConverter) or JsonConverter
            //   (with schemas.enable=true) with a non-Integer numeric type, we will
            //   have a Number we can convert to int
            return ((Number) value).intValue();
        }
        // if the connector is paired with SimpleHeaderConverter or StringConverter,
        //   we will have a string representation of the integer
        try {
            return Integer.parseInt(value.toString());
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Property '%s': cannot convert '%s' to Integer", key, value), e);
        }
    }

    private Boolean toBoolean(final String key, final Object value) {
        if (value instanceof Boolean) {
            // if the connector is paired with 
            //  JsonConverter (with schemas.enable=true), we will 
            //  have a boolean we can use directly
            return (Boolean) value;
        }

        final String s = value.toString();
        if ("true".equalsIgnoreCase(s) || "1".equals(s)) return Boolean.TRUE;
        if ("false".equalsIgnoreCase(s) || "0".equals(s)) return Boolean.FALSE;
        throw new IllegalArgumentException(
                String.format("Property '%s': cannot parse '%s' as Boolean (expected true/false/1/0)", key, value));
    }
}
