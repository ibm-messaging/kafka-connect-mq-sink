/**
 * Copyright 2018, 2019, 2023, 2024, 2026 IBM Corporation
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
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *  Copies Kafka Connect headers to JMS message properties.
 *
 * <p>Supports both legacy deployments where headers arrive as strings (for example from older
 * MQ source connector versions) and typed headers produced by schema-aware connectors.
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
     * - These properties are IBM MQ extensions and may not be fully JMS-compliant
     * - The connector passes values through; IBM MQ will validate/reject invalid values
     *
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
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
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     */
    private static final Set<String> MQMD_BYTES_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_MQMD_MSGID,
            JmsConstants.JMS_IBM_MQMD_CORRELID,
            JmsConstants.JMS_IBM_MQMD_ACCOUNTINGTOKEN,
            JmsConstants.JMS_IBM_MQMD_GROUPID
    ));

    /**
     * All MQMD properties (Integer, String, and byte[]) that can only be set when mq.message.mqmd.write=true.
     * This is a combined set of all MQMD_INTEGER_PROPERTIES, MQMD_STRING_PROPERTIES, and MQMD_BYTES_PROPERTIES.
     *
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
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
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
     *
     * Note: Many JMS_IBM properties are read-only (set by IBM MQ) and cannot be set by applications.
     * Only settable properties are included here.
     */
    private static final Set<String> JMS_IBM_INTEGER_PROPERTIES = new HashSet<>(Arrays.asList(
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
     * Note: JMS_IBM_PutAppl, JMS_IBM_PutDate, JMS_IBM_PutTime are read-only and cannot be set.
     */
    private static final Set<String> JMS_IBM_STRING_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_FORMAT,
            JmsConstants.JMS_IBM_CHARACTER_SET
    ));

    /**
     * JMS_IBM properties that require Boolean type.
     */
    private static final Set<String> JMS_IBM_BOOLEAN_PROPERTIES = new HashSet<>(Arrays.asList(
            JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP
    ));

    /**
     * Copy a Kafka Connect header to a JMS message property with appropriate type conversion.
     *
     * IBM MQ properties require specific types according to IBM MQ JMS specification:
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=application-jms-message-object-properties
     * See: https://www.ibm.com/docs/en/ibm-mq/9.4.x?topic=messages-jms-fields-properties-corresponding-mqmd-fields
     *
     * @param message the JMS message
     * @param header  the Kafka Connect header
     * @throws javax.jms.JMSException if the property cannot be set
     * @throws ConnectException       if the value type is not compatible
     */
    public void copyHeaderToJmsProperty(final javax.jms.Message message, final org.apache.kafka.connect.header.Header header)
            throws javax.jms.JMSException {
        final String key = header.key();
        final Object value = header.value();
        
        if (value == null) {
            // Set null value as property - JMS allows null values
            // Source connector copies jms headers with null values to kafka headers.
            message.setObjectProperty(key, null);
            log.debug("Set property '{}' with null value", key);
            return;
        }

        final Object convertedValue = convertHeaderValue(key, value);
        
        if (convertedValue != null) {
            // ALL MQMD properties can only be set when mq.message.mqmd.write=true
            if (ALL_MQMD_PROPERTIES.contains(key)) {
                if (!mqmdWriteEnabled) {
                    log.debug("Skipping MQMD property '{}' - requires mq.message.mqmd.write=true", key);
                    return;
                }
            }
            
            message.setObjectProperty(key, convertedValue);
            log.debug("Set property '{}' with type {}: {}", key, convertedValue.getClass().getSimpleName(), convertedValue);
        }
    }
    

    /**
     * Convert Kafka header value to the expected JMS property type based on IBM MQ requirements.
     * Returns null if the value should be skipped (e.g., invalid byte array conversions).
     *
     * @param key   the property key
     * @param value the property value from Kafka header
     * @return the converted value, or null if the value should be skipped
     */
    Object convertHeaderValue(final String key, final Object value) {
        try {
            if (MQMD_BYTES_PROPERTIES.contains(key)) {
                return convertToByteArray(key, value);
            }
            
            if (MQMD_INTEGER_PROPERTIES.contains(key) || JMS_IBM_INTEGER_PROPERTIES.contains(key)) {
                return convertToInteger(key, value);
            }
            
            if (JMS_IBM_BOOLEAN_PROPERTIES.contains(key)) {
                return convertToBoolean(key, value);
            }
            
            if (MQMD_STRING_PROPERTIES.contains(key) || JMS_IBM_STRING_PROPERTIES.contains(key)) {
                return convertToString(value);
            }


            // JMS only supports: Boolean, Byte, Short, Integer, Long, Float, Double, String
            // For unsupported types, convert to String
            return ensureJmsSupportedType(key, value);
        } catch (final IllegalArgumentException e) {
            // Invalid value for the expected type - return null to skip this property
            log.warn("Skipping header '{}': {}", key, e.getMessage());
            return null;
        }
    }

    /**
     * Ensure the value is a type supported by JMS API
     * JMS supports: Boolean, Byte, Short, Integer, Long, Float, Double, String
     *
     * For unsupported types (Struct, Map, Array, Date, Time, Timestamp, Decimal, etc.),
     * convert to String representation.
     *
     * @param key   the property key (for logging)
     * @param value the property value
     * @return the value if it's a supported JMS type, or String representation otherwise
     */
    private Object ensureJmsSupportedType(final String key, final Object value) {
        // Check if value is already a JMS-supported type
        if (value instanceof Boolean || value instanceof String || value instanceof Number) {
            return value;
        }

        // For unsupported types, convert to String
        log.debug("Converting property '{}' from unsupported type {} to String",
                  key, value.getClass().getSimpleName());
        return value.toString();
    }

    /**
     * Convert value to byte array. 
     * Returns null if conversion is not possible (e.g., String from an old Source Connector version).
     */
    private Object convertToByteArray(final String key, final Object value) {
        if (value instanceof byte[]) {
            return value;
        }

        log.debug("Skipping byte array property '{}' with incompatible type: {}", 
                  key, value.getClass().getSimpleName());
        return null;
    }

    /**
     * Convert value to Integer.
     * Supports Integer, Number, and String (for backward compatibility with old Source Connector versions).
     *
     * @throws IllegalArgumentException if the value cannot be converted to Integer
     */
    private Object convertToInteger(final String key, final Object value) {
        if (value instanceof Integer) {
            return value;
        }
        
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        
        if (value instanceof String) {
            try {
                return Integer.valueOf((String) value);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("Failed to parse integer property '" + key + "': " + value, e);
            }
        }
        
        throw new IllegalArgumentException("Property '" + key + "' requires Integer type, got: " +
                                           value.getClass().getName());
    }

    /**
     * Convert value to Boolean.
     * Supports Boolean, Integer (0/1), and String (for backward compatibility with old Source Connector versions).
     *
     * @throws IllegalArgumentException if the value cannot be converted to Boolean
     */
    private Object convertToBoolean(final String key, final Object value) {
        if (value instanceof Boolean) {
            return value;
        }
        
        if (value instanceof Integer) {
            return ((Integer) value) != 0;
        }
        
        if (value instanceof String) {
            final String strValue = (String) value;
            if ("true".equalsIgnoreCase(strValue) || "1".equals(strValue)) {
                return Boolean.TRUE;
            }
            if ("false".equalsIgnoreCase(strValue) || "0".equals(strValue)) {
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("Failed to parse boolean property '" + key + "': " + value);
        }
        
        throw new IllegalArgumentException("Property '" + key + "' requires Boolean type, got: " +
                                           value.getClass().getName());
    }

    /**
     * Convert value to String.
     * Accepts String directly or converts from other types.
     */
    private Object convertToString(final Object value) {
        return value instanceof String ? value : value.toString();
    }
}
