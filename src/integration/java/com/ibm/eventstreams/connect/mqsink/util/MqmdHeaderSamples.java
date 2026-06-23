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
package com.ibm.eventstreams.connect.mqsink.util;

import org.apache.kafka.connect.header.ConnectHeaders;

import com.ibm.msg.client.jms.JmsConstants;

/**
 * MQMD header samples from the client issue: Kafka string values as produced by MQ source.
 */
public final class MqmdHeaderSamples {

    private MqmdHeaderSamples() {
    }

    /** IBM MQ integer MQMD field copied as a string Kafka header. */
    public static final class IntegerProperty {
        private final String key;
        private final String stringValue;
        private final int intValue;

        public IntegerProperty(final String key, final String stringValue, final int intValue) {
            this.key = key;
            this.stringValue = stringValue;
            this.intValue = intValue;
        }

        public String key() {
            return key;
        }

        public String stringValue() {
            return stringValue;
        }

        public int intValue() {
            return intValue;
        }

        public ConnectHeaders asSingleHeader() {
            final ConnectHeaders headers = new ConnectHeaders();
            headers.addString(key, stringValue);
            return headers;
        }
    }

    /** IBM MQ string MQMD field copied as a string Kafka header. */
    public static final class StringProperty {
        private final String key;
        private final String value;

        public StringProperty(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        public ConnectHeaders asSingleHeader() {
            final ConnectHeaders headers = new ConnectHeaders();
            headers.addString(key, value);
            return headers;
        }
    }

    /** Custom (non-MQMD) business header copied as a string Kafka header. */
    public static final class CustomProperty {
        private final String key;
        private final String value;

        public CustomProperty(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        public ConnectHeaders asSingleHeader() {
            final ConnectHeaders headers = new ConnectHeaders();
            headers.addString(key, value);
            return headers;
        }
    }

    public static IntegerProperty[] integerMqmdProperties() {
        return new IntegerProperty[] {
                new IntegerProperty(JmsConstants.JMS_IBM_MQMD_PRIORITY, "2", 2),
                new IntegerProperty(JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID, "437", 437),
                new IntegerProperty(JmsConstants.JMS_IBM_ENCODING, "546", 546),
                new IntegerProperty(JmsConstants.JMS_IBM_MQMD_EXPIRY, "-1", -1),
                new IntegerProperty(JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER, "1", 1),
                new IntegerProperty(JmsConstants.JMS_IBM_MQMD_MSGFLAGS, "0", 0),
                new IntegerProperty(JmsConstants.JMS_IBM_MQMD_OFFSET, "0", 0),
                new IntegerProperty(JmsConstants.JMS_IBM_MSGTYPE, "1", 1),
                new IntegerProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE, "11", 11),
        };
    }

    public static StringProperty[] stringMqmdProperties() {
        return new StringProperty[] {
                new StringProperty(JmsConstants.JMS_IBM_CHARACTER_SET, "IBM437"),
                new StringProperty(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME, "C:\\rfhutilc.exe             "),
                new StringProperty(JmsConstants.JMS_IBM_MQMD_PUTTIME, "10214957"),
                new StringProperty(JmsConstants.JMS_IBM_MQMD_PUTDATE, "20260521"),
                new StringProperty(JmsConstants.JMS_IBM_FORMAT, "MQSTR   "),
        };
    }

    public static CustomProperty[] customProperties() {
        return new CustomProperty[] {
                new CustomProperty("facilityCountryCode", "US"),
                new CustomProperty("volume", "11"),
                new CustomProperty("enabled", "true"),
        };
    }

    public static ConnectHeaders clientMqmdPipelineAsStrings() {
        final ConnectHeaders headers = new ConnectHeaders();
        for (final IntegerProperty property : integerMqmdProperties()) {
            headers.addString(property.key(), property.stringValue());
        }
        for (final StringProperty property : stringMqmdProperties()) {
            headers.addString(property.key(), property.value());
        }
        return headers;
    }
}
