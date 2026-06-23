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

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.TextMessage;

import com.ibm.msg.client.jms.JmsConstants;

/**
 * JMS property sets mirroring {@code mq-jms-test-put} scenarios for source-output sink ITs.
 */
public final class JmsTestPropertySets {

    private JmsTestPropertySets() {
    }

    public enum Scenario {
        CUSTOM,
        LEGACY,
        TYPES,
        MQMD
    }

    public static void applyScenario(final TextMessage message, final Scenario scenario) throws JMSException {
        switch (scenario) {
            case CUSTOM:
                applyCustomProperties(message);
                break;
            case LEGACY:
                applyLegacyStringProperties(message);
                break;
            case TYPES:
                applyAllTypesProperties(message);
                break;
            case MQMD:
                applySafeMqmdProperties(message);
                break;
            default:
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }

    private static void applyCustomProperties(final TextMessage message) throws JMSException {
        message.setStringProperty("facilityCountryCode", "US");
        message.setIntProperty("volume", 11);
        message.setDoubleProperty("decimalmeaning", 42.0);
        message.setBooleanProperty("enabled", true);
        message.setLongProperty("createdAt", 1_609_459_200_000L);
    }

    private static void applyAllTypesProperties(final TextMessage message) throws JMSException {
        message.setStringProperty("stringProp", "hello");
        message.setIntProperty("intProp", 42);
        message.setLongProperty("longProp", 9_000_000_000L);
        message.setShortProperty("shortProp", (short) 7);
        message.setByteProperty("byteProp", (byte) 3);
        message.setFloatProperty("floatProp", 3.14f);
        message.setDoubleProperty("doubleProp", 2.718);
        message.setBooleanProperty("booleanProp", true);
    }

    private static void applyLegacyStringProperties(final TextMessage message) throws JMSException {
        message.setStringProperty("facilityCountryCode", "US");
        message.setStringProperty("volume", "11");
        message.setStringProperty("decimalmeaning", "42.0");
        message.setStringProperty("enabled", "true");
        message.setStringProperty("createdAt", "1609459200000");
        message.setStringProperty("customBytesHex", "01020304");
    }

    private static void applySafeMqmdProperties(final TextMessage message) throws JMSException {
        message.setJMSPriority(5);
        message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);

        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_CODEDCHARSETID, 1208);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_ENCODING, 273);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGSEQNUMBER, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGFLAGS, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_OFFSET, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_REPORT, 2);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_FEEDBACK, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_MSGTYPE, 8);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_ORIGINALLENGTH, 1);

        message.setIntProperty(JmsConstants.JMS_IBM_ENCODING, 273);
        message.setIntProperty(JmsConstants.JMS_IBM_MSGTYPE, 8);
        message.setIntProperty(JmsConstants.JMS_IBM_FEEDBACK, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_RETAIN, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_MQMD_BACKOUTCOUNT, 1);
        message.setBooleanProperty(JmsConstants.JMS_IBM_LAST_MSG_IN_GROUP, true);

        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_EXCEPTION, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_EXPIRATION, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_COA, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_COD, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_PAN, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_NAN, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_PASS_MSG_ID, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_PASS_CORREL_ID, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_REPORT_DISCARD_MSG, 1);
        message.setIntProperty(JmsConstants.JMS_IBM_PUTAPPLTYPE, 1);

        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_REPLYTOQ, "REPLY.Q");
        message.setStringProperty(JmsConstants.JMS_IBM_MQMD_REPLYTOQMGR, "QM1");
        message.setStringProperty(JmsConstants.JMS_IBM_CHARACTER_SET, "UTF-8");

        message.setJMSCorrelationIDAsBytes(
                HexUtils.parseHex("414D51207061756C745639344C545320EBC32F6A01A00740"));
        message.setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID,
                HexUtils.parseHex("414141407061756C745639344C545320EBC32F6A01A00740"));
        message.setStringProperty(JmsConstants.JMSX_GROUPID, "mygroup");
        message.setIntProperty(JmsConstants.JMSX_GROUPSEQ, 1);
    }
}
