/**
 * Copyright 2024 IBM Corporation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsink.util;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.ibm.eventstreams.connect.mqsink.builders.DefaultMessageBuilder;
import com.ibm.msg.client.jms.JmsConstants;

public class MessageDescriptorBuilder extends DefaultMessageBuilder {

    @Override
    public Message getJMSMessage(JMSContext jmsCtxt, SinkRecord record) {

        Message message = super.getJMSMessage(jmsCtxt, record);

        // add MQMD values
        // JMS_IBM_MQMD_MsgId - byte[]
        // JMS_IBM_MQMD_ApplIdentityData - string
        // JMS_IBM_MQMD_PutApplName - string
        // https://www.ibm.com/docs/en/ibm-mq/9.4?topic=application-jms-message-object-properties
        try {
            message.setObjectProperty(JmsConstants.JMS_IBM_MQMD_MSGID, "ThisIsMyId".getBytes());
            message.setStringProperty(JmsConstants.JMS_IBM_MQMD_APPLIDENTITYDATA, "ThisIsMyApplicationData");
            message.setStringProperty(JmsConstants.JMS_IBM_MQMD_PUTAPPLNAME, "ThisIsMyPutApplicationName");

        } catch (JMSException e) {
            throw new ConnectException("Failed to write property", e);
        }

        return message;
    }
}
