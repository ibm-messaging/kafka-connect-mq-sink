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

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;

import com.ibm.msg.client.wmq.WMQConstants;

/**
 * Puts JMS messages onto IBM MQ queues using the same queue URL options as {@code mq-jms-test-put}.
 *
 * <p>MQMD write, all-context, and priority must be set on the queue URI so byte[] MQMD fields
 * (correl id, msg id) survive the put. Producer time-to-live sets JMS expiration for expiry
 * header round-trip tests.
 *
 * <p>Requires the putting user to have {@code +setall} on the target queue and queue manager.
 * In integration tests call {@link com.ibm.eventstreams.connect.mqsink.AbstractJMSContextIT
 * #grantAppUserMqmdPutOnQueue(String)} before {@link #putMessage}.
 */
public final class JmsMqPutHelper {

    /** Matches {@code mq-jms-test-put} producer TTL (5 minutes). */
    public static final long DEFAULT_TIME_TO_LIVE_MS = 300_000L;

    public static final int DEFAULT_PRIORITY = 5;

    private JmsMqPutHelper() {
    }

    public static Queue createPutQueue(final JMSContext context, final String queueName) {
        return context.createQueue("queue:///" + queueName + "?"
                + WMQConstants.WMQ_MQMD_WRITE_ENABLED + "=true&"
                + WMQConstants.WMQ_MQMD_MESSAGE_CONTEXT + "=" + WMQConstants.WMQ_MDCTX_SET_ALL_CONTEXT
                + "&" + WMQConstants.PRIORITY + "=" + DEFAULT_PRIORITY);
    }

    public static void putMessage(final JMSContext context, final String queueName, final Message message)
            throws JMSException {
        final Queue queue = createPutQueue(context, queueName);
        final JMSProducer producer = context.createProducer();
        producer.setTimeToLive(DEFAULT_TIME_TO_LIVE_MS);
        producer.send(queue, message);
    }
}
